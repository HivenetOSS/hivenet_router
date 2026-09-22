// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

// newTestProcessor builds a RequestProcessor with just enough state for
// drainStream (metrics + counter store) — no libp2p host, no executor, no
// rate limiter.
func newTestProcessor(t *testing.T) *RequestProcessor {
	t.Helper()
	m := metrics.NewRouterMetrics()
	counters := metrics.NewUniversalCounterStore(testutil.NoopStorage{}, m)
	return NewRequestProcessor(nil, nil, nil, m, counters, 1, nil, nil)
}

// blockingReader emits its data, then blocks until done is closed — a stand-in
// for an agent response body that streams for a long time.
type blockingReader struct {
	data string
	pos  int
	done chan struct{}
}

func (b *blockingReader) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		<-b.done // hold the stream open until the test ends it
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}

func newStreamingResponse(body io.Reader) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(body),
	}
}

func newDrainPending() *domain.PendingRequest {
	return &domain.PendingRequest{
		ID:      "req-drain-1",
		Request: &domain.ChatRequest{},
	}
}

// releaseRecorder wraps the slot-release closure and reports how many times it
// fired. The drain goroutine calls releaseSlot exactly once per stream.
func releaseRecorder(release func()) (tracked func(), calls *atomic.Int64) {
	var n atomic.Int64
	tracked = func() {
		release()
		n.Add(1)
	}
	return tracked, &n
}

// TestDrainStream_HoldsSlotUntilStreamEnds pins the slot-ownership contract for
// streamed responses: the in-flight slot must stay held while the generation is
// in flight and be released exactly once when the stream ends. A regression to
// release-at-stream-start would show up as load 0 while the stream is still open.
func TestDrainStream_HoldsSlotUntilStreamEnds(t *testing.T) {
	agent := domain.NewAgent(peer.ID("agent-drain-1"), domain.AgentMetadata{
		Model: "test-model", Capacity: 2, Capability: domain.CapabilityLLM, Engine: "vllm",
	}, "")
	p := newTestProcessor(t)

	// Simulate the executor having claimed the slot for this request.
	if !agent.TryAcquireSlot() {
		t.Fatal("failed to acquire slot")
	}
	if got := agent.GetLoad(); got != 1 {
		t.Fatalf("load after acquire = %d, want 1", got)
	}

	body := &blockingReader{data: "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n", done: make(chan struct{})}
	pending := newDrainPending()
	pr, pw := io.Pipe()
	defer pr.Close()

	// The handler-side consumer: in production the API handler reads the pipe
	// while streaming to the client; io.Pipe has no buffer, so the drain's
	// writes would block without it.
	go io.Copy(io.Discard, pr)

	// Same slot-release closure shape as forwardToAgent: sync.Once around
	// DecrementLoad.
	var once sync.Once
	releaseSlot, calls := releaseRecorder(func() { once.Do(agent.DecrementLoad) })

	go p.drainStream(agent, pending, pw, newStreamingResponse(body), func() {}, NewSSETokenMeter(),
		"EU-France", "vllm", "test-model", 5.0, releaseSlot)

	// Let io.Copy deliver the chunk; the stream is still open.
	time.Sleep(50 * time.Millisecond)
	if got := agent.GetLoad(); got != 1 {
		t.Fatalf("load while stream open = %d, want 1 (slot must be held for the whole generation)", got)
	}

	close(body.done) // stream ends
	waitForRelease(t, calls)
	if got := agent.GetLoad(); got != 0 {
		t.Fatalf("load after stream end = %d, want 0", got)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("releaseSlot called %d times, want exactly 1", n)
	}
}

// infiniteReader never ends on its own — a stream that only stops when the
// reader side of the pipe goes away (client disconnect).
type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) { return copy(p, "x"), nil }

// TestDrainStream_ReleasesSlotOnClientDisconnect covers the early-termination
// path: the handler closes the client pipe (client went away), io.Copy fails on
// the next write, and the drain goroutine must still release the slot exactly
// once — otherwise the agent's capacity would be leaked permanently.
func TestDrainStream_ReleasesSlotOnClientDisconnect(t *testing.T) {
	agent := domain.NewAgent(peer.ID("agent-drain-2"), domain.AgentMetadata{
		Model: "test-model", Capacity: 2, Capability: domain.CapabilityLLM, Engine: "vllm",
	}, "")
	p := newTestProcessor(t)

	if !agent.TryAcquireSlot() {
		t.Fatal("failed to acquire slot")
	}

	pending := newDrainPending()
	pr, pw := io.Pipe()

	var once sync.Once
	releaseSlot, calls := releaseRecorder(func() { once.Do(agent.DecrementLoad) })

	go p.drainStream(agent, pending, pw, newStreamingResponse(infiniteReader{}), func() {}, NewSSETokenMeter(),
		"EU-France", "vllm", "test-model", 5.0, releaseSlot)

	time.Sleep(20 * time.Millisecond) // let the first write land
	if err := pr.Close(); err != nil { // client disconnect → next write fails
		t.Fatalf("closing pipe: %v", err)
	}

	waitForRelease(t, calls)
	if got := agent.GetLoad(); got != 0 {
		t.Fatalf("load after client disconnect = %d, want 0", got)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("releaseSlot called %d times, want exactly 1", n)
	}
}

func waitForRelease(t *testing.T, calls *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for slot release")
		}
		time.Sleep(10 * time.Millisecond)
	}
}