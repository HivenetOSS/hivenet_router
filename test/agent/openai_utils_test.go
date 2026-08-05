// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package agent_test contains black-box tests for the agent's streaming response
// writer, which applies a rolling per-chunk write deadline so a stalled reader cannot
// leave the underlying libp2p write blocked forever (the bug that wedged the agent).
package agent_test

import (
	"net/http"
	"os"
	"testing"
	"time"

	"hivenet_router/internal/agent"
)

// stalledWriter models the failure condition: a libp2p stream whose send window is full
// because the reader (router → client) stopped reading. Its Write blocks — exactly like
// the real yamux write the goroutine dump was frozen on — until the write deadline fires
// (returning a timeout error) or, when no deadline is set, until the test releases it.
// It also satisfies http.Flusher and SetWriteDeadline so NewStreamWriter wraps it and
// http.NewResponseController can reach it.
type stalledWriter struct {
	hdr       http.Header
	deadline  time.Time
	release   chan struct{}
	deadlines int
}

func newStalledWriter() *stalledWriter { return &stalledWriter{release: make(chan struct{})} }

func (s *stalledWriter) Header() http.Header {
	if s.hdr == nil {
		s.hdr = make(http.Header)
	}
	return s.hdr
}
func (s *stalledWriter) WriteHeader(int) {}
func (s *stalledWriter) Flush()          {}
func (s *stalledWriter) SetWriteDeadline(t time.Time) error {
	s.deadline = t
	s.deadlines++
	return nil
}
func (s *stalledWriter) Write(p []byte) (int, error) {
	if !s.deadline.IsZero() {
		timer := time.NewTimer(time.Until(s.deadline))
		defer timer.Stop()
		select {
		case <-timer.C:
			return 0, os.ErrDeadlineExceeded // window still full when the deadline fired
		case <-s.release:
			return len(p), nil
		}
	}
	<-s.release // no deadline: block until released — the original "hangs forever" leak
	return len(p), nil
}

// THE FIX: when the reader stalls, the rolling write deadline must make the blocked
// write fail (so the handler returns and the stream is released) instead of hanging.
func TestStreamWriter_StalledWriteFailsOnDeadline(t *testing.T) {
	sw := newStalledWriter()
	t.Cleanup(func() { close(sw.release) })
	w := agent.NewStreamWriter(sw, 50*time.Millisecond)

	errc := make(chan error, 1)
	go func() { _, err := w.Write([]byte("data")); errc <- err }()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected the stalled write to fail once the deadline fired, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stalled write never unblocked — the write deadline was not applied (the bug)")
	}
}

// THE BUG (contrast): with the timeout disabled, the same stalled write blocks forever —
// this is the condition that leaked ~200 handlers and wedged the agent.
func TestStreamWriter_NoTimeoutBlocksForever(t *testing.T) {
	sw := newStalledWriter()
	t.Cleanup(func() { close(sw.release) })
	w := agent.NewStreamWriter(sw, 0)

	errc := make(chan error, 1)
	go func() { _, err := w.Write([]byte("data")); errc <- err }()

	select {
	case <-errc:
		t.Fatal("write returned, but with no deadline a stalled write must block (the leak)")
	case <-time.After(200 * time.Millisecond):
		// Still blocked after 200ms — the unfixed behaviour the deadline fix addresses.
		if sw.deadlines != 0 {
			t.Fatalf("SetWriteDeadline called %d times with timeout disabled, want 0", sw.deadlines)
		}
	}
}

// A healthy stream (writes that complete promptly) must pass through, flush each chunk,
// and have a fresh rolling deadline set per chunk — never falsely tripping the timeout.
func TestStreamWriter_HealthyStreamPassesThrough(t *testing.T) {
	rec := &fastWriter{}
	w := agent.NewStreamWriter(rec, 30*time.Second)

	for _, chunk := range []string{"a", "b", "c"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("healthy write %q failed: %v", chunk, err)
		}
	}
	if rec.flushes != 3 {
		t.Fatalf("flushes = %d, want 3", rec.flushes)
	}
	if rec.deadlines != 3 {
		t.Fatalf("rolling deadlines set = %d, want 3 (one per chunk)", rec.deadlines)
	}
	if string(rec.written) != "abc" {
		t.Fatalf("written = %q, want %q", string(rec.written), "abc")
	}
}

// fastWriter is a non-blocking ResponseWriter+Flusher that records writes, flushes, and
// deadline calls — used for the healthy-path test.
type fastWriter struct {
	hdr       http.Header
	written   []byte
	flushes   int
	deadlines int
}

func (f *fastWriter) Header() http.Header {
	if f.hdr == nil {
		f.hdr = make(http.Header)
	}
	return f.hdr
}
func (f *fastWriter) Write(p []byte) (int, error) {
	f.written = append(f.written, p...)
	return len(p), nil
}
func (f *fastWriter) WriteHeader(int) {}
func (f *fastWriter) Flush()          { f.flushes++ }
func (f *fastWriter) SetWriteDeadline(time.Time) error {
	f.deadlines++
	return nil
}
