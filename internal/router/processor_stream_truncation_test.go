// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
)

const truncatedFrameMarker = "stream_truncated"

// failingReader emits its data, then returns fail on every subsequent read —
// a stand-in for an agent response body whose stream dies mid-generation
// (connection reset, request deadline).
type failingReader struct {
	data string
	pos  int
	fail error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.pos < len(f.data) {
		n := copy(p, f.data[f.pos:])
		f.pos += n
		return n, nil
	}
	return 0, f.fail
}

// openAIChunkReader streams OpenAI-dialect delta chunks forever — the
// production client-disconnect scenario: the client goes away mid-OpenAI
// stream, leaving events seen with no terminal marker. Without the
// clientGone guard in drainStream that would be miscounted as missing_terminal.
type openAIChunkReader struct{ pos int }

func (r *openAIChunkReader) Read(p []byte) (int, error) {
	chunk := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
	n := copy(p, chunk[r.pos:])
	r.pos += n
	if r.pos >= len(chunk) {
		r.pos = 0
	}
	return n, nil
}

// counterValue reads a single counter series (by metric name and label set)
// from the router's private registry. A missing series reads as zero.
func counterValue(t *testing.T, m *metrics.RouterMetrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, mtr := range f.GetMetric() {
			match := true
			for _, l := range mtr.GetLabel() {
				if labels[l.GetName()] != l.GetValue() {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			c := mtr.GetCounter()
			if c == nil {
				t.Fatalf("metric %s is not a counter", name)
			}
			return c.GetValue()
		}
	}
	return 0
}

// counterTotal sums every series of a counter family, across all label sets.
// A missing family reads as zero. Use this when the test does not know (or
// care which) reason label the increment landed under.
func counterTotal(t *testing.T, m *metrics.RouterMetrics, name string) float64 {
	t.Helper()
	families, err := m.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var sum float64
		for _, mtr := range f.GetMetric() {
			if c := mtr.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return sum
	}
	return 0
}

func drainTestAgent(t *testing.T) *domain.Agent {
	t.Helper()
	agent := domain.NewAgent(peer.ID("agent-trunc-1"), domain.AgentMetadata{
		Model: "test-model", Capacity: 2, Capability: domain.CapabilityLLM, Engine: "vllm",
	}, "")
	if !agent.TryAcquireSlot() {
		t.Fatal("failed to acquire slot")
	}
	return agent
}

// drainAndRead runs drainStream against body and reads everything the client
// pipe delivers, including the terminal error frame if one is injected.
func drainAndRead(t *testing.T, p *RequestProcessor, agent *domain.Agent, body io.Reader) string {
	t.Helper()
	return drainPathAndRead(t, p, agent, "", body)
}

// drainPathAndRead is drainAndRead for a request forwarded to path (e.g.
// "/v1/messages"), which selects the dialect of the injected error frame.
func drainPathAndRead(t *testing.T, p *RequestProcessor, agent *domain.Agent, path string, body io.Reader) string {
	t.Helper()
	pending := newDrainPending()
	pending.Path = path
	pr, pw := io.Pipe()
	go p.drainStream(agent, pending, pw, newStreamingResponse(body), func() {}, NewSSETokenMeter(),
		"EU-France", "vllm", "test-model", 5.0, func() { agent.DecrementLoad() })
	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("reading client pipe: %v", err)
	}
	return string(out)
}

// TestDrainStream_UpstreamErrorInjectsErrorFrame pins the silent-truncation
// fix: when the agent stream dies mid-generation, the client must receive a
// terminal SSE error frame (so a partial response is never mistaken for a
// complete one) and the truncation must be visible in metrics.
func TestDrainStream_UpstreamErrorInjectsErrorFrame(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	body := &failingReader{
		data: "data: {\"choices\":[{\"delta\":{\"content\":\"partial \"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"text\"}}]}\n\n",
		fail: errors.New("upstream stream reset"),
	}

	out := drainAndRead(t, p, agent, body)

	if !strings.Contains(out, truncatedFrameMarker) {
		t.Fatalf("client stream missing terminal error frame; got: %q", out)
	}
	if !strings.Contains(out, `"error"`) {
		t.Fatalf("terminal frame is not an OpenAI error frame; got: %q", out)
	}
	// The partial content must still be delivered, frame included at the end.
	idxPartial := strings.Index(out, "partial")
	idxFrame := strings.Index(out, truncatedFrameMarker)
	if idxPartial < 0 || idxFrame < idxPartial {
		t.Fatalf("error frame must come after the partial content; got: %q", out)
	}
	if got := counterValue(t, p.metrics, "hivenet_stream_truncated_total",
		map[string]string{"model": "test-model", "reason": "upstream_error"}); got != 1 {
		t.Fatalf("stream_truncated_total{upstream_error} = %v, want 1", got)
	}
	// The truncated stream still counts as served (tokens were generated); the
	// truncation counter is the visibility signal.
	if got := agent.GetLoad(); got != 0 {
		t.Fatalf("load after upstream error = %d, want 0", got)
	}
}

// TestDrainStream_MissingTerminalInjectsErrorFrame covers the agent-side
// abort: the agent's own write failed and its handler returned, so the router
// sees a CLEAN EOF — but the stream never carried a finish_reason or [DONE].
func TestDrainStream_MissingTerminalInjectsErrorFrame(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	body := io.NopCloser(
		strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"only a delta\"}}]}\n\n"),
	)

	out := drainAndRead(t, p, agent, body)

	if !strings.Contains(out, truncatedFrameMarker) {
		t.Fatalf("client stream missing terminal error frame; got: %q", out)
	}
	if got := counterValue(t, p.metrics, "hivenet_stream_truncated_total",
		map[string]string{"model": "test-model", "reason": "missing_terminal"}); got != 1 {
		t.Fatalf("stream_truncated_total{missing_terminal} = %v, want 1", got)
	}
}

// TestDrainStream_CompleteStreamNoFrame is the negative case: a healthy stream
// (delta + terminal finish_reason + [DONE]) must pass through untouched, with
// no injected frame and no truncation counter.
func TestDrainStream_CompleteStreamNoFrame(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	body := io.NopCloser(strings.NewReader(
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: [DONE]\n\n",
	))

	out := drainAndRead(t, p, agent, body)

	if strings.Contains(out, truncatedFrameMarker) {
		t.Fatalf("complete stream must not carry an error frame; got: %q", out)
	}
	if strings.Contains(out, `"error"`) {
		t.Fatalf("complete stream must not carry an error frame; got: %q", out)
	}
	if got := counterTotal(t, p.metrics, "hivenet_stream_truncated_total"); got != 0 {
		t.Fatalf("stream_truncated_total = %v, want 0 for a complete stream", got)
	}
	// Byte-exact forwarding must survive: the [DONE] marker is still the last
	// thing the client sees.
	if !strings.HasSuffix(strings.TrimSpace(out), "data: [DONE]") {
		t.Fatalf("stream must end with the [DONE] marker; got: %q", out)
	}
}

// TestDrainStream_ClientDisconnectNotCounted pins the client-side boundary:
// when the CLIENT goes away mid-OpenAI-stream (handler closes the pipe), the
// failed write is not an upstream truncation — no error frame is expected and
// the counter must stay at zero. The upstream streams OpenAI-dialect chunks
// forever (sawOpenAI set, no terminal), i.e. the production scenario where a
// user aborts pi mid-generation.
func TestDrainStream_ClientDisconnectNotCounted(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	pending := newDrainPending()
	pr, pw := io.Pipe()

	// drained closes once drainStream has released the slot, which happens
	// after the truncation decision — so the counter is final by then.
	drained := make(chan struct{})
	go p.drainStream(agent, pending, pw, newStreamingResponse(&openAIChunkReader{}), func() {}, NewSSETokenMeter(),
		"EU-France", "vllm", "test-model", 5.0, func() { agent.DecrementLoad(); close(drained) })

	// Consume the pipe in the background: io.Pipe is unbuffered, so without a
	// reader the very first write blocks forever and the token meter would
	// see no chunks. The reader must see the OpenAI chunks land before the
	// client disconnects, or the case under test never arises.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, pr)
	}()

	// Let chunks flow, then disconnect the client.
	time.Sleep(30 * time.Millisecond)
	if err := pr.Close(); err != nil {
		t.Fatalf("closing pipe: %v", err)
	}
	<-done

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("drainStream did not finish after the client disconnected")
	}
	// Sum the whole family: the test must not pass because an increment
	// landed under a label set it cannot see.
	if got := counterTotal(t, p.metrics, "hivenet_stream_truncated_total"); got != 0 {
		t.Fatalf("client disconnect must not count as truncation; counter = %v", got)
	}
	if got := agent.GetLoad(); got != 0 {
		t.Fatalf("load after client disconnect = %d, want 0", got)
	}
}

// anthropicEvents renders Anthropic-dialect SSE events (event + data lines).
func anthropicEvents(events ...string) string {
	var b strings.Builder
	for _, data := range events {
		var head struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal([]byte(data), &head)
		b.WriteString("event: " + head.Type + "\ndata: " + data + "\n\n")
	}
	return b.String()
}

var anthropicPrefix = []string{
	`{"type":"message_start","message":{"usage":{"input_tokens":10,"output_tokens":1}}}`,
	`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
	`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
}

// TestDrainStream_AnthropicStreamNotMisjudged guards against false positives:
// a complete Anthropic-dialect stream ends with message_stop, never with an
// OpenAI finish_reason or [DONE]. It must not be flagged as truncated.
func TestDrainStream_AnthropicStreamNotMisjudged(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	body := io.NopCloser(strings.NewReader(anthropicEvents(append(anthropicPrefix,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}`,
		`{"type":"message_stop"}`,
	)...)))

	out := drainPathAndRead(t, p, agent, "/v1/messages", body)

	if strings.Contains(out, truncatedFrameMarker) {
		t.Fatalf("Anthropic-dialect stream must not carry an error frame; got: %q", out)
	}
	if got := counterTotal(t, p.metrics, "hivenet_stream_truncated_total"); got != 0 {
		t.Fatalf("Anthropic-dialect stream must not count as truncated; counter = %v", got)
	}
}

// TestDrainStream_AnthropicMissingStopInjectsAnthropicError covers a clean-EOF
// cut of an Anthropic stream: no message_stop arrived, so the client must get
// an Anthropic-shaped error event (event: error) it will actually surface.
func TestDrainStream_AnthropicMissingStopInjectsAnthropicError(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	out := drainPathAndRead(t, p, agent, "/v1/messages",
		io.NopCloser(strings.NewReader(anthropicEvents(anthropicPrefix...))))

	if !strings.HasSuffix(out, "\n\n") || !strings.Contains(out, "event: error\ndata: ") {
		t.Fatalf("missing Anthropic error event; got: %q", out)
	}
	frame := out[strings.LastIndex(out, "data: ")+len("data: "):]
	var ev struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(frame)), &ev); err != nil {
		t.Fatalf("error event is not valid JSON: %v; got: %q", err, frame)
	}
	if ev.Type != "error" || ev.Error.Type != "api_error" || !strings.Contains(ev.Error.Message, truncatedFrameMarker) {
		t.Fatalf("unexpected Anthropic error event: %+v", ev)
	}
	if got := counterValue(t, p.metrics, "hivenet_stream_truncated_total",
		map[string]string{"model": "test-model", "reason": "missing_terminal"}); got != 1 {
		t.Fatalf("stream_truncated_total{missing_terminal} = %v, want 1", got)
	}
}

// TestDrainStream_CutMidLineDropsPartialLine pins the frame boundary: when the
// upstream dies in the middle of a data line, the half line is not forwarded,
// so every data line the client parses is valid JSON and the last one is the
// stream_truncated error.
func TestDrainStream_CutMidLineDropsPartialLine(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	body := &failingReader{
		data: "data: {\"choices\":[{\"delta\":{\"content\":\"whole\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"cont",
		fail: errors.New("upstream stream reset"),
	}

	out := drainAndRead(t, p, agent, body)

	var last string
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		last = strings.TrimPrefix(line, "data: ")
		if !json.Valid([]byte(last)) {
			t.Fatalf("client received a data line that is not valid JSON: %q (stream: %q)", line, out)
		}
	}
	if !strings.Contains(last, truncatedFrameMarker) {
		t.Fatalf("last data line must be the truncation error; got: %q", out)
	}
	if !strings.Contains(out, "whole") {
		t.Fatalf("complete lines before the cut must still be delivered; got: %q", out)
	}
}

// TestDrainStream_UnterminatedFinalLineNotTruncated covers a complete stream
// whose final "data: [DONE]" has no trailing newline: it must still count as
// terminated, and the held final line must reach the client.
func TestDrainStream_UnterminatedFinalLineNotTruncated(t *testing.T) {
	agent := drainTestAgent(t)
	p := newTestProcessor(t)

	body := io.NopCloser(strings.NewReader(
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n" +
			"data: [DONE]",
	))

	out := drainAndRead(t, p, agent, body)

	if strings.Contains(out, truncatedFrameMarker) {
		t.Fatalf("unterminated [DONE] must not be judged truncated; got: %q", out)
	}
	if !strings.HasSuffix(out, "data: [DONE]") {
		t.Fatalf("the unterminated final line must be forwarded; got: %q", out)
	}
	if got := counterTotal(t, p.metrics, "hivenet_stream_truncated_total"); got != 0 {
		t.Fatalf("stream_truncated_total = %v, want 0", got)
	}
}
