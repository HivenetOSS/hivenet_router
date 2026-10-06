// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package router_test contains black-box tests for the streaming token meter,
// which estimates per-stream token usage from a copy of the SSE response so the
// processor can do post-hoc budget accounting without disturbing the byte-exact
// stream forwarded to the client.
package router_test

import (
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/router"
)

// reqWithPrompt builds a minimal chat request whose messages estimate to a
// non-zero prompt token count.
func reqWithPrompt() *domain.ChatRequest {
	return &domain.ChatRequest{
		Model: "test-model",
		Messages: []domain.ChatCompletionMessage{
			{Role: "user", Content: "hello world, please write a short greeting"},
		},
	}
}

// TestMeter_EstimatesFromDeltas verifies that, when the backend does not emit a
// usage object, the meter estimates completion tokens from the accumulated delta
// text and prompt tokens from the request messages.
func TestMeter_EstimatesFromDeltas(t *testing.T) {
	m := router.NewSSETokenMeter()
	chunks := []string{
		`data: {"choices":[{"delta":{"content":"Hello"}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"content":" there, friend"}}]}` + "\n\n",
		"data: [DONE]\n\n",
	}
	for _, c := range chunks {
		if _, err := m.Write([]byte(c)); err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
	}

	prompt, completion := m.Tokens(reqWithPrompt())
	if completion <= 0 {
		t.Errorf("expected completion tokens estimated from deltas > 0, got %d", completion)
	}
	if prompt <= 0 {
		t.Errorf("expected prompt tokens estimated from messages > 0, got %d", prompt)
	}
}

// TestMeter_PrefersExactUsage verifies that an explicit usage object (emitted
// when the request set stream_options.include_usage) takes precedence over the
// text estimate, regardless of the request passed to Tokens.
func TestMeter_PrefersExactUsage(t *testing.T) {
	m := router.NewSSETokenMeter()
	stream := `data: {"choices":[{"delta":{"content":"Hi"}}]}` + "\n\n" +
		`data: {"choices":[],"usage":{"prompt_tokens":42,"completion_tokens":7}}` + "\n\n" +
		"data: [DONE]\n\n"
	if _, err := m.Write([]byte(stream)); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	// Pass nil to prove the exact usage is used, not the request estimate.
	prompt, completion := m.Tokens(nil)
	if prompt != 42 {
		t.Errorf("expected exact prompt_tokens 42, got %d", prompt)
	}
	if completion != 7 {
		t.Errorf("expected exact completion_tokens 7, got %d", completion)
	}
}

// TestMeter_ContentObserverFiresPerChunk verifies the growth hook: the content
// observer is invoked with each delta as it is parsed, so the occupancy budget
// can grow an undeclared request live rather than only at end-of-stream.
func TestMeter_ContentObserverFiresPerChunk(t *testing.T) {
	m := router.NewSSETokenMeter()
	var deltas []string
	m.SetContentObserver(func(d string) { deltas = append(deltas, d) })

	chunks := []string{
		`data: {"choices":[{"delta":{"content":"Hello"}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"content":" there"}}]}` + "\n\n",
		`data: {"choices":[{"delta":{}}]}` + "\n\n", // no content → no callback
		"data: [DONE]\n\n",
	}
	for _, c := range chunks {
		if _, err := m.Write([]byte(c)); err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
	}

	want := []string{"Hello", " there"}
	if len(deltas) != len(want) {
		t.Fatalf("observer must fire once per content delta; got %d calls %v, want %v", len(deltas), deltas, want)
	}
	for i := range want {
		if deltas[i] != want[i] {
			t.Errorf("delta %d = %q, want %q", i, deltas[i], want[i])
		}
	}
}

// countingReservation records the total tokens grown, for the growth-observer test.
type countingReservation struct{ grown int }

func (c *countingReservation) Grow(tokens int) { c.grown += tokens }
func (c *countingReservation) Adjust(int)      {}
func (c *countingReservation) Release()        {}

// TestGrowthObserver_ChargesFloorOfCumulativeBytes verifies growth charges
// floor(totalBytes/4) across many small deltas, not the sum of per-chunk floors
// (which would round each tiny delta down to zero and badly under-count).
func TestGrowthObserver_ChargesFloorOfCumulativeBytes(t *testing.T) {
	res := &countingReservation{}
	obs := router.NewGrowthObserver(res)

	// 100 deltas of 3 bytes each = 300 bytes. Per-chunk floor(3/4)=0 would total 0;
	// cumulative floor(300/4)=75.
	for i := 0; i < 100; i++ {
		obs("abc")
	}
	if res.grown != 75 {
		t.Errorf("cumulative growth must be floor(300/4)=75, got %d", res.grown)
	}
}

// TestMeter_HaveUsage verifies the exactness signal the handler uses to decide
// whether to feed a streaming prompt count into the learned estimator.
func TestMeter_HaveUsage(t *testing.T) {
	withUsage := router.NewSSETokenMeter()
	if _, err := withUsage.Write([]byte(`data: {"choices":[],"usage":{"prompt_tokens":42,"completion_tokens":7}}` + "\n\n")); err != nil {
		t.Fatal(err)
	}
	if !withUsage.HaveUsage() {
		t.Error("HaveUsage must be true after a backend usage chunk")
	}

	noUsage := router.NewSSETokenMeter()
	if _, err := noUsage.Write([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n")); err != nil {
		t.Fatal(err)
	}
	if noUsage.HaveUsage() {
		t.Error("HaveUsage must be false when the stream carried no usage object")
	}
}

// TestMeter_HandlesSplitWrites verifies that an SSE event split across multiple
// Write calls (as happens with TCP/stream chunking) is still parsed once the
// terminating newline arrives.
func TestMeter_HandlesSplitWrites(t *testing.T) {
	m := router.NewSSETokenMeter()
	// Split mid-JSON, before the newline that completes the event.
	if _, err := m.Write([]byte(`data: {"choices":[{"delta":{"con`)); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if _, err := m.Write([]byte(`tent":"assembled across writes"}}]}` + "\n\n")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	_, completion := m.Tokens(reqWithPrompt())
	if completion <= 0 {
		t.Errorf("expected the split event to be parsed and counted, got completion=%d", completion)
	}
}

// TestMeter_ParsesAnthropicEvents verifies the meter reads the Anthropic
// /v1/messages SSE dialect: exact input_tokens from message_start, cumulative
// output_tokens from message_delta, and streamed text from content_block_delta
// — so Anthropic streams get exact usage (metering, OTPM, estimator learning)
// instead of silently falling back to estimation.
func TestMeter_ParsesAnthropicEvents(t *testing.T) {
	m := router.NewSSETokenMeter()
	var streamed string
	m.SetContentObserver(func(delta string) { streamed += delta })

	chunks := []string{
		"event: message_start\n",
		`data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":42,"output_tokens":1}}}` + "\n\n",
		"event: content_block_delta\n",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}` + "\n\n",
		"event: content_block_delta\n",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}` + "\n\n",
		"event: message_delta\n",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9}}` + "\n\n",
	}
	for _, c := range chunks {
		if _, err := m.Write([]byte(c)); err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
	}

	if !m.HaveUsage() {
		t.Fatal("Anthropic message_start must mark usage as exact")
	}
	prompt, completion := m.Tokens(nil)
	if prompt != 42 {
		t.Errorf("prompt = %d, want 42 (message_start input_tokens)", prompt)
	}
	if completion != 9 {
		t.Errorf("completion = %d, want 9 (final message_delta output_tokens)", completion)
	}
	if streamed != "Hello world" {
		t.Errorf("content observer saw %q, want %q", streamed, "Hello world")
	}
}

// TestMeter_TerminalMarkers pins the stream-completion flags drainStream uses
// to detect truncation: which events count as "a stream was seen" and which
// as its terminal marker, in both dialects.
func TestMeter_TerminalMarkers(t *testing.T) {
	cases := []struct {
		name         string
		stream       string
		wantEvents   bool
		wantTerminal bool
	}{
		{
			name:       "openai delta with null finish_reason is not terminal",
			stream:     `data: {"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n",
			wantEvents: true,
		},
		{
			name:       "openai empty finish_reason is not terminal",
			stream:     `data: {"choices":[{"delta":{"content":"hi"},"finish_reason":""}]}` + "\n\n",
			wantEvents: true,
		},
		{
			name:         "openai finish_reason is terminal",
			stream:       `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n",
			wantEvents:   true,
			wantTerminal: true,
		},
		{
			name:         "openai [DONE] is terminal",
			stream:       "data: [DONE]\n\n",
			wantEvents:   true,
			wantTerminal: true,
		},
		{
			name: "anthropic without message_stop is not terminal",
			stream: "event: message_start\n" +
				`data: {"type":"message_start","message":{"usage":{"input_tokens":3}}}` + "\n\n" +
				"event: message_delta\n" +
				`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}` + "\n\n",
			wantEvents: true,
		},
		{
			name:         "anthropic message_stop is terminal",
			stream:       "event: message_stop\n" + `data: {"type":"message_stop"}` + "\n\n",
			wantEvents:   true,
			wantTerminal: true,
		},
		{
			name:         "anthropic error event is terminal",
			stream:       "event: error\n" + `data: {"type":"error","error":{"type":"overloaded_error","message":"busy"}}` + "\n\n",
			wantEvents:   true,
			wantTerminal: true,
		},
		{
			name:   "keepalives and comments are not events",
			stream: ": ping\n\ndata: keepalive\n\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := router.NewSSETokenMeter()
			if _, err := m.Write([]byte(tc.stream)); err != nil {
				t.Fatalf("Write returned error: %v", err)
			}
			m.Flush()
			if got := m.SawEvents(); got != tc.wantEvents {
				t.Errorf("SawEvents() = %v, want %v", got, tc.wantEvents)
			}
			if got := m.SawTerminal(); got != tc.wantTerminal {
				t.Errorf("SawTerminal() = %v, want %v", got, tc.wantTerminal)
			}
		})
	}
}

// TestMeter_FlushParsesUnterminatedFinalLine verifies a final line without a
// trailing newline is parsed only once Flush is called at end of stream.
func TestMeter_FlushParsesUnterminatedFinalLine(t *testing.T) {
	m := router.NewSSETokenMeter()
	if _, err := m.Write([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n" + "data: [DONE]")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if m.SawTerminal() {
		t.Fatal("an unterminated line must not be parsed before Flush")
	}
	m.Flush()
	if !m.SawTerminal() {
		t.Fatal("Flush must parse the unterminated final [DONE]")
	}
}
