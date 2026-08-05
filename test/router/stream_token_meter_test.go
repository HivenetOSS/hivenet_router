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
