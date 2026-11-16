// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import "testing"

// The meter reads the engine's cached prompt tokens from OpenAI streams
// (prompt_tokens_details.cached_tokens) and from Anthropic streams
// (cache_read_input_tokens in message_start or message_delta).
func TestSSETokenMeterCached(t *testing.T) {
	cases := []struct {
		name   string
		stream string
		cached int
		known  bool
	}{
		{"openai", `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n" +
			`data: {"choices":[],"usage":{"prompt_tokens":900,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":640}}}` + "\n\ndata: [DONE]\n\n", 640, true},
		{"anthropic message_start", `data: {"type":"message_start","message":{"usage":{"input_tokens":900,"output_tokens":1,"cache_read_input_tokens":300}}}` + "\n\n" +
			`data: {"type":"content_block_delta","delta":{"text":"hi"}}` + "\n\n" +
			`data: {"type":"message_delta","usage":{"output_tokens":2}}` + "\n\n" + `data: {"type":"message_stop"}` + "\n\n", 300, true},
		{"anthropic message_delta", `data: {"type":"message_start","message":{"usage":{"input_tokens":900,"output_tokens":1}}}` + "\n\n" +
			`data: {"type":"message_delta","usage":{"output_tokens":2,"cache_read_input_tokens":512}}` + "\n\n" + `data: {"type":"message_stop"}` + "\n\n", 512, true},
		{"anthropic without cache report", `data: {"type":"message_start","message":{"usage":{"input_tokens":900,"output_tokens":1}}}` + "\n\n" +
			`data: {"type":"message_stop"}` + "\n\n", 0, false},
	}
	for _, c := range cases {
		m := NewSSETokenMeter()
		if _, err := m.Write([]byte(c.stream)); err != nil {
			t.Fatal(err)
		}
		if got, known := m.Cached(); got != c.cached || known != c.known {
			t.Errorf("%s: cached %d known %v, want %d %v", c.name, got, known, c.cached, c.known)
		}
	}
}
