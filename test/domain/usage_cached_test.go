// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"encoding/json"
	"testing"

	"hivenet_router/internal/domain"
)

// Cached prompt tokens are read from the OpenAI shape (vLLM, SGLang) and the
// Anthropic one, and known only when reported; they never change what is
// serialized.
func TestUsageCachedTokens(t *testing.T) {
	for _, tc := range []struct {
		body   string
		cached int
		known  bool
	}{
		{`{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":64}}`, 64, true},
		{`{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":0}}`, 0, true},
		{`{"input_tokens":100,"output_tokens":5,"cache_read_input_tokens":80}`, 80, true},
		{`{"prompt_tokens":100,"completion_tokens":5}`, 0, false},
		{`{"prompt_tokens":100,"prompt_tokens_details":null}`, 0, false},
	} {
		var u domain.Usage
		if err := json.Unmarshal([]byte(tc.body), &u); err != nil {
			t.Fatal(err)
		}
		if u.CachedTokens != tc.cached || u.CachedKnown != tc.known {
			t.Errorf("%s: cached %d known %v", tc.body, u.CachedTokens, u.CachedKnown)
		}
		out, _ := json.Marshal(u)
		if string(out) != `{"prompt_tokens":`+jsonInt(u.PromptTokens)+`,"completion_tokens":`+jsonInt(u.CompletionTokens)+`,"total_tokens":`+jsonInt(u.TotalTokens)+`}` {
			t.Errorf("serialized usage changed: %s", out)
		}
	}
}

func jsonInt(n int) string { b, _ := json.Marshal(n); return string(b) }
