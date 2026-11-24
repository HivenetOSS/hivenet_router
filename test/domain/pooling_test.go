// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"testing"

	"hivenet_router/internal/domain"
)

// TestParsePoolingInput: every shape of an embedding's "input" and of a
// rerank's documents is measured, and a rerank counts the query once per
// document, as a cross-encoder reads it.
func TestParsePoolingInput(t *testing.T) {
	for _, tc := range []struct {
		name       string
		capability string
		body       string
		want       domain.PoolingInput
	}{
		{"embedding string", domain.CapabilityEmbedding, `{"model":"m","input":"hello"}`,
			domain.PoolingInput{TextBytes: 5, Segments: 1}},
		{"embedding strings", domain.CapabilityEmbedding, `{"model":"m","input":["hello","hi"]}`,
			domain.PoolingInput{TextBytes: 7, Segments: 2}},
		{"embedding token IDs", domain.CapabilityEmbedding, `{"model":"m","input":[1,2,3]}`,
			domain.PoolingInput{TokenIDs: 3, Segments: 1}},
		{"embedding arrays of token IDs", domain.CapabilityEmbedding, `{"model":"m","input":[[1,2,3],[4,5]]}`,
			domain.PoolingInput{TokenIDs: 5, Segments: 2}},
		{"embedding without input", domain.CapabilityEmbedding, `{"model":"m"}`,
			domain.PoolingInput{}},
		{"rerank strings", domain.CapabilityReranker, `{"model":"m","query":"abc","documents":["de","fghi"]}`,
			domain.PoolingInput{TextBytes: 3 + 2 + 3 + 4, Segments: 2}},
		{"rerank objects", domain.CapabilityReranker, `{"model":"m","query":"abc","documents":[{"text":"de"},{"text":"fghi"}]}`,
			domain.PoolingInput{TextBytes: 3 + 2 + 3 + 4, Segments: 2}},
		{"rerank without documents", domain.CapabilityReranker, `{"model":"m","query":"abc"}`,
			domain.PoolingInput{}},
		{"not JSON", domain.CapabilityEmbedding, `not json`, domain.PoolingInput{}},
		{"chat", domain.CapabilityLLM, `{"model":"m","input":"hello"}`, domain.PoolingInput{}},
	} {
		if got := domain.ParsePoolingInput(tc.capability, []byte(tc.body)); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// TestParsePoolingUsage: the usage shapes the engines answer with, measured
// on vLLM, SGLang, llama.cpp, Ollama and TEI.
func TestParsePoolingUsage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		tokens int
		ok     bool
	}{
		{"prompt_tokens (vLLM, SGLang, llama.cpp, Ollama, TEI embeddings)",
			`{"object":"list","data":[],"usage":{"prompt_tokens":26,"total_tokens":26}}`, 26, true},
		{"total_tokens only", `{"results":[],"usage":{"total_tokens":68}}`, 68, true},
		{"SGLang rerank: an array with meta_info per result",
			`[{"score":8.4,"index":0,"meta_info":{"prompt_tokens":21}},{"score":7.6,"index":2,"meta_info":{"prompt_tokens":29}},{"score":0.1,"index":1,"meta_info":{"prompt_tokens":18}}]`,
			68, true},
		{"array without meta_info", `[{"index":0,"score":0.99}]`, 0, false},
		{"no usage", `{"object":"list","data":[]}`, 0, false},
		{"zero usage", `{"usage":{"prompt_tokens":0,"total_tokens":0}}`, 0, false},
		{"leading whitespace", " \n" + `{"usage":{"prompt_tokens":3}}`, 3, true},
		{"not JSON", `oops`, 0, false},
		{"empty", ``, 0, false},
	} {
		tokens, ok := domain.ParsePoolingUsage([]byte(tc.body))
		if tokens != tc.tokens || ok != tc.ok {
			t.Errorf("%s: (%d, %v), want (%d, %v)", tc.name, tokens, ok, tc.tokens, tc.ok)
		}
	}
}
