// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"bytes"
	"encoding/json"
)

// PoolingInput is what an embedding or rerank request makes the model read,
// measured from the request body before it is sent: the input to the token
// estimate charged at admission, and the fallback count when the backend
// reports no usable usage.
type PoolingInput struct {
	// TextBytes is the bytes of text the model reads. A cross-encoder reads
	// the query once with every document, so a rerank counts the query once
	// per document, plus every document.
	TextBytes int
	// Segments is the number of sequences the model reads: the inputs of an
	// embedding, the query-document pairs of a rerank.
	Segments int
	// TokenIDs is the number of tokens an embedding gives as token IDs
	// instead of text: those are counted exactly.
	TokenIDs int
}

// ParsePoolingInput measures an embedding (CapabilityEmbedding: "input" as a
// string, strings, token IDs or arrays of token IDs) or rerank
// (CapabilityReranker: "query" and "documents", each document a string or an
// object with "text") request body. An unreadable body measures zero.
func ParsePoolingInput(capability string, body []byte) PoolingInput {
	switch capability {
	case CapabilityEmbedding:
		var req struct {
			Input json.RawMessage `json:"input"`
		}
		if json.Unmarshal(body, &req) != nil {
			return PoolingInput{}
		}
		return parseEmbeddingInput(req.Input)
	case CapabilityReranker:
		var req struct {
			Query     string            `json:"query"`
			Documents []json.RawMessage `json:"documents"`
		}
		if json.Unmarshal(body, &req) != nil {
			return PoolingInput{}
		}
		in := PoolingInput{Segments: len(req.Documents)}
		for _, d := range req.Documents {
			in.TextBytes += len(req.Query) + len(documentText(d))
		}
		return in
	}
	return PoolingInput{}
}

// parseEmbeddingInput measures the four shapes of the OpenAI "input" field.
func parseEmbeddingInput(raw json.RawMessage) PoolingInput {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return PoolingInput{TextBytes: len(s), Segments: 1}
	}
	var ss []string
	if json.Unmarshal(raw, &ss) == nil {
		in := PoolingInput{Segments: len(ss)}
		for _, t := range ss {
			in.TextBytes += len(t)
		}
		return in
	}
	var ids []int
	if json.Unmarshal(raw, &ids) == nil {
		return PoolingInput{TokenIDs: len(ids), Segments: 1}
	}
	var idss [][]int
	if json.Unmarshal(raw, &idss) == nil {
		in := PoolingInput{Segments: len(idss)}
		for _, t := range idss {
			in.TokenIDs += len(t)
		}
		return in
	}
	return PoolingInput{}
}

// documentText is a rerank document's text: the string itself, or the "text"
// field of a document object.
func documentText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var obj struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(raw, &obj)
	return obj.Text
}

// ParsePoolingUsage returns the input tokens an embedding or rerank backend
// reports in its response body, and whether it reports any:
//   - an object's usage.prompt_tokens, else usage.total_tokens (the
//     OpenAI-compatible shape: vLLM, llama.cpp, Ollama, TEI; vLLM's rerank
//     reports only total_tokens in some versions);
//   - for a top-level array (SGLang's rerank), the sum of every result's
//     meta_info.prompt_tokens.
//
// A pooling model reads its input and generates nothing, so every token is an
// input token.
func ParsePoolingUsage(body []byte) (tokens int, ok bool) {
	body = bytes.TrimSpace(body)
	if len(body) > 0 && body[0] == '[' {
		var results []struct {
			MetaInfo struct {
				PromptTokens int `json:"prompt_tokens"`
			} `json:"meta_info"`
		}
		if json.Unmarshal(body, &results) != nil {
			return 0, false
		}
		for _, r := range results {
			tokens += r.MetaInfo.PromptTokens
		}
		return tokens, tokens > 0
	}
	var resp struct {
		Usage *struct {
			PromptTokens int `json:"prompt_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &resp) != nil || resp.Usage == nil {
		return 0, false
	}
	if resp.Usage.PromptTokens > 0 {
		return resp.Usage.PromptTokens, true
	}
	return resp.Usage.TotalTokens, resp.Usage.TotalTokens > 0
}
