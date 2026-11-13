// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// messageStartProber is implemented by engines that can render a chat through
// their chat template: the agent learns the token that opens every message.
type messageStartProber interface {
	MessageStartToken(ctx context.Context, baseURL string, httpClient *http.Client, model string) (int, error)
}

// chatTokenizer is implemented by engines that can tokenize a chat request
// body with its chat template, as they would serve it.
type chatTokenizer interface {
	TokenizeChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, body []byte) ([]int, error)
}

// probeMessageStart renders one and three messages and returns the token that
// opens every message: it occurs exactly twice more with three messages than
// with one (whether or not the engine appends a generation prompt), and of
// such tokens it is the first to occur (an end-of-message token follows it).
func probeMessageStart(render func([]map[string]string) ([]int, error)) (int, error) {
	one, err := render([]map[string]string{{"role": "user", "content": "ping"}})
	if err != nil {
		return -1, err
	}
	three, err := render([]map[string]string{{"role": "user", "content": "ping"}, {"role": "assistant", "content": "pong"}, {"role": "user", "content": "again"}})
	if err != nil {
		return -1, err
	}
	return messageStartFromProbes(one, three)
}

// messageStartFromProbes picks, among the tokens of one in order of first
// occurrence, the first that occurs exactly twice more in three.
func messageStartFromProbes(one, three []int) (int, error) {
	seen := map[int]bool{}
	for _, t := range one {
		if seen[t] {
			continue
		}
		seen[t] = true
		if countOf(three, t) == countOf(one, t)+2 {
			return t, nil
		}
	}
	return -1, fmt.Errorf("probe: messages have no common opening token")
}

func countOf(v []int, x int) int {
	n := 0
	for _, t := range v {
		if t == x {
			n++
		}
	}
	return n
}

// postTokens POSTs body as JSON to url and returns the "tokens" of the answer.
func postTokens(ctx context.Context, httpClient *http.Client, url string, body any) ([]int, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tokenize: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Tokens []int `json:"tokens"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return out.Tokens, nil
}

// chatParts are the fields of a chat request body that its rendering depends on.
type chatParts struct {
	Messages             json.RawMessage `json:"messages"`
	Tools                json.RawMessage `json:"tools,omitempty"`
	AddGenerationPrompt  *bool           `json:"add_generation_prompt,omitempty"`
	ContinueFinalMessage *bool           `json:"continue_final_message,omitempty"`
}

// tokenizeChatBody tokenizes a chat request body through an OpenAI-style
// tokenize endpoint that applies the chat template (vLLM, SGLang).
func tokenizeChatBody(ctx context.Context, httpClient *http.Client, url, model string, body []byte) ([]int, error) {
	var p chatParts
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, err
	}
	req := map[string]any{"model": model, "messages": p.Messages}
	if len(p.Tools) > 0 && string(p.Tools) != "null" {
		req["tools"] = p.Tools
	}
	if p.AddGenerationPrompt != nil {
		req["add_generation_prompt"] = *p.AddGenerationPrompt
	}
	if p.ContinueFinalMessage != nil {
		req["continue_final_message"] = *p.ContinueFinalMessage
	}
	return postTokens(ctx, httpClient, url, req)
}
