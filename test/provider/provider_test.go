// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package provider_test contains black-box tests for the closed-source provider
// fallback adapters (OpenAI and Anthropic). Each adapter is exercised against an
// httptest server via the injectable Config.BaseURL seam, so no real API is
// contacted. Coverage focuses on: request forwarding + model override, auth
// headers, response translation, stop-reason mapping, and error paths.
package provider_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/provider"
)

// chatReq builds a minimal ChatRequest with the given model and messages.
func chatReq(model string, msgs ...domain.ChatCompletionMessage) *domain.ChatRequest {
	return &domain.ChatRequest{Model: model, Messages: msgs}
}

// userMsg is shorthand for a single user-role message.
func userMsg(text string) domain.ChatCompletionMessage {
	return domain.ChatCompletionMessage{Role: "user", Content: text}
}

// --- OpenAI ---------------------------------------------------------------

func TestOpenAI_Complete_Success(t *testing.T) {
	// Capture what the adapter actually sends upstream so we can assert on the
	// forwarded auth header, path, and request body.
	var gotAuth, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		// Canned OpenAI-shaped success response the adapter must translate.
		io.WriteString(w, `{"id":"cmpl-1","object":"chat.completion","model":"gpt-4o",
			"choices":[{"index":0,"message":{"role":"assistant","content":"hi there"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`)
	}))
	defer srv.Close()

	// BaseURL points at the test server, so no real API is contacted.
	p, err := provider.New(provider.Config{Name: "openai", APIKey: "sk-test", BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.Name() != "openai" {
		t.Fatalf("Name = %q, want openai", p.Name())
	}

	// Original request model is "orig"; the fallback overrides it to "gpt-4o".
	resp, err := p.Complete(context.Background(), chatReq("orig", userMsg("hello")), "gpt-4o")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Adapter must hit the standard chat-completions endpoint.
	if gotPath != "/v1/chat/completions" {
		t.Errorf("path = %q, want /v1/chat/completions", gotPath)
	}
	// API key must be forwarded as a Bearer token.
	if gotAuth != "Bearer sk-test" {
		t.Errorf("Authorization = %q, want Bearer sk-test", gotAuth)
	}
	// The override arg, not the request's original model, must be sent upstream.
	if gotBody["model"] != "gpt-4o" {
		t.Errorf("forwarded model = %v, want gpt-4o (override)", gotBody["model"])
	}
	// Response must be stamped with the provenance tag.
	if resp.ProcessedBy != "provider:openai" {
		t.Errorf("ProcessedBy = %q, want provider:openai", resp.ProcessedBy)
	}
	// Choice content is passed through unchanged.
	if len(resp.Choices) != 1 || resp.Choices[0].Message.Content != "hi there" {
		t.Errorf("unexpected choices: %+v", resp.Choices)
	}
	// Usage is copied from the upstream response (5+2=7).
	if resp.Usage.TotalTokens != 7 {
		t.Errorf("TotalTokens = %d, want 7", resp.Usage.TotalTokens)
	}
}

// TestOpenAI_Complete_HTTPError: a non-2xx upstream status must surface as an
// error carrying the status code, not be silently swallowed.
func TestOpenAI_Complete_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":"boom"}`)
	}))
	defer srv.Close()

	p, _ := provider.New(provider.Config{Name: "openai", APIKey: "k", BaseURL: srv.URL})
	_, err := p.Complete(context.Background(), chatReq("m", userMsg("x")), "m")
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("want HTTP 500 error, got %v", err)
	}
}

// TestOpenAI_Complete_NoChoices: a well-formed 200 with an empty choices array
// is still unusable and must be reported as an error.
func TestOpenAI_Complete_NoChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"id":"x","object":"chat.completion","choices":[]}`)
	}))
	defer srv.Close()

	p, _ := provider.New(provider.Config{Name: "openai", APIKey: "k", BaseURL: srv.URL})
	_, err := p.Complete(context.Background(), chatReq("m", userMsg("x")), "m")
	if err == nil || !strings.Contains(err.Error(), "no choices") {
		t.Fatalf("want no-choices error, got %v", err)
	}
}

// --- Anthropic ------------------------------------------------------------

// TestAnthropic_Complete_SuccessAndTranslation covers the full Messages-API
// translation: OpenAI-style request in, Anthropic wire format out, and the
// Anthropic response mapped back to the OpenAI-shaped domain response.
func TestAnthropic_Complete_SuccessAndTranslation(t *testing.T) {
	// Capture Anthropic-specific auth/version headers and the translated body.
	var gotKey, gotVersion string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		// Canned Anthropic Messages-API response for the adapter to translate.
		io.WriteString(w, `{"id":"msg-1","model":"claude","stop_reason":"end_turn",
			"content":[{"type":"text","text":"hello from claude"}],
			"usage":{"input_tokens":11,"output_tokens":4}}`)
	}))
	defer srv.Close()

	p, _ := provider.New(provider.Config{Name: "anthropic", APIKey: "ak", BaseURL: srv.URL})
	if p.Name() != "anthropic" {
		t.Fatalf("Name = %q", p.Name())
	}

	// Mixed system + user request: system content must be lifted into the
	// top-level "system" field, not left inline in the messages array.
	req := chatReq("orig",
		domain.ChatCompletionMessage{Role: "system", Content: "be brief"},
		userMsg("hi"),
	)
	resp, err := p.Complete(context.Background(), req, "claude-3")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Request translation assertions.
	// x-api-key must carry the raw key and the version header must be set.
	if gotKey != "ak" || gotVersion == "" {
		t.Errorf("headers: x-api-key=%q anthropic-version=%q", gotKey, gotVersion)
	}
	if gotBody["model"] != "claude-3" {
		t.Errorf("forwarded model = %v, want claude-3 (override)", gotBody["model"])
	}
	if gotBody["system"] != "be brief" {
		t.Errorf("system field = %v, want 'be brief' (extracted from system message)", gotBody["system"])
	}
	// max_tokens not set in the request → default filled in.
	if mt, _ := gotBody["max_tokens"].(float64); mt != 4096 {
		t.Errorf("max_tokens = %v, want default 4096", gotBody["max_tokens"])
	}

	// Response translation assertions.
	// Provenance tag identifies the fallback provider.
	if resp.ProcessedBy != "provider:anthropic" {
		t.Errorf("ProcessedBy = %q", resp.ProcessedBy)
	}
	// Text block is flattened into the choice message content.
	if resp.Choices[0].Message.Content != "hello from claude" {
		t.Errorf("content = %q", resp.Choices[0].Message.Content)
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %q, want stop (end_turn maps to stop)", resp.Choices[0].FinishReason)
	}
	// Anthropic input/output tokens map to prompt/completion; total is derived (11+4).
	if resp.Usage.PromptTokens != 11 || resp.Usage.CompletionTokens != 4 || resp.Usage.TotalTokens != 15 {
		t.Errorf("usage = %+v, want 11/4/15", resp.Usage)
	}
}

// TestAnthropic_StopReasonMapping locks in the stop_reason → finish_reason
// translation table (Anthropic vocabulary on the left, OpenAI on the right).
func TestAnthropic_StopReasonMapping(t *testing.T) {
	cases := map[string]string{
		"max_tokens": "length",     // truncated by token limit
		"tool_use":   "tool_calls", // model wants to call a tool
		"end_turn":   "stop",       // natural completion
		"stop":       "stop",       // already OpenAI-style, passes through
	}
	for anthropicStop, wantFinish := range cases {
		t.Run(anthropicStop, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, `{"id":"x","model":"c","stop_reason":"`+anthropicStop+`",
					"content":[{"type":"text","text":"t"}],"usage":{"input_tokens":1,"output_tokens":1}}`)
			}))
			defer srv.Close()
			p, _ := provider.New(provider.Config{Name: "anthropic", APIKey: "k", BaseURL: srv.URL})
			resp, err := p.Complete(context.Background(), chatReq("m", userMsg("x")), "c")
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if resp.Choices[0].FinishReason != wantFinish {
				t.Errorf("stop_reason %q → finish %q, want %q", anthropicStop, resp.Choices[0].FinishReason, wantFinish)
			}
		})
	}
}

// TestAnthropic_NoTextBlock: a response whose content has no text block yields
// nothing to return as message content, so Complete must error.
func TestAnthropic_NoTextBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only a tool_use block, no text.
		io.WriteString(w, `{"id":"x","model":"c","stop_reason":"tool_use",
			"content":[{"type":"tool_use","text":""}],"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()
	p, _ := provider.New(provider.Config{Name: "anthropic", APIKey: "k", BaseURL: srv.URL})
	_, err := p.Complete(context.Background(), chatReq("m", userMsg("x")), "c")
	if err == nil || !strings.Contains(err.Error(), "no text content") {
		t.Fatalf("want no-text-content error, got %v", err)
	}
}

func TestAnthropic_RejectsSystemOnlyRequest(t *testing.T) {
	// A request with only a system message has no non-system messages; Anthropic
	// requires at least one, so Complete must fail before any HTTP call.
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer srv.Close()

	p, _ := provider.New(provider.Config{Name: "anthropic", APIKey: "k", BaseURL: srv.URL})
	req := chatReq("m", domain.ChatCompletionMessage{Role: "system", Content: "only system"})
	_, err := p.Complete(context.Background(), req, "c")
	if err == nil || !strings.Contains(err.Error(), "non-system message") {
		t.Fatalf("want non-system-message error, got %v", err)
	}
	if called {
		t.Error("HTTP call must not be made for an invalid request")
	}
}

// --- Factory --------------------------------------------------------------

// TestFactory covers provider.New validation and the IsSupported registry.
func TestFactory(t *testing.T) {
	// A missing API key is a misconfiguration and must fail construction.
	if _, err := provider.New(provider.Config{Name: "openai", APIKey: ""}); err == nil {
		t.Error("empty APIKey must be rejected")
	}
	// An unregistered provider name must fail construction.
	if _, err := provider.New(provider.Config{Name: "grok", APIKey: "k"}); err == nil {
		t.Error("unknown provider must be rejected")
	}
	// Only the two shipped adapters are advertised as supported.
	if !provider.IsSupported("openai") || !provider.IsSupported("anthropic") {
		t.Error("openai/anthropic must be supported")
	}
	if provider.IsSupported("grok") {
		t.Error("grok must not be supported")
	}
}
