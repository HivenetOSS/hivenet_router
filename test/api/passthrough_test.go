// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package api_test contains black-box tests for the generic, allowlisted
// passthrough handler: the allowlist security gate and the front-door daily
// token budget check (prompt + worst-case max_tokens reserved before queueing).
package api_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"

	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

// fakeLimiter lets each test pin the peeked remaining budget and the result of
// the input-token charge; RPM and output checks always allow so they never
// interfere. inputCalls records how many times the (deducting) AllowInputTokens
// was invoked, so a test can assert a rejected request was never charged.
type fakeLimiter struct {
	remaining    int // RemainingTokens peek result
	inAllowed    bool
	inRemaining  int
	inputCalls   int // number of AllowInputTokens (deducting) calls
	chargedInput int // last token count passed to AllowInputTokens
}

func (f *fakeLimiter) AllowRequest(string, string, int) (bool, int, error) { return true, -1, nil }
func (f *fakeLimiter) RemainingTokens(_, _ string, _ int) (int, error)     { return f.remaining, nil }
func (f *fakeLimiter) AllowOutputTokens(string, string, int, int) (bool, int, error) {
	return true, -1, nil
}
func (f *fakeLimiter) Reset() {}
func (f *fakeLimiter) AllowInputTokens(_, _ string, _, count int) (bool, int, error) {
	f.inputCalls++
	f.chargedInput = count
	return f.inAllowed, f.inRemaining, nil
}

var _ auth.RateLimiter = (*fakeLimiter)(nil)

func newTestHandlers(q chan *domain.PendingRequest, lim auth.RateLimiter) *api.Handlers {
	// Passthrough only uses the queue, timeout, and limiter; the remaining
	// dependencies are unused here and may be nil (see NewHandlers doc). The
	// trailing nil is the healthyAgentCount counter — these tests exercise the
	// legacy flat path, which doesn't consult it.
	return api.NewHandlers(
		nil, nil, q, 2*time.Second,
		nil, nil, nil, lim,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil,
	)
}

func newCtx(path string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return c, w
}

// TestPassthrough_RejectsNonAllowlistedPath verifies the allowlist gate: a path
// that is not registered for passthrough is rejected (404) before any routing.
func TestPassthrough_RejectsNonAllowlistedPath(t *testing.T) {
	h := newTestHandlers(make(chan *domain.PendingRequest, 1), &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	c, w := newCtx("/v1/load_lora_adapter", []byte(`{"model":"m"}`))

	h.Passthrough(c)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for non-allowlisted path, got %d", w.Code)
	}
}

// TestPassthrough_FrontDoorRejectsMaxTokens verifies that a request whose
// worst-case output (max_tokens) cannot fit in the remaining budget is rejected
// up front with 429, before it is queued — and crucially WITHOUT being charged
// (the admission check peeks, it does not deduct).
func TestPassthrough_FrontDoorRejectsMaxTokens(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	lim := &fakeLimiter{remaining: 10} // only 10 tokens left
	h := newTestHandlers(q, lim)
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hello there general kenobi"}],"max_tokens":1000}`)
	c, w := newCtx("/v1/chat/completions", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when max_tokens exceeds remaining budget, got %d", w.Code)
	}
	if len(q) != 0 {
		t.Errorf("over-budget request must not be queued, queue depth=%d", len(q))
	}
	if lim.inputCalls != 0 {
		t.Errorf("a rejected request must not be charged; AllowInputTokens called %d times", lim.inputCalls)
	}
}

// TestPassthrough_FrontDoorRejectsWhenBudgetExhausted verifies that when the
// remaining budget cannot cover even the prompt, the request is rejected (429)
// and not charged.
func TestPassthrough_FrontDoorRejectsWhenBudgetExhausted(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	lim := &fakeLimiter{remaining: 0} // nothing left
	h := newTestHandlers(q, lim)
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"a very long prompt"}],"max_tokens":1}`)
	c, w := newCtx("/v1/chat/completions", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when budget is exhausted, got %d", w.Code)
	}
	if lim.inputCalls != 0 {
		t.Errorf("a rejected request must not be charged; AllowInputTokens called %d times", lim.inputCalls)
	}
}

// TestPassthrough_FrontDoorGatesTextlessPrompt verifies that an image/tool-only
// message (no estimable text — GetMessageSlice returns empty, so the raw estimate
// is 0) still passes through the budget gate via the per-message floor, rather
// than bypassing it. With a tiny budget and a large max_tokens it must be
// rejected, not admitted.
func TestPassthrough_FrontDoorGatesTextlessPrompt(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	lim := &fakeLimiter{remaining: 2} // tiny budget
	h := newTestHandlers(q, lim)
	// Content is an image-only block array: no text parts → 0 raw estimate.
	body := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/y.png"}}]}],"max_tokens":1000}`)
	c, w := newCtx("/v1/chat/completions", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("textless prompt must not bypass the budget gate; expected 429, got %d", w.Code)
	}
	if len(q) != 0 {
		t.Errorf("rejected request must not be queued, queue depth=%d", len(q))
	}
}

// TestPassthrough_ChargesMultimodalFloor verifies the token counting on the
// admission path for a textless (image-only) prompt: the raw text estimate is 0,
// so the per-message floor applies and the request is charged exactly
// perMessageTokenOverhead (4) per message — here 1 message → 4 tokens — rather
// than 0. This locks in that multimodal prompts are counted, not free.
func TestPassthrough_ChargesMultimodalFloor(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 999_996}
	h := newTestHandlers(q, lim)
	body := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/y.png"}}]}],"max_tokens":10}`)
	c, w := newCtx("/v1/chat/completions", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()

	select {
	case pending := <-q:
		// 1 message × per-message floor (4) = 4 charged for the textless prompt.
		if lim.chargedInput != 4 {
			t.Errorf("expected floored charge of 4 tokens for a 1-message image prompt, got %d", lim.chargedInput)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("multimodal request was not enqueued")
	}

	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestPassthrough_EnqueuesAllowlistedRequest verifies the happy path: an
// allowlisted, in-budget request is enqueued with the LLM capability and the
// original path, so the processor forwards it to the backend's native endpoint.
func TestPassthrough_EnqueuesAllowlistedRequest(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	body := []byte(`{"model":"qwen","messages":[{"role":"user","content":"hi"}],"max_tokens":10}`)
	c, w := newCtx("/v1/messages", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()

	select {
	case pending := <-q:
		if pending.Capability != domain.CapabilityLLM {
			t.Errorf("expected capability %q, got %q", domain.CapabilityLLM, pending.Capability)
		}
		if pending.Path != "/v1/messages" {
			t.Errorf("expected forward path /v1/messages, got %q", pending.Path)
		}
		if pending.Request.Model != "qwen" {
			t.Errorf("expected model qwen, got %q", pending.Request.Model)
		}
		// Unblock the waiting handler with a minimal response.
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("allowlisted in-budget request was not enqueued")
	}

	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on success, got %d", w.Code)
	}
}

// TestPassthrough_StreamingUpdatesAuditTokens locks in the fix for the audit-log
// bug where streamed responses recorded input_tokens=0 / output_tokens=0. The
// processor's meter goroutine now publishes the real totals on PendingRequest
// before closing the pipe, and the handler reads them after io.Copy completes
// and overwrites the audit context keys. This test feeds a streaming response,
// pre-stamps the streamed totals (simulating what the processor would do), and
// asserts the audit values end up correct.
func TestPassthrough_StreamingUpdatesAuditTokens(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	body := []byte(`{"model":"qwen","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":10}`)
	c, w := newCtx("/v1/chat/completions", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()

	select {
	case pending := <-q:
		// Simulate the processor's streaming-meter goroutine: stash the real
		// token totals on pending BEFORE closing the body (the production
		// processor does this just before its deferred pw.Close() fires).
		pending.StreamedPromptTokens.Store(11)
		pending.StreamedCompletionTokens.Store(22)
		// Hand back a streaming response: zero usage in the parsed struct, body
		// carries the SSE bytes.
		pending.Response <- &domain.ChatResponse{
			Body: io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")),
		}
	case <-time.After(time.Second):
		t.Fatal("streaming request was not enqueued")
	}

	<-done
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// The audit keys must reflect the streamed totals, not the initial zeros.
	gotIn, _ := c.Get("audit_input_tokens")
	gotOut, _ := c.Get("audit_output_tokens")
	if gotIn != int64(11) {
		t.Errorf("expected audit input_tokens=11 after streaming, got %v", gotIn)
	}
	if gotOut != int64(22) {
		t.Errorf("expected audit output_tokens=22 after streaming, got %v", gotOut)
	}
}

// TestPassthrough_AllowsCountTokens verifies /v1/messages/count_tokens is on the
// allowlist and routed like any other inference request (it has a model field
// and no max_tokens), so it reaches an agent rather than being rejected.
func TestPassthrough_AllowsCountTokens(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	body := []byte(`{"model":"qwen","messages":[{"role":"user","content":"how many tokens is this"}]}`)
	c, w := newCtx("/v1/messages/count_tokens", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()

	select {
	case pending := <-q:
		if pending.Path != "/v1/messages/count_tokens" {
			t.Errorf("expected forward path /v1/messages/count_tokens, got %q", pending.Path)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"input_tokens":7}`)}
	case <-time.After(time.Second):
		t.Fatal("count_tokens request was not enqueued (should be allowlisted)")
	}

	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
