// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"

	"github.com/gin-gonic/gin"
)

// servePooling sends body to an embeddings or rerank handler, answers the
// queued request with resp (after letting the test inspect the pending
// request), and returns the context and the recorded response.
func servePooling(t *testing.T, lim auth.RateLimiter, path, body string, inspect func(*domain.PendingRequest), resp *domain.ChatResponse) (*gin.Context, int, http.Header) {
	t.Helper()
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, lim)
	c, w := newCtx(path, []byte(body))
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})
	done := make(chan struct{})
	go func() {
		if path == "/v1/rerank" {
			h.Rerank(c)
		} else {
			h.Embeddings(c)
		}
		close(done)
	}()
	select {
	case pending := <-q:
		if inspect != nil {
			inspect(pending)
		}
		pending.Response <- resp
	case <-done:
		return c, w.Code, w.Header()
	case <-time.After(time.Second):
		t.Fatal("request was neither queued nor answered")
	}
	<-done
	return c, w.Code, w.Header()
}

// TestPooling_ChecksEstimateBeforeSending: an embedding or rerank request is
// checked against the daily budgets with its estimated input tokens before it
// is queued, but not charged: the processor charges the exact tokens after
// the response, and gets the estimate for a backend that reports none. A
// rerank counts the query once per document, and token IDs count as given.
func TestPooling_ChecksEstimateBeforeSending(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		want             int
	}{
		// 23 bytes ≈ 5 tokens at 4 bytes per token, plus 3 special tokens.
		{"embedding text", "/v1/embeddings", `{"model":"e","input":"twenty-three bytes long"}`, 23/4 + 3},
		// 5 token IDs plus 3 special tokens for each of the 2 inputs.
		{"embedding token IDs", "/v1/embeddings", `{"model":"e","input":[[1,2,3],[4,5]]}`, 5 + 2*3},
		// (8+8) + (8+8) bytes for 2 pairs ≈ 8 tokens, plus 3 per pair.
		{"rerank", "/v1/rerank", `{"model":"r","query":"12345678","documents":["abcdefgh","ijklmnop"]}`, 32/4 + 2*3},
	} {
		lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true}
		var pending *domain.PendingRequest
		_, code, hdr := servePooling(t, lim, tc.path, tc.body, func(p *domain.PendingRequest) { pending = p },
			&domain.ChatResponse{RawBytes: []byte(`{}`), Usage: domain.Usage{PromptTokens: 7, TotalTokens: 7}})
		if code != http.StatusOK {
			t.Fatalf("%s: status %d", tc.name, code)
		}
		if lim.inputCalls != 0 {
			t.Errorf("%s: charged at admission (%d calls), want only a check", tc.name, lim.inputCalls)
		}
		if pending.EstimatedInputTokens != tc.want || pending.ChargedInputTokens != 0 {
			t.Errorf("%s: pending estimate %d charged %d, want %d and 0", tc.name, pending.EstimatedInputTokens, pending.ChargedInputTokens, tc.want)
		}
		if pending.TokensPerDay != 1_000_000 {
			t.Errorf("%s: pending TokensPerDay %d, want the key's 1000000 (for the true-up)", tc.name, pending.TokensPerDay)
		}
		if got := hdr.Get("X-RateLimit-Remaining-Tokens"); got != "" {
			t.Errorf("%s: X-RateLimit-Remaining-Tokens %q, want none (the processor set none)", tc.name, got)
		}
	}
}

// TestPooling_RefusedWhenBudgetSpent: a request whose estimate does not fit
// the daily budget gets 429 token_limit_exceeded, is never queued and is not
// charged.
func TestPooling_RefusedWhenBudgetSpent(t *testing.T) {
	lim := &fakeLimiter{remaining: 0, inAllowed: true}
	_, code, _ := servePooling(t, lim, "/v1/embeddings", `{"model":"e","input":"hello"}`, nil, nil)
	if code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", code)
	}
	if lim.inputCalls != 0 {
		t.Errorf("a refused request was charged (%d calls)", lim.inputCalls)
	}
}

// TestPooling_AuditUsage: the audit line gets the kind, the input tokens the
// processor counted, no output tokens, and whether the count is the
// backend's; the remaining budget after the true-up is sent to the client.
func TestPooling_AuditUsage(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, kind string
		estimated              bool
		source                 string
	}{
		{"embedding, backend usage", "/v1/embeddings", `{"model":"e","input":"hello"}`, "embedding", false, "exact"},
		{"rerank, estimated", "/v1/rerank", `{"model":"r","query":"q","documents":["d"]}`, "rerank", true, "estimated"},
	} {
		lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 999}
		c, _, hdr := servePooling(t, lim, tc.path, tc.body,
			func(p *domain.PendingRequest) { p.RemainingTokens = 500 },
			&domain.ChatResponse{RawBytes: []byte(`{}`), Usage: domain.Usage{PromptTokens: 26, TotalTokens: 26}, UsageEstimated: tc.estimated})
		if got := c.GetString("audit_kind"); got != tc.kind {
			t.Errorf("%s: kind %q, want %q", tc.name, got, tc.kind)
		}
		if got, _ := c.Get("audit_input_tokens"); got != int64(26) {
			t.Errorf("%s: input_tokens %v, want 26", tc.name, got)
		}
		if got, _ := c.Get("audit_output_tokens"); got != int64(0) {
			t.Errorf("%s: output_tokens %v, want 0", tc.name, got)
		}
		if got := c.GetString("audit_usage_source"); got != tc.source {
			t.Errorf("%s: usage_source %q, want %q", tc.name, got, tc.source)
		}
		if got := hdr.Get("X-RateLimit-Remaining-Tokens"); got != "500" {
			t.Errorf("%s: X-RateLimit-Remaining-Tokens %q, want the processor's 500", tc.name, got)
		}
	}
}
