// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for two admission-gate boundaries: the B1 input cap must see
// the same estimate B2 reserves (learned estimator over message text + the
// Anthropic top-level system prompt), and /v1/messages/count_tokens — a
// stateless tokenizer lookup that holds no KV cache — must skip the admission
// gates rather than be billed like an inference call.
package api_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/tokenizer"
)

// newEstimatorHandlers builds a Handlers with a live estimator and admission
// controller, whose executor serves global for every model.
func newEstimatorHandlers(q chan *domain.PendingRequest, ctrl *admission.Controller, global *policy.Policy) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, global, 0, 0)
	return api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		ctrl, nil, nil, nil,
		nil, tokenizer.NewEstimator(),
	)
}

// TestB1_AnthropicSystemPromptCounted verifies the B1 input cap uses the same
// estimate the occupancy budget reserves — including the Anthropic top-level
// system prompt. Before the fix B1 counted only message text with the legacy
// len/4 heuristic, so a huge system prompt sailed under max_input_tokens.
func TestB1_AnthropicSystemPromptCounted(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newEstimatorHandlers(q, admission.NewController(1.0, 0), &policy.Policy{MaxInputTokens: 10})
	// The message alone estimates to ~5 tokens (under the cap of 10); the 400-
	// byte system prompt pushes the estimate far over it.
	body := []byte(`{"model":"m","system":"` + strings.Repeat("s", 400) + `","messages":[{"role":"user","content":"hi"}]}`)
	c, w := newCtx("/v1/messages", body)

	h.Passthrough(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an over-cap system prompt, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), string(domain.ErrCodeInputTooLong)) {
		t.Errorf("expected %q in body, got %s", domain.ErrCodeInputTooLong, w.Body.String())
	}
	if len(q) != 0 {
		t.Errorf("a rejected request must not be queued, depth=%d", len(q))
	}
}

// TestCountTokens_SkipsAdmissionGates verifies /v1/messages/count_tokens passes
// the B1 input cap and the B2 occupancy budget untouched: it is a stateless
// tokenizer lookup, so a prompt too big to INFER must still be countable, and
// counting must never hold KV budget or burn per-key token buckets (a client
// that counts before sending would otherwise be billed twice per request).
func TestCountTokens_SkipsAdmissionGates(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	// Caps tight enough that the same body on /v1/messages would be rejected
	// by B1 (input over 10) and B2 (footprint over budget 5) outright.
	h := newEstimatorHandlers(q, ctrl, &policy.Policy{MaxInputTokens: 10, AdmitBudgetTokens: 5})
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"` + strings.Repeat("a", 400) + `"}]}`)
	c, w := newCtx("/v1/messages/count_tokens", body)

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if sumW, count := ctrl.Occupancy("m"); sumW != 0 || count != 0 {
			t.Errorf("count_tokens must hold no occupancy reservation; got sumW=%d count=%d", sumW, count)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"input_tokens":123}`)}
	case <-time.After(time.Second):
		t.Fatal("count_tokens request was not enqueued — an admission gate rejected it")
	}
	<-done

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body %s)", w.Code, w.Body.String())
	}
}

// TestCountTokens_SkipsDailyBudget verifies /v1/messages/count_tokens neither
// checks nor charges the daily token budget: counting a prompt bigger than the
// remaining budget must still succeed, and the budget must be untouched after —
// a count-then-send client would otherwise pay for every prompt twice.
func TestCountTokens_SkipsDailyBudget(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	lim := auth.NewInMemoryLimiter()
	exec := policy.NewExecutor(nil, nil, &policy.Policy{}, 0, 0)
	h := api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, lim,
		nil, nil, nil, nil, nil, nil, nil, nil,
		admission.NewController(1.0, 0), nil, nil, nil,
		nil, tokenizer.NewEstimator(),
	)
	// The 400-byte prompt estimates to ~125 tokens — far over a 10-token daily
	// budget, so the same body on /v1/messages would be a 429 token_limit_exceeded.
	const tpd = 10
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"` + strings.Repeat("a", 400) + `"}]}`)
	c, w := newCtx("/v1/messages/count_tokens", body)
	c.Set("tenant_id", "t1")
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: tpd})

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"input_tokens":123}`)}
	case <-time.After(time.Second):
		t.Fatal("count_tokens request was not enqueued — the daily budget rejected it")
	}
	<-done

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body %s)", w.Code, w.Body.String())
	}
	if remaining, err := lim.RemainingTokens("t1", "", tpd); err != nil || remaining != tpd {
		t.Errorf("count_tokens must not charge the daily budget; remaining=%d (want %d), err=%v", remaining, tpd, err)
	}
}
