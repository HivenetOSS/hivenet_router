// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package api_test contains black-box tests for the serverless per-key caps
// (B4): the per-key occupancy share and the input-token-per-minute rate, plus
// the guarantee that a reserved-mode replica bypasses all of them.
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

	"github.com/gin-gonic/gin"
)

// newB4Handlers builds a Handlers with a global occupancy controller (sized so
// only the per-key caps bind), a per-key occupancy controller, and a per-minute
// (ITPM/OTPM) limiter, serving pol for every model.
func newB4Handlers(q chan *domain.PendingRequest, pol *policy.Policy) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, pol, 0, 0)
	return api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		admission.NewController(1.0, 0), // global budget (fraction 1)
		nil,                             // engine pressure
		admission.NewController(1.0, 0), // per-key occupancy share
		auth.NewMinuteRateLimiter(),     // ITPM/OTPM
		nil,                             // admission reject callback
	)
}

// withKey stamps the caller identity and resolved quota limits a serverless
// request would carry.
func withKey(c *gin.Context, limits auth.QuotaLimits) {
	c.Set("tenant_id", "t1")
	c.Set("key_id", "k1")
	c.Set("quota_limits", limits)
}

func serverless(admitBudget int) *policy.Policy {
	return &policy.Policy{Mode: policy.ModeServerless, AdmitBudgetTokens: admitBudget}
}

// TestB4_PerKeyOccupancyShare_DeniesOverShare verifies a key whose in-flight
// footprint would exceed max_occupancy_share × admit_budget_tokens is denied with
// 429 rate_limit_exceeded, even though the global budget has room.
func TestB4_PerKeyOccupancyShare_DeniesOverShare(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB4Handlers(q, serverless(1000)) // global budget 1000
	// share 0.40 → per-key budget 400; footprint = input(4) + max_tokens(500) = 504 > 400.
	c, w := newCtx("/v1/chat/completions", b2Body(500))
	withKey(c, auth.QuotaLimits{MaxOccupancyShare: 0.40})

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 over per-key share, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), string(domain.ErrCodeRateLimitExceeded)) {
		t.Errorf("expected %q in body, got %s", domain.ErrCodeRateLimitExceeded, w.Body.String())
	}
	if len(q) != 0 {
		t.Errorf("a denied request must not be queued, depth=%d", len(q))
	}
}

// TestB4_WithinShare_Passes verifies a request within the per-key share admits.
func TestB4_WithinShare_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB4Handlers(q, serverless(1000))
	// footprint = 4 + 300 = 304 <= per-key budget 400.
	c, w := newCtx("/v1/chat/completions", b2Body(300))
	withKey(c, auth.QuotaLimits{MaxOccupancyShare: 0.40})

	runAndRespond(t, h, c, q)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 within the per-key share, got %d", w.Code)
	}
}

// TestB4_ITPMDeniesOverRate verifies the per-key input-token-per-minute cap
// denies a request whose input exceeds the remaining rate with 429.
func TestB4_ITPMDeniesOverRate(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB4Handlers(q, serverless(0)) // no occupancy budget; only ITPM gates
	// content 40 chars → input estimate 40/4 + 4 = 14 tokens, over the ITPM burst of 10.
	c, w := newCtx("/v1/chat/completions", textBody(40, 0))
	withKey(c, auth.QuotaLimits{InputTokensPerMinute: 10})

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 over ITPM, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), string(domain.ErrCodeRateLimitExceeded)) {
		t.Errorf("expected %q in body, got %s", domain.ErrCodeRateLimitExceeded, w.Body.String())
	}
}

// TestB4_ReservedModeBypassesAllCaps verifies a reserved-mode replica ignores all
// four B4 caps: a request that would breach the per-key share AND the ITPM cap is
// admitted anyway (only the global circuit breaker applies on reserved replicas).
func TestB4_ReservedModeBypassesAllCaps(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	// Reserved (default mode) with a large global budget so B2 doesn't gate.
	h := newB4Handlers(q, &policy.Policy{AdmitBudgetTokens: 1_000_000})
	// These caps WOULD deny under serverless: tiny share and tiny ITPM.
	c, w := newCtx("/v1/chat/completions", textBody(40, 500)) // input 14, footprint 514
	withKey(c, auth.QuotaLimits{MaxOccupancyShare: 0.0001, InputTokensPerMinute: 2, OutputTokensPerMinute: 2})

	runAndRespond(t, h, c, q)
	if w.Code != http.StatusOK {
		t.Errorf("reserved mode must bypass all B4 caps; expected 200, got %d", w.Code)
	}
}

// TestB4_FullContextRequestPassesAllCaps verifies a legitimate request passes all
// serverless caps when they are sized to admit it (D16: never throttle legit
// traffic) — the caps admit a normal request rather than clamping it.
func TestB4_FullContextRequestPassesAllCaps(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB4Handlers(q, serverless(1_000_000)) // roomy global + per-key budgets
	c, w := newCtx("/v1/chat/completions", textBody(40, 100))
	withKey(c, auth.QuotaLimits{
		MaxOccupancyShare:     0.64,
		InputTokensPerMinute:  519_540,
		OutputTokensPerMinute: 47_424,
	})

	runAndRespond(t, h, c, q)
	if w.Code != http.StatusOK {
		t.Errorf("a legit serverless request must pass all B4 caps; expected 200, got %d", w.Code)
	}
}

// TestB4_ITPMDenialDoesNotChargeDailyBudget verifies the gate order: a request
// rejected by the per-key input-rate cap must not have spent the tenant's daily
// token budget (the per-minute caps are enforced before the daily charge).
func TestB4_ITPMDenialDoesNotChargeDailyBudget(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	exec := policy.NewExecutor(nil, nil, serverless(0), 0, 0)
	lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000}
	h := api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, lim,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, auth.NewMinuteRateLimiter(), nil,
	)
	c, w := newCtx("/v1/chat/completions", textBody(40, 0)) // input 14 > ITPM burst 10
	withKey(c, auth.QuotaLimits{TokensPerDay: 1_000_000, InputTokensPerMinute: 10})

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 from ITPM, got %d", w.Code)
	}
	if lim.inputCalls != 0 {
		t.Errorf("an ITPM-denied request must not charge the daily budget; AllowInputTokens called %d times", lim.inputCalls)
	}
}

// runAndRespond runs Passthrough in a goroutine, answers the queued request, and
// waits for completion — the shared drive-to-success helper for admit tests.
func runAndRespond(t *testing.T, h *api.Handlers, c *gin.Context, q chan *domain.PendingRequest) {
	t.Helper()
	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
}
