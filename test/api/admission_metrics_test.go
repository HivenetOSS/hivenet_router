// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package api_test verifies that every admission gate reports its distinct
// rejection reason to the metrics callback, so "429 by reason" dashboards can
// attribute each reject to the gate that produced it.
package api_test

import (
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

// meteredHandler builds a Handlers wired with the given admission dependencies
// and a reject callback that records the reason of the last rejection.
func meteredHandler(q chan *domain.PendingRequest, pol *policy.Policy, keyAdm *admission.Controller,
	minLim *auth.MinuteRateLimiter, pressure func(string) (*float64, *float64), reason *string,
) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, pol, 0, 0)
	return api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		admission.NewController(1.0, 0), pressure, keyAdm, minLim,
		func(r, _ string) { *reason = r },
	)
}

// TestAdmissionRejectReason drives each gate to reject and asserts the metrics
// callback fired with that gate's distinct reason label.
func TestAdmissionRejectReason(t *testing.T) {
	t.Run("b1", func(t *testing.T) {
		var reason string
		h := meteredHandler(make(chan *domain.PendingRequest, 1), &policy.Policy{MaxInputTokens: 10}, nil, nil, nil, &reason)
		c, w := newCtx("/v1/chat/completions", textBody(28, 0)) // input 11 > 10
		h.Passthrough(c)
		assertReason(t, w.Code, http.StatusBadRequest, reason, "b1")
	})

	t.Run("b2", func(t *testing.T) {
		var reason string
		h := meteredHandler(make(chan *domain.PendingRequest, 1), &policy.Policy{AdmitBudgetTokens: 5}, nil, nil, nil, &reason)
		c, w := newCtx("/v1/chat/completions", b2Body(500)) // footprint 504 > 5
		h.Passthrough(c)
		assertReason(t, w.Code, http.StatusTooManyRequests, reason, "b2")
	})

	t.Run("b3", func(t *testing.T) {
		var reason string
		pol := &policy.Policy{ShedIf: map[string]policy.ThresholdRule{"kv_cache_utilization": {GT: f64(0.90)}}}
		h := meteredHandler(make(chan *domain.PendingRequest, 1), pol, nil, nil,
			func(string) (*float64, *float64) { return f64(0.99), nil }, &reason)
		c, w := newCtx("/v1/chat/completions", b2Body(0))
		h.Passthrough(c)
		assertReason(t, w.Code, http.StatusTooManyRequests, reason, "b3")
	})

	t.Run("b4_occupancy", func(t *testing.T) {
		var reason string
		h := meteredHandler(make(chan *domain.PendingRequest, 1), serverless(1000), admission.NewController(1.0, 0), nil, nil, &reason)
		c, w := newCtx("/v1/chat/completions", b2Body(500)) // footprint 504 > share budget 400
		withKey(c, auth.QuotaLimits{MaxOccupancyShare: 0.40})
		h.Passthrough(c)
		assertReason(t, w.Code, http.StatusTooManyRequests, reason, "b4_occupancy")
	})

	t.Run("b4_itpm", func(t *testing.T) {
		var reason string
		h := meteredHandler(make(chan *domain.PendingRequest, 1), serverless(0), nil, auth.NewMinuteRateLimiter(), nil, &reason)
		c, w := newCtx("/v1/chat/completions", textBody(40, 0)) // input 14 > ITPM 10
		withKey(c, auth.QuotaLimits{InputTokensPerMinute: 10})
		h.Passthrough(c)
		assertReason(t, w.Code, http.StatusTooManyRequests, reason, "b4_itpm")
	})

	t.Run("b4_otpm", func(t *testing.T) {
		var reason string
		minLim := auth.NewMinuteRateLimiter()
		minLim.ChargeOutputTokens("k1", b2Model, 100, 100) // drain the key's OTPM bucket for this model
		h := meteredHandler(make(chan *domain.PendingRequest, 1), serverless(0), nil, minLim, nil, &reason)
		c, w := newCtx("/v1/chat/completions", b2Body(0))
		withKey(c, auth.QuotaLimits{OutputTokensPerMinute: 100})
		h.Passthrough(c)
		assertReason(t, w.Code, http.StatusTooManyRequests, reason, "b4_otpm")
	})
}

func assertReason(t *testing.T, gotCode, wantCode int, gotReason, wantReason string) {
	t.Helper()
	if gotCode != wantCode {
		t.Fatalf("status = %d, want %d", gotCode, wantCode)
	}
	if gotReason != wantReason {
		t.Errorf("reject reason = %q, want %q", gotReason, wantReason)
	}
}
