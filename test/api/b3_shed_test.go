// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package api_test contains black-box tests for the front-door KV-pressure shed:
// when the aggregate live engine pressure for a model breaches the policy's
// shed_if thresholds, new requests are rejected with 429 + Retry-After before
// queueing, while a missing signal or an unset shed_if leaves the gate open.
package api_test

import (
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

func f64(v float64) *float64 { return &v }

// shedPolicy is a policy that sheds above 0.90 KV utilization or 20 waiting.
func shedPolicy() *policy.Policy {
	return &policy.Policy{ShedIf: map[string]policy.ThresholdRule{
		"kv_cache_utilization": {GT: f64(0.90)},
		"waiting_requests":     {GT: f64(20)},
	}}
}

// newB3Handlers builds a Handlers with the given shed policy and an engine-
// pressure provider returning fixed aggregate values.
func newB3Handlers(q chan *domain.PendingRequest, pol *policy.Policy, pressure func(string) (*float64, *float64)) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, pol, 0, 0)
	return api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, pressure, nil, nil,
		nil,
	)
}

// TestB3_KVUtilAboveThreshold_429 verifies a model whose aggregate KV
// utilization is above the shed threshold rejects new requests at the door.
func TestB3_KVUtilAboveThreshold_429(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB3Handlers(q, shedPolicy(), func(string) (*float64, *float64) {
		return f64(0.91), f64(0) // KV over threshold, waiting fine
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when KV utilization is over threshold, got %d", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("a shed 429 must carry a Retry-After header")
	}
	if len(q) != 0 {
		t.Errorf("a shed request must not be queued, depth=%d", len(q))
	}
}

// TestB3_WaitingAboveThreshold_429 verifies the waiting-requests dimension sheds
// independently of KV utilization.
func TestB3_WaitingAboveThreshold_429(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB3Handlers(q, shedPolicy(), func(string) (*float64, *float64) {
		return f64(0.10), f64(21) // KV fine, waiting over threshold
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when waiting requests exceed threshold, got %d", w.Code)
	}
}

// TestB3_BelowThreshold_Passes verifies a model under both thresholds admits
// normally and reaches the queue.
func TestB3_BelowThreshold_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB3Handlers(q, shedPolicy(), func(string) (*float64, *float64) {
		return f64(0.50), f64(5) // both comfortably below threshold
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("a request below the shed thresholds must be admitted")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 below thresholds, got %d", w.Code)
	}
}

// TestB3_NoSignal_Passes verifies the gate fails open when no healthy replica
// reports engine metrics (e.g. a non-vLLM engine) — a missing signal is not
// treated as pressure.
func TestB3_NoSignal_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB3Handlers(q, shedPolicy(), func(string) (*float64, *float64) {
		return nil, nil // no engine snapshot
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("a request must pass when no engine signal is available (fail open)")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with no engine signal, got %d", w.Code)
	}
}

// TestB3_PartialSignal_EvaluatesAvailableDimension verifies that when only one
// metric is reported, the gate still evaluates that dimension (nil KV, high
// waiting → shed) rather than skipping the whole check.
func TestB3_PartialSignal_EvaluatesAvailableDimension(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB3Handlers(q, shedPolicy(), func(string) (*float64, *float64) {
		return nil, f64(21) // KV unavailable, waiting over threshold
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("a present-but-breaching dimension must still shed; got %d", w.Code)
	}
}

// TestB3_ReadsShedIfNotExcludeIf verifies the front-door shed reads the shed_if
// thresholds, independent of the per-agent exclude_if routing gates. A policy
// whose exclude_if would fire at 0.10 but whose shed_if fires at 0.90 must admit
// a request at 0.50 pressure — B3 must not shed on the routing thresholds.
func TestB3_ReadsShedIfNotExcludeIf(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{ExcludeIf: map[string]policy.ThresholdRule{
			"kv_cache_utilization": {GT: f64(0.10)}, // per-agent routing gate — not B3's concern
		}},
		ShedIf: map[string]policy.ThresholdRule{
			"kv_cache_utilization": {GT: f64(0.90)}, // front-door shed
		},
	}
	h := newB3Handlers(q, pol, func(string) (*float64, *float64) {
		return f64(0.50), nil // above exclude_if 0.10, below shed_if 0.90
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("B3 must read shed_if (0.90), not exclude_if (0.10); the request should pass")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 — B3 must not shed on exclude_if thresholds; got %d", w.Code)
	}
}

// TestB3_InertWhenNoShedIf verifies a policy without shed_if never sheds, even
// under extreme reported pressure.
func TestB3_InertWhenNoShedIf(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newB3Handlers(q, &policy.Policy{}, func(string) (*float64, *float64) {
		return f64(0.99), f64(100) // pressure through the roof
	})
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("with no shed_if declared the gate must be inert")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with shed_if unset, got %d", w.Code)
	}
}
