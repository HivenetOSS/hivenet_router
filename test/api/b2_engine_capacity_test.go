// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

// Without admit_budget_tokens the occupancy budget is the KV capacity the
// engines report: a request that does not fit is refused.
func TestB2_BudgetFromEngineKVCapacity(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{}, time.Second)
	h.SetEngineKVCapacity(func(string) int { return 50 })
	holdBudget(t, h, q, b2Body(100), nil)
	c, w := newCtx("/v1/chat/completions", b2Body(100)) // footprint ≈ 104 > 50
	h.Passthrough(c)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429 against the engine's KV capacity, got %d", w.Code)
	}
}

// With no budget from the policy and none from the engines, B2 stays off.
func TestB2_OffWithoutPolicyOrEngineCapacity(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{}, time.Second)
	h.SetEngineKVCapacity(func(string) int { return 0 })
	c, _ := newCtx("/v1/chat/completions", b2Body(100))
	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case p := <-q:
		p.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(2 * time.Second):
		t.Fatal("request was not queued although no budget applies")
	}
	<-done
}

// A policy budget overrides the engine's capacity.
func TestB2_PolicyBudgetOverridesEngine(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 50}, time.Second)
	h.SetEngineKVCapacity(func(string) int { return 1_000_000 })
	holdBudget(t, h, q, b2Body(100), nil)
	c, w := newCtx("/v1/chat/completions", b2Body(100))
	h.Passthrough(c)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429 against the policy budget, got %d", w.Code)
	}
}

// Several routers serve the same backends: each admits its share of the
// budget, so together they never exceed it.
func TestB2_SplitAcrossRouters(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{}, time.Second)
	h.SetEngineKVCapacity(func(string) int { return 300 })
	h.SetRouterShare(func() int { return 3 }) // share = 100
	holdBudget(t, h, q, b2Body(200), nil)
	c, w := newCtx("/v1/chat/completions", b2Body(200))
	h.Passthrough(c)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429: a footprint of ~204 exceeds this router's share of 100, got %d", w.Code)
	}
}

// A model nothing bounds (no policy budget, no KV size from its engines, e.g.
// Ollama) is held to the default requests in flight per replica.
func TestDefaultMaxInflightWhenNothingBounds(t *testing.T) {
	q := make(chan *domain.PendingRequest, 2)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{}, time.Second)
	h.SetEngineKVCapacity(func(string) int { return 0 })
	h.SetDefaultMaxInflight(1) // one replica in this harness
	holdBudget(t, h, q, b2Body(10), nil)
	c, w := newCtx("/v1/chat/completions", b2Body(10))
	h.Passthrough(c)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429 beyond the default requests in flight, got %d", w.Code)
	}
}

// Without the engine's KV size (HIVENET_ROUTER_ADMIT_FROM_ENGINE_KV=false the
// router does not set it) and without a policy budget, B2 stays off and only
// the default request cap applies.
func TestEngineKVBudgetCanBeTurnedOff(t *testing.T) {
	q := make(chan *domain.PendingRequest, 2)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{}, time.Second)
	h.SetDefaultMaxInflight(2)
	holdBudget(t, h, q, b2Body(1_000_000), nil) // no token budget: any size passes
	c, _ := newCtx("/v1/chat/completions", b2Body(1_000_000))
	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for len(q) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("second request not admitted although no token budget applies")
		}
		time.Sleep(time.Millisecond)
	}
	(<-q).Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	(<-q).Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	<-done
}
