// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for the provider-fallback interaction with admission
// accounting: a request served by the cloud provider runs on the operator's
// provider account, not local GPU capacity, so its occupancy reservation must
// be released the moment the request leaves the local pool.
package router_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/provider"
	"hivenet_router/internal/router"
)

// fakeProvider returns a canned provider-stamped response.
type fakeProvider struct{ calls atomic.Int32 }

func (f *fakeProvider) Name() string { return "openai" }
func (f *fakeProvider) Complete(_ context.Context, _ *domain.ChatRequest, _ string) (*domain.ChatResponse, error) {
	f.calls.Add(1)
	return &domain.ChatResponse{
		RawBytes:    []byte(`{"ok":true}`),
		ProcessedBy: "provider:openai",
		Usage:       domain.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}, nil
}

var _ provider.Provider = (*fakeProvider)(nil)

// fakeReservation records reservation lifecycle calls.
type fakeReservation struct {
	released atomic.Bool
	grown    atomic.Int64
}

func (f *fakeReservation) Grow(tokens int) { f.grown.Add(int64(tokens)) }
func (f *fakeReservation) Adjust(int)      {}
func (f *fakeReservation) Release()        { f.released.Store(true) }

var _ domain.Reservation = (*fakeReservation)(nil)

// TestProviderFallback_ReleasesOccupancyReservation verifies the processor
// frees the request's occupancy reservation when routing exhausts the local
// pool and hands the request to the cloud provider — otherwise phantom load
// would sit against the local KV budget for the provider call's duration,
// exactly while the degraded pool needs its admission headroom.
func TestProviderFallback_ReleasesOccupancyReservation(t *testing.T) {
	// No agents for the model → Select fails → the provider fallback fires.
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{}}
	stor := &stubStorage{}
	m := metrics.NewRouterMetrics()
	counters := metrics.NewUniversalCounterStore(stor, m)
	eval := policy.NewEvaluator(stor, counters)
	pol := &policy.Policy{
		RoutingPolicy:    policy.PolicyStep{Strategy: "least-loaded", MaxTries: 1},
		FallbackProvider: &policy.FallbackProvider{Engine: "openai", Model: "gpt-fallback"},
	}
	exec := policy.NewExecutor(lister, eval, pol, 3, 0)

	fp := &fakeProvider{}
	queue := make(chan *domain.PendingRequest, 1)
	p := router.NewRequestProcessor(queue, exec, nil, m, counters, 10,
		map[string]provider.Provider{"openai": fp}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Start(ctx)

	pending := domain.NewPendingRequest("req-fallback", &domain.ChatRequest{Model: testModel}, time.Minute)
	pending.Capability = domain.CapabilityLLM
	pending.Ctx = context.Background()
	res := &fakeReservation{}
	pending.Reservation = res
	queue <- pending

	resp, err := awaitResult(t, pending)
	if err != nil {
		t.Fatalf("provider fallback must serve the request, got error: %v", err)
	}
	if !resp.ServedByProvider() {
		t.Fatalf("response must be provider-stamped, got ProcessedBy=%q", resp.ProcessedBy)
	}
	if fp.calls.Load() != 1 {
		t.Errorf("provider Complete calls = %d, want 1", fp.calls.Load())
	}
	if !res.released.Load() {
		t.Error("the occupancy reservation must be released when the request goes to the provider")
	}
}
