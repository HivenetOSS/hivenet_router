// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package router_test contains black-box tests for the request processor's
// connection-eviction behaviour. They exercise only exported symbols and drive
// the processor through its public Start + queue path, injecting a stub forward
// function and a fake peer-closer so no real libp2p networking is required.
package router_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/router"
	"hivenet_router/internal/storage"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

const testModel = "test-model"

// ── stubs ───────────────────────────────────────────────────────────────────

// stubStorage is a no-op RoutingStorage (see testutil.NoopStorage).
type stubStorage struct{ testutil.NoopStorage }

var _ storage.RoutingStorage = (*stubStorage)(nil)

// stubAgentLister returns a fixed agent pool keyed by model name.
type stubAgentLister struct {
	agents map[string][]*domain.Agent
}

func (l *stubAgentLister) ListByModel(model string) []*domain.Agent { return l.agents[model] }

var _ policy.AgentLister = (*stubAgentLister)(nil)

// fakePeerCloser records ClosePeer calls so tests can assert on eviction.
type fakePeerCloser struct {
	mu     sync.Mutex
	closed []peer.ID
}

func (f *fakePeerCloser) ClosePeer(id peer.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = append(f.closed, id)
	return nil
}

func (f *fakePeerCloser) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.closed)
}

var _ router.PeerCloser = (*fakePeerCloser)(nil)

// ── helpers ───────────────────────────────────────────────────────────────────

// newExec builds a single-agent, single-step (max_tries=2) executor plus its counter
// store and metrics, shared by the processor helpers below.
func newExec(t *testing.T) (*policy.Executor, *metrics.UniversalCounterStore, *metrics.RouterMetrics) {
	t.Helper()
	agent := domain.NewAgent(peer.ID("agent-pid-0001"), domain.AgentMetadata{
		Model:      testModel,
		Capacity:   10,
		Capability: domain.CapabilityLLM,
		Engine:     "vllm",
	}, "")
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{testModel: {agent}}}

	stor := &stubStorage{}
	m := metrics.NewRouterMetrics()
	counters := metrics.NewUniversalCounterStore(stor, m)
	counters.Bootstrap(agent.ID, testModel, "vllm", "", "")
	eval := policy.NewEvaluator(stor, counters)
	pol := &policy.Policy{RoutingPolicy: policy.PolicyStep{Strategy: "least-loaded", MaxTries: 2}}
	return policy.NewExecutor(lister, eval, pol, 3, 0), counters, m
}

// startProcessor constructs and starts a processor (stopped via t.Cleanup) with the
// injected forward function and the supplied options, returning its request queue.
// httpHost is nil: the injected forward func means no real networking is exercised.
func startProcessor(t *testing.T, forward router.ForwardFunc, opts ...router.ProcessorOption) chan *domain.PendingRequest {
	t.Helper()
	exec, counters, m := newExec(t)
	queue := make(chan *domain.PendingRequest, 1)
	allOpts := append([]router.ProcessorOption{router.WithForwardFunc(forward)}, opts...)
	p := router.NewRequestProcessor(queue, exec, nil, m, counters, 10, nil, nil, allOpts...)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Start(ctx)
	return queue
}

// newProcessor starts a processor with a fake peer-closer injected, returning the
// queue and the fake so tests can assert on eviction.
func newProcessor(t *testing.T, forward router.ForwardFunc) (chan *domain.PendingRequest, *fakePeerCloser) {
	t.Helper()
	fake := &fakePeerCloser{}
	return startProcessor(t, forward, router.WithPeerCloser(fake)), fake
}

func newPending() *domain.PendingRequest {
	pending := domain.NewPendingRequest("req-1", &domain.ChatRequest{Model: testModel}, time.Minute)
	pending.Capability = domain.CapabilityLLM
	pending.Ctx = context.Background()
	return pending
}

// disconnected is the connection-level error a dead libp2p path produces.
func disconnected() error {
	return domain.NewRouterError(domain.ErrCodeAgentDisconnected, "Agent unreachable: dial failed", domain.SourceRouter)
}

// awaitResult blocks until the request resolves or the test times out.
func awaitResult(t *testing.T, pending *domain.PendingRequest) (*domain.ChatResponse, error) {
	t.Helper()
	select {
	case resp := <-pending.Response:
		return resp, nil
	case err := <-pending.Error:
		return nil, err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the request to resolve")
		return nil, nil
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// A connection-level failure followed by a successful re-dial must close the stale
// connection once and let the request succeed within the same dispatch.
func TestDispatchFreeReconnect_RecoversOnRetry(t *testing.T) {
	var calls atomic.Int32
	queue, fake := newProcessor(t, func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
		a.DecrementLoad() // forwardToAgent owns the slot release; honour the same contract
		if calls.Add(1) == 1 {
			return nil, 5, disconnected()
		}
		return &domain.ChatResponse{ProcessedBy: a.ID.String()}, 5, nil
	})

	pending := newPending()
	queue <- pending
	resp, err := awaitResult(t, pending)

	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected a non-nil response on recovery")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("forward calls = %d, want 2 (1 failed + 1 re-dialed)", got)
	}
	if got := fake.count(); got != 1 {
		t.Fatalf("ClosePeer calls = %d, want 1", got)
	}
}

// When every forward fails with a connection-level error, the free re-dial must NOT
// consume the policy try budget: with a single agent the dispatcher makes 2 forward
// attempts (1 free reconnect + 1 budgeted try) before the step exhausts. If the free
// reconnect wrongly charged the budget, the agent would be marked failed after the
// first attempt and excluded, yielding only 1 forward attempt.
func TestDispatchFreeReconnect_DoesNotBurnBudget(t *testing.T) {
	var calls atomic.Int32
	queue, fake := newProcessor(t, func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
		a.DecrementLoad()
		calls.Add(1)
		return nil, 5, disconnected()
	})

	pending := newPending()
	queue <- pending
	_, err := awaitResult(t, pending)

	var re *domain.RouterError
	if !errors.As(err, &re) || re.Code != domain.ErrCodeNoAgentsAvailable {
		t.Fatalf("error = %v, want code %s", err, domain.ErrCodeNoAgentsAvailable)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("forward calls = %d, want 2 (free reconnect must not consume the try budget)", got)
	}
	if got := fake.count(); got != 1 {
		t.Fatalf("ClosePeer calls = %d, want 1 (one eviction, then the budgeted retry)", got)
	}
}

// An application-level error (e.g. context_length_exceeded) must NOT trigger a
// connection eviction — the agent responded, so the connection is healthy.
func TestDispatchAppError_NoEviction(t *testing.T) {
	queue, fake := newProcessor(t, func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
		a.DecrementLoad()
		return nil, 5, domain.NewRouterError(domain.ErrCodeContextLengthExceeded, "prompt too long", domain.SourceBackend)
	})

	pending := newPending()
	queue <- pending
	_, err := awaitResult(t, pending)

	var re *domain.RouterError
	if !errors.As(err, &re) || re.Code != domain.ErrCodeContextLengthExceeded {
		t.Fatalf("error = %v, want code %s", err, domain.ErrCodeContextLengthExceeded)
	}
	if got := fake.count(); got != 0 {
		t.Fatalf("ClosePeer calls = %d, want 0 for an application-level error", got)
	}
}

// With no PeerCloser injected and a nil httpHost, the constructor must default to a
// no-op closer so a connection-level failure does not panic on the eviction path.
func TestDispatchNilPeerCloser_NoPanic(t *testing.T) {
	queue := startProcessor(t, func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
		a.DecrementLoad()
		return nil, 5, disconnected()
	})

	pending := newPending()
	queue <- pending
	_, err := awaitResult(t, pending) // must resolve (not panic)

	var re *domain.RouterError
	if !errors.As(err, &re) || re.Code != domain.ErrCodeNoAgentsAvailable {
		t.Fatalf("error = %v, want code %s", err, domain.ErrCodeNoAgentsAvailable)
	}
}
