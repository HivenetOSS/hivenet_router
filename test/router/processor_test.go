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

// deadlineTimeout is the error the forward path returns when the request deadline
// fires while waiting on a slow-but-healthy agent (the stream read deadline p2phttp
// arms from the request context surfaces as os.ErrDeadlineExceeded / "i/o deadline
// reached").
func deadlineTimeout() error {
	return domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request deadline exceeded while contacting agent", domain.SourceRouter)
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

// A request deadline firing on a slow-but-healthy agent must NOT evict the
// connection: evicting would GOAWAY the agent (it logs "Disconnected, will
// retry in 5s" and re-registers) and truncate every other in-flight stream on
// the connection. The request fails cleanly with request_timeout instead, and
// no doomed retry is made (the deadline is request-scoped: every remaining
// attempt would be dead on arrival).
func TestDispatchDeadline_NoEviction(t *testing.T) {
	var calls atomic.Int32
	queue, fake := newProcessor(t, func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
		a.DecrementLoad()
		calls.Add(1)
		return nil, 5, deadlineTimeout()
	})

	pending := newPending()
	queue <- pending
	_, err := awaitResult(t, pending)

	var re *domain.RouterError
	if !errors.As(err, &re) || re.Code != domain.ErrCodeRequestTimeout {
		t.Fatalf("error = %v, want code %s", err, domain.ErrCodeRequestTimeout)
	}
	if got := fake.count(); got != 0 {
		t.Fatalf("ClosePeer calls = %d, want 0 (a deadline is not a dead connection)", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("forward calls = %d, want 1 (retrying past a request-scoped deadline is doomed)", got)
	}
}

// A request deadline must not penalise the agent's health counters: queueing
// behind long generations is not an agent defect, and a multi-minute RTT would
// poison its SRTT and consecutive-failure streak (a busy-but-healthy agent
// getting excluded is how the 503 no_agents_available cascade happens).
func TestDispatchDeadline_DoesNotPenalizeAgent(t *testing.T) {
	exec, counters, m := newExec(t)
	queue := make(chan *domain.PendingRequest, 1)
	p := router.NewRequestProcessor(queue, exec, nil, m, counters, 10, nil, nil,
		router.WithForwardFunc(func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
			a.DecrementLoad()
			return nil, 5, deadlineTimeout()
		}),
		router.WithPeerCloser(&fakePeerCloser{}))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Start(ctx)

	pending := newPending()
	queue <- pending
	if _, err := awaitResult(t, pending); err == nil {
		t.Fatal("expected the deadline error to reach the client")
	}

	successRate, srtt, consecutiveFails, ok := counters.LiveSnapshot(peer.ID("agent-pid-0001"))
	if !ok {
		// No counter state at all: nothing was recorded against the agent — exactly
		// what we want.
		return
	}
	if consecutiveFails != 0 {
		t.Errorf("consecutiveFails = %d, want 0 after a request deadline", consecutiveFails)
	}
	if srtt != 0 {
		t.Errorf("srtt = %v, want 0 (a deadline must not record a multi-minute RTT)", srtt)
	}
	if successRate != 0 {
		t.Errorf("successRate = %v, want 0 (no successes were recorded)", successRate)
	}
}

// A client-side error from the backend (context_length_exceeded,
// invalid_parameter) is a verdict on the request, not the agent: it must not
// raise the agent's consecutive-failure streak or lower its success rate, or a
// client repeatedly sending over-long prompts could get a healthy agent
// excluded by policy.
func TestDispatchClientError_DoesNotPenalizeAgent(t *testing.T) {
	for _, code := range []domain.ErrorCode{domain.ErrCodeContextLengthExceeded, domain.ErrCodeInvalidParameter} {
		t.Run(string(code), func(t *testing.T) {
			exec, counters, m := newExec(t)
			queue := make(chan *domain.PendingRequest, 1)
			forward := func(_ context.Context, a *domain.Agent, _ *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
				a.DecrementLoad()
				return nil, 5, domain.NewRouterError(code, "rejected by backend", domain.SourceBackend)
			}
			p := router.NewRequestProcessor(queue, exec, nil, m, counters, 10, nil, nil,
				router.WithForwardFunc(forward), router.WithPeerCloser(&fakePeerCloser{}))
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			go p.Start(ctx)

			for range 3 {
				pending := newPending()
				queue <- pending
				if _, err := awaitResult(t, pending); err == nil {
					t.Fatal("expected the backend error to reach the client")
				}
			}

			successRate, _, consecutiveFails, ok := counters.LiveSnapshot(peer.ID("agent-pid-0001"))
			if !ok {
				t.Fatal("no counter state for the agent")
			}
			if consecutiveFails != 0 {
				t.Errorf("consecutiveFails = %d, want 0 after client-side errors", consecutiveFails)
			}
			if successRate != 1 {
				t.Errorf("successRate = %.2f, want 1 after client-side errors", successRate)
			}
		})
	}
}
