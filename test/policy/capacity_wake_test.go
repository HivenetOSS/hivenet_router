// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
)

// mutableLister is an AgentLister whose pool can change while sessions wait.
type mutableLister struct {
	mu     sync.Mutex
	agents map[string][]*domain.Agent
}

func (l *mutableLister) ListByModel(model string) []*domain.Agent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*domain.Agent(nil), l.agents[model]...)
}

func (l *mutableLister) add(model string, a *domain.Agent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.agents[model] = append(l.agents[model], a)
}

func healthyAgent(id string, capacity int) *domain.Agent {
	a := makeAgent(id, domain.AgentMetadata{Model: "m", Capacity: capacity, Capability: domain.CapabilityLLM})
	a.SetHealthy(true)
	a.SetBackendHealthy(true)
	return a
}

// newQueuedExec builds an executor with the wait queue enabled.
func newQueuedExec(lister policy.AgentLister) *policy.Executor {
	stor := &stubStorage{}
	counters := metrics.NewUniversalCounterStore(stor, nil)
	pol := &policy.Policy{RoutingPolicy: policy.PolicyStep{Strategy: "least-loaded", MaxTries: 1}}
	return policy.NewExecutor(lister, policy.NewEvaluator(stor, counters), pol, 3, 30)
}

// parkSelects starts n sessions that all find the pool full and park in the
// wait queue. It returns the channel each selected agent ID arrives on.
func parkSelects(t *testing.T, exec *policy.Executor, n int) <-chan string {
	t.Helper()
	got := make(chan string, n)
	for range n {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			a, err := exec.NewSession("m", domain.CapabilityLLM).Select(ctx)
			if err != nil {
				got <- "error: " + err.Error()
				return
			}
			got <- string(a.ID)
		}()
	}
	time.Sleep(50 * time.Millisecond) // let every session park
	return got
}

// TestBroadcastCapacity_NewAgentWakesEveryWaiter checks the registration
// path: a new agent brings several slots, and every waiting request must get
// one at once, not only the first.
func TestBroadcastCapacity_NewAgentWakesEveryWaiter(t *testing.T) {
	full := healthyAgent("full", 1)
	full.TryAcquireSlot() // the only slot is taken
	lister := &mutableLister{agents: map[string][]*domain.Agent{"m": {full}}}
	exec := newQueuedExec(lister)

	got := parkSelects(t, exec, 3)
	lister.add("m", healthyAgent("new", 3))
	exec.BroadcastCapacity("m")

	deadline := time.After(500 * time.Millisecond)
	for i := range 3 {
		select {
		case id := <-got:
			if id != "new" {
				t.Fatalf("waiter %d got %q, want the new agent", i, id)
			}
		case <-deadline:
			t.Fatalf("only %d of 3 waiters were woken by the new agent", i)
		}
	}
}

// TestSignalCapacity_FreedSlotWakesOneWaiter checks the release path: one
// freed slot wakes exactly one waiter, the others stay parked.
func TestSignalCapacity_FreedSlotWakesOneWaiter(t *testing.T) {
	a := healthyAgent("a", 1)
	a.TryAcquireSlot()
	lister := &mutableLister{agents: map[string][]*domain.Agent{"m": {a}}}
	exec := newQueuedExec(lister)

	got := parkSelects(t, exec, 2)
	a.DecrementLoad()
	exec.SignalCapacity("m")

	select {
	case id := <-got:
		if id != "a" {
			t.Fatalf("woken waiter got %q, want a", id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("the freed slot woke no waiter")
	}
	select {
	case id := <-got:
		t.Fatalf("a second waiter returned %q, but only one slot was freed", id)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestReleasedFromHold_SlotCallsHook checks that a request released by a
// wake is told when selection gives it an agent slot, which takes it out of
// the pending demand, and not before.
func TestReleasedFromHold_SlotCallsHook(t *testing.T) {
	full := healthyAgent("full", 1)
	full.TryAcquireSlot() // the only slot is taken
	lister := &mutableLister{agents: map[string][]*domain.Agent{"m": {full}}}
	exec := newQueuedExec(lister)

	hooked := make(chan struct{})
	ctx, cancel := context.WithTimeout(
		policy.WithReleasedFromHold(context.Background(), func() { close(hooked) }), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := exec.NewSession("m", domain.CapabilityLLM).Select(ctx)
		done <- err
	}()

	select {
	case <-hooked:
		t.Fatal("hook called while the request still waits for a slot")
	case <-time.After(50 * time.Millisecond):
	}
	full.DecrementLoad()
	exec.SignalCapacity("m")
	if err := <-done; err != nil {
		t.Fatalf("Select: %v", err)
	}
	select {
	case <-hooked:
	case <-time.After(time.Second):
		t.Fatal("hook not called after the request got a slot")
	}
}
