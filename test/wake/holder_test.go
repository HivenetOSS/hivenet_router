// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package wake_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/wake"
)

// holdNow holds like h.Hold and releases a Woken request at once, as if the
// model answered it straight away.
func holdNow(h *wake.Holder, ctx context.Context, model, caller, requestID string, opts wake.Options) wake.Outcome {
	o, release := h.Hold(ctx, model, caller, requestID, opts)
	release()
	return o
}

// fleet is a fake count of healthy agents per model.
type fleet struct {
	mu      sync.Mutex
	healthy map[string]int
}

func (f *fleet) count(model string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy[model]
}

func (f *fleet) set(model string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy[model] = n
}

// ended records every finished hold.
type ended struct {
	mu   sync.Mutex
	list []wake.Outcome
	held []time.Duration
}

func (e *ended) record(_ string, o wake.Outcome, held time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, o)
	e.held = append(e.held, held)
}

func (e *ended) outcomes() []wake.Outcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]wake.Outcome(nil), e.list...)
}

func newHolder(t *testing.T, max int) (*wake.Holder, *fleet, *ended) {
	t.Helper()
	f := &fleet{healthy: map[string]int{}}
	e := &ended{}
	h := wake.NewHolder(wake.Limits{PerModel: max, PerCaller: max, Total: 1000}, f.count, e.record)
	h.SetPollInterval(time.Hour) // only Wake releases, unless a test says otherwise
	return h, f, e
}

// hold starts a hold in the background and waits until it is counted.
func hold(t *testing.T, h *wake.Holder, ctx context.Context, model, key string, opts wake.Options) <-chan wake.Outcome {
	t.Helper()
	before := total(h.Demand())
	out := make(chan wake.Outcome, 1)
	go func() { out <- holdNow(h, ctx, model, "caller", key, opts) }()
	deadline := time.Now().Add(time.Second)
	for total(h.Demand()) == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	return out
}

func total(d map[string]int) int {
	n := 0
	for _, v := range d {
		n += v
	}
	return n
}

func outcome(t *testing.T, out <-chan wake.Outcome, within time.Duration) wake.Outcome {
	t.Helper()
	select {
	case o := <-out:
		return o
	case <-time.After(within):
		t.Fatalf("hold did not end within %v", within)
		return ""
	}
}

var long = wake.Options{Budget: time.Minute}

// TestHold_WakeReleasesEveryHeldRequest checks the main path: many requests
// are held, one agent registers, and every one of them is released at once.
func TestHold_WakeReleasesEveryHeldRequest(t *testing.T) {
	h, f, e := newHolder(t, 100)
	var outs []<-chan wake.Outcome
	for i := range 20 {
		outs = append(outs, hold(t, h, context.Background(), "m", string(rune('a'+i)), long))
	}
	if d := h.Demand()["m"]; d != 20 {
		t.Fatalf("demand = %d, want 20", d)
	}

	f.set("m", 1)
	h.Wake("m")
	for i, out := range outs {
		if o := outcome(t, out, 500*time.Millisecond); o != wake.Woken {
			t.Fatalf("hold %d = %s, want woken", i, o)
		}
	}
	if d := h.Demand(); len(d) != 0 {
		t.Fatalf("demand after release = %v, want empty (no state left behind)", d)
	}
	if n := len(e.outcomes()); n != 20 {
		t.Fatalf("%d holds reported, want 20", n)
	}
}

// TestHold_WakeWithoutHealthyAgentKeepsHolding checks that a wake-up only
// releases requests when an agent is really healthy.
func TestHold_WakeWithoutHealthyAgentKeepsHolding(t *testing.T) {
	h, f, _ := newHolder(t, 10)
	out := hold(t, h, context.Background(), "m", "a", long)

	h.Wake("m") // an agent of another model, or not healthy yet
	select {
	case o := <-out:
		t.Fatalf("hold ended (%s) with no healthy agent", o)
	case <-time.After(50 * time.Millisecond):
	}

	f.set("m", 1)
	h.Wake("m")
	if o := outcome(t, out, 500*time.Millisecond); o != wake.Woken {
		t.Fatalf("hold = %s, want woken", o)
	}
}

// TestHold_PollCatchesRecoveryWithoutWake checks the fallback: an agent that
// becomes healthy without a wake-up is still found by the periodic check.
func TestHold_PollCatchesRecoveryWithoutWake(t *testing.T) {
	h, f, _ := newHolder(t, 10)
	h.SetPollInterval(10 * time.Millisecond)
	out := hold(t, h, context.Background(), "m", "a", long)
	f.set("m", 1)
	if o := outcome(t, out, 500*time.Millisecond); o != wake.Woken {
		t.Fatalf("hold = %s, want woken", o)
	}
}

// TestHold_AlreadyHealthyIsWokenAtOnce checks that a request is never held
// when an agent became healthy between the caller's check and the hold.
func TestHold_AlreadyHealthyIsWokenAtOnce(t *testing.T) {
	h, f, _ := newHolder(t, 10)
	f.set("m", 1)
	start := time.Now()
	if o := holdNow(h, context.Background(), "m", "caller", "a", long); o != wake.Woken {
		t.Fatalf("hold = %s, want woken", o)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("hold took %v, want immediate", d)
	}
}

// TestHold_BudgetExpires checks that a request is held no longer than its
// budget.
func TestHold_BudgetExpires(t *testing.T) {
	h, _, e := newHolder(t, 10)
	start := time.Now()
	o := holdNow(h, context.Background(), "m", "caller", "a", wake.Options{Budget: 50 * time.Millisecond})
	if o != wake.Expired {
		t.Fatalf("hold = %s, want expired", o)
	}
	if d := time.Since(start); d < 50*time.Millisecond || d > 500*time.Millisecond {
		t.Fatalf("hold lasted %v, want about the 50ms budget", d)
	}
	if got := e.outcomes(); len(got) != 1 || got[0] != wake.Expired {
		t.Fatalf("reported %v, want [expired]", got)
	}
}

// TestHold_FullModelRefusesAtOnce checks the per-model cap: a request beyond
// it is not held and not counted in the demand.
func TestHold_FullModelRefusesAtOnce(t *testing.T) {
	h, f, _ := newHolder(t, 2)
	a := hold(t, h, context.Background(), "m", "a", long)
	b := hold(t, h, context.Background(), "m", "b", long)

	if o := holdNow(h, context.Background(), "m", "caller", "c", long); o != wake.Full {
		t.Fatalf("third hold = %s, want full", o)
	}
	if d := h.Demand()["m"]; d != 2 {
		t.Fatalf("demand = %d, want 2 (a refused request is not counted)", d)
	}
	// Another model has its own cap.
	other := hold(t, h, context.Background(), "other", "c", long)

	f.set("m", 1)
	f.set("other", 1)
	h.Wake("m")
	h.Wake("other")
	for _, out := range []<-chan wake.Outcome{a, b, other} {
		if o := outcome(t, out, 500*time.Millisecond); o != wake.Woken {
			t.Fatalf("hold = %s, want woken", o)
		}
	}
}

// TestHold_ClientGone checks that a client that leaves ends its hold and
// leaves the demand.
func TestHold_ClientGone(t *testing.T) {
	h, _, _ := newHolder(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	out := hold(t, h, ctx, "m", "a", long)
	cancel()
	if o := outcome(t, out, 500*time.Millisecond); o != wake.Gone {
		t.Fatalf("hold = %s, want gone", o)
	}
	if d := h.Demand(); len(d) != 0 {
		t.Fatalf("demand = %v, want empty", d)
	}
}

// TestHold_DemandCountsDistinctRequests checks that holds with the same key
// (a client retrying the same request) count once.
func TestHold_DemandCountsDistinctRequests(t *testing.T) {
	h, f, _ := newHolder(t, 10)
	first := hold(t, h, context.Background(), "m", "same", long)
	second := make(chan wake.Outcome, 1)
	go func() { second <- holdNow(h, context.Background(), "m", "caller", "same", long) }()
	third := hold(t, h, context.Background(), "m", "other", long)
	time.Sleep(20 * time.Millisecond)

	if d := h.Demand()["m"]; d != 2 {
		t.Fatalf("demand = %d, want 2 distinct requests", d)
	}
	f.set("m", 1)
	h.Wake("m")
	for _, out := range []<-chan wake.Outcome{first, second, third} {
		if o := outcome(t, out, 500*time.Millisecond); o != wake.Woken {
			t.Fatalf("hold = %s, want woken", o)
		}
	}
}

// TestHold_KeepAlive checks that keepalives start after the interval and
// repeat, and that a failed keepalive ends the hold as gone.
func TestHold_KeepAlive(t *testing.T) {
	h, _, _ := newHolder(t, 10)
	var n atomic.Int64
	o := holdNow(h, context.Background(), "m", "caller", "a", wake.Options{
		Budget:    500 * time.Millisecond,
		KeepAlive: 100 * time.Millisecond,
		OnKeepAlive: func() error {
			n.Add(1)
			return nil
		},
	})
	if o != wake.Expired {
		t.Fatalf("hold = %s, want expired", o)
	}
	if got := n.Load(); got < 3 || got > 5 {
		t.Fatalf("%d keepalives in 500ms at 100ms, want about 4", got)
	}

	o = holdNow(h, context.Background(), "m", "caller", "b", wake.Options{
		Budget:      time.Minute,
		KeepAlive:   10 * time.Millisecond,
		OnKeepAlive: func() error { return errors.New("client closed the connection") },
	})
	if o != wake.Gone {
		t.Fatalf("hold with a failing keepalive = %s, want gone", o)
	}
}

// TestHold_ReportsHeldDuration checks what is reported when a hold ends.
func TestHold_ReportsHeldDuration(t *testing.T) {
	h, f, e := newHolder(t, 1)
	out := hold(t, h, context.Background(), "m", "a", long)
	holdNow(h, context.Background(), "m", "caller", "b", long) // full
	time.Sleep(30 * time.Millisecond)
	f.set("m", 1)
	h.Wake("m")
	outcome(t, out, 500*time.Millisecond)

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.list) != 2 || e.list[0] != wake.Full || e.list[1] != wake.Woken {
		t.Fatalf("reported %v, want [full woken]", e.list)
	}
	if e.held[0] != 0 || e.held[1] < 30*time.Millisecond {
		t.Fatalf("held %v, want 0 for full and at least 30ms for woken", e.held)
	}
}

// TestHold_SameRequestIDFromAnotherCallerCountsSeparately checks that a
// request ID chosen by one client cannot hide another client's demand.
func TestHold_SameRequestIDFromAnotherCallerCountsSeparately(t *testing.T) {
	h, f, _ := newHolder(t, 10)
	outs := []<-chan wake.Outcome{hold(t, h, context.Background(), "m", "same", long)}
	o := make(chan wake.Outcome, 1)
	go func() { o <- holdNow(h, context.Background(), "m", "other-caller", "same", long) }()
	outs = append(outs, o)
	deadline := time.Now().Add(time.Second)
	for h.Demand()["m"] != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if d := h.Demand()["m"]; d != 2 {
		t.Fatalf("demand = %d, want 2: same request ID, different callers", d)
	}
	f.set("m", 1)
	h.Wake("m")
	for _, out := range outs {
		outcome(t, out, 500*time.Millisecond)
	}
}

// TestHold_PerCallerAndTotalLimits checks that one caller cannot take a
// model's whole share, and that the total bounds every model together.
func TestHold_PerCallerAndTotalLimits(t *testing.T) {
	f := &fleet{healthy: map[string]int{}}
	h := wake.NewHolder(wake.Limits{PerModel: 10, PerCaller: 2, Total: 3}, f.count, nil)
	h.SetPollInterval(time.Hour)
	start := func(model, caller, rid string) <-chan wake.Outcome {
		out := make(chan wake.Outcome, 1)
		go func() { out <- holdNow(h, context.Background(), model, caller, rid, long) }()
		return out
	}
	held := []<-chan wake.Outcome{start("m", "k1", "1"), start("m", "k1", "2")}
	time.Sleep(20 * time.Millisecond)
	if o := holdNow(h, context.Background(), "m", "k1", "3", long); o != wake.Full {
		t.Fatalf("third hold of one caller = %s, want full (per-caller limit 2)", o)
	}
	held = append(held, start("m", "k2", "1")) // another caller still fits
	time.Sleep(20 * time.Millisecond)
	if o := holdNow(h, context.Background(), "other", "k3", "1", long); o != wake.Full {
		t.Fatalf("fourth hold overall = %s, want full (total limit 3)", o)
	}
	f.set("m", 1)
	h.Wake("m")
	for _, out := range held {
		if o := outcome(t, out, 500*time.Millisecond); o != wake.Woken {
			t.Fatalf("hold = %s, want woken", o)
		}
	}
	// Released holds free the limits again.
	f.set("other", 1)
	if o := holdNow(h, context.Background(), "other", "k3", "1", long); o != wake.Woken {
		t.Fatalf("hold after the release = %s, want woken", o)
	}
}

// TestHold_WokenCountsUntilReleased checks that a woken request stays in the
// pending demand until its release, but no longer counts against the hold
// limits.
func TestHold_WokenCountsUntilReleased(t *testing.T) {
	h, f, _ := newHolder(t, 1)
	long := wake.Options{Budget: time.Minute}
	f.set("m", 1)
	o, release := h.Hold(context.Background(), "m", "caller", "a", long)
	if o != wake.Woken {
		t.Fatalf("outcome %s, want woken", o)
	}
	if n := h.Demand()["m"]; n != 1 {
		t.Errorf("demand before release = %d, want 1", n)
	}
	// The limit is 1, and the woken request no longer takes it.
	if o := holdNow(h, context.Background(), "m", "caller", "b", long); o != wake.Woken {
		t.Errorf("second hold: %s, want woken (not full)", o)
	}
	release()
	release() // a second call does nothing
	if d := h.Demand(); len(d) != 0 {
		t.Errorf("demand after release = %v, want empty", d)
	}
}
