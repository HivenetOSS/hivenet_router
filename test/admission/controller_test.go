// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package admission_test exercises the KV-occupancy admit budget through its
// exported surface: the weighted in-flight counter, the admit fraction, the
// max-inflight backstop, the park-then-reject behaviour, and reservation
// release/grow — all the accounting the request path depends on.
package admission_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/admission"
)

const model = "gemma-4-31b"

// TestThreeConcurrent250K_TwoRejected is the worked example from the spec: with a
// ~409K budget, one 250K request fits and two more are rejected, and the weighted
// sum never exceeds the budget.
func TestThreeConcurrent250K_TwoRejected(t *testing.T) {
	c := admission.NewController(1.0, 0) // no fraction scaling, no parking
	ctx := context.Background()

	r1 := c.Admit(ctx, model, 250_000, false, 409_000, 0)
	if r1 == nil {
		t.Fatal("first 250K request must be admitted against a 409K budget")
	}
	if r2 := c.Admit(ctx, model, 250_000, false, 409_000, 0); r2 != nil {
		t.Error("second 250K request must be rejected (500K > 409K)")
	}
	if r3 := c.Admit(ctx, model, 250_000, false, 409_000, 0); r3 != nil {
		t.Error("third 250K request must be rejected")
	}

	sumW, count := c.Occupancy(model)
	if sumW != 250_000 || count != 1 {
		t.Fatalf("occupancy must reflect only the admitted request; got sumW=%d count=%d", sumW, count)
	}
	if sumW > 409_000 {
		t.Fatalf("weighted sum %d exceeded the budget 409000", sumW)
	}

	// Releasing the admitted request frees the budget for a new one.
	r1.Release()
	if sumW, count := c.Occupancy(model); sumW != 0 || count != 0 {
		t.Fatalf("release must restore occupancy to zero; got sumW=%d count=%d", sumW, count)
	}
	if r4 := c.Admit(ctx, model, 250_000, false, 409_000, 0); r4 == nil {
		t.Error("a request must be admitted once the budget is freed")
	}
}

// TestAdmitFractionScalesBudget verifies the admit fraction shrinks the effective
// budget: at 0.85 a request that would fit the raw budget is rejected.
func TestAdmitFractionScalesBudget(t *testing.T) {
	full := admission.NewController(1.0, 0)
	if r := full.Admit(context.Background(), model, 900, false, 1000, 0); r == nil {
		t.Fatal("900 must fit a raw 1000 budget at fraction 1.0")
	}
	scaled := admission.NewController(0.85, 0)
	if r := scaled.Admit(context.Background(), model, 900, false, 1000, 0); r != nil {
		t.Error("900 must be rejected at fraction 0.85 (effective budget 850)")
	}
	if r := scaled.Admit(context.Background(), model, 850, false, 1000, 0); r == nil {
		t.Error("850 must be admitted at the effective budget of 850")
	}
}

// TestMaxInflightBackstop verifies the plain in-flight count cap rejects the
// (N+1)-th request even when the token budget still has room.
func TestMaxInflightBackstop(t *testing.T) {
	c := admission.NewController(1.0, 0)
	ctx := context.Background()
	// Budget is effectively unlimited (0); only max_inflight=2 gates.
	if c.Admit(ctx, model, 1, false, 0, 2) == nil {
		t.Fatal("first tiny request must be admitted")
	}
	if c.Admit(ctx, model, 1, false, 0, 2) == nil {
		t.Fatal("second tiny request must be admitted")
	}
	if c.Admit(ctx, model, 1, false, 0, 2) != nil {
		t.Error("third request must hit the max_inflight backstop even with token budget to spare")
	}
}

// TestBudgetAndBackstopInertWhenZero verifies a model that declares neither a
// budget nor a backstop always admits — the gate is a no-op.
func TestBudgetAndBackstopInertWhenZero(t *testing.T) {
	c := admission.NewController(0.85, 0)
	for i := 0; i < 100; i++ {
		if c.Admit(context.Background(), model, 1_000_000, false, 0, 0) == nil {
			t.Fatalf("admission must be inert with no budget/backstop, rejected at i=%d", i)
		}
	}
}

// TestParkAdmitsAfterRelease verifies an over-budget request parks and is admitted
// once an in-flight request releases, rather than being rejected outright.
func TestParkAdmitsAfterRelease(t *testing.T) {
	c := admission.NewController(1.0, 2*time.Second)
	ctx := context.Background()
	r1 := c.Admit(ctx, model, 1000, false, 1000, 0) // fills the budget
	if r1 == nil {
		t.Fatal("first request must fill the budget")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		r1.Release()
	}()

	start := time.Now()
	r2 := c.Admit(ctx, model, 1000, false, 1000, 0)
	elapsed := time.Since(start)
	if r2 == nil {
		t.Fatal("parked request must be admitted after the slot frees")
	}
	if elapsed >= 2*time.Second {
		t.Errorf("request should have admitted on release (~20ms), not waited the full park window; took %v", elapsed)
	}
}

// TestHopelessRequestRejectedFast verifies a request whose footprint alone
// exceeds the budget is rejected immediately, not parked for the full window —
// no release can ever make it fit.
func TestHopelessRequestRejectedFast(t *testing.T) {
	c := admission.NewController(1.0, 2*time.Second)
	start := time.Now()
	r := c.Admit(context.Background(), model, 2000, false, 1000, 0) // 2000 > budget 1000
	if r != nil {
		t.Fatal("a request larger than the budget can never fit and must be rejected")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("a hopeless request must not park for the full window; took %v", elapsed)
	}
}

// TestSmallBudgetFlooredNotDisabled verifies a positive budget the admit fraction
// would truncate to zero is floored at 1 rather than silently becoming unlimited:
// a footprint above the floor is rejected, at the floor is admitted.
func TestSmallBudgetFlooredNotDisabled(t *testing.T) {
	c := admission.NewController(0.85, 0) // 0.85 × 1 = 0.85 → would truncate to 0
	if r := c.Admit(context.Background(), model, 2, false, 1, 0); r != nil {
		t.Error("footprint 2 must be rejected against a budget floored at 1 (not admitted as unlimited)")
	}
	if r := c.Admit(context.Background(), model, 1, false, 1, 0); r == nil {
		t.Error("footprint 1 must be admitted at the floored budget of 1")
	}
}

// TestPerKeyOversubscriptionIsIndependent models the per-key occupancy share:
// three keys each capped at 0.40 of a 1000-token budget (400 each) all admit
// their full share concurrently, because per-key buckets are independent — the
// shares are intentionally oversubscribed (3 × 400 = 1200 > 1000). Box safety is
// the separate global budget, not the sum of shares.
func TestPerKeyOversubscriptionIsIndependent(t *testing.T) {
	c := admission.NewController(1.0, 0) // per-key controller: no fraction, no park
	ctx := context.Background()
	const perKeyBudget = 400 // 0.40 × 1000
	for _, key := range []string{"keyA\x00m", "keyB\x00m", "keyC\x00m"} {
		if c.Admit(ctx, key, perKeyBudget, false, perKeyBudget, 0) == nil {
			t.Errorf("key %q must admit its full 0.40 share independently", key)
		}
	}
	// Each key is at its own cap; a second request on any key is denied.
	if c.Admit(ctx, "keyA\x00m", 1, false, perKeyBudget, 0) != nil {
		t.Error("a key already at its share must be denied more")
	}
}

// TestParkTimeoutRejects verifies a request that never gets room is rejected when
// the park window expires.
func TestParkTimeoutRejects(t *testing.T) {
	c := admission.NewController(1.0, 40*time.Millisecond)
	ctx := context.Background()
	if c.Admit(ctx, model, 1000, false, 1000, 0) == nil {
		t.Fatal("first request must fill the budget")
	}
	start := time.Now()
	r := c.Admit(ctx, model, 1000, false, 1000, 0)
	if r != nil {
		t.Fatal("a request that never gets room must be rejected")
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Error("rejection must wait out the park window before giving up")
	}
}

// TestParkCancelledByContext verifies a parked request gives up when its context
// is cancelled, without waiting for the full park window.
func TestParkCancelledByContext(t *testing.T) {
	c := admission.NewController(1.0, 2*time.Second)
	if c.Admit(context.Background(), model, 1000, false, 1000, 0) == nil {
		t.Fatal("first request must fill the budget")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if r := c.Admit(ctx, model, 1000, false, 1000, 0); r != nil {
		t.Fatal("a request whose context is cancelled must not be admitted")
	}
	if time.Since(start) >= 2*time.Second {
		t.Error("context cancellation must cut the park short")
	}
}

// TestGrowIncreasesOccupancy verifies an undeclared reservation grows the
// weighted sum, a declared one ignores growth, and the release returns exactly
// what was charged (input + all growth) with no leak.
func TestGrowIncreasesOccupancy(t *testing.T) {
	c := admission.NewController(1.0, 0)
	ctx := context.Background()

	// Undeclared: reserve input only, then grow.
	und := c.Admit(ctx, model, 100, true, 1_000_000, 0)
	und.Grow(30)
	und.Grow(20)
	if sumW, _ := c.Occupancy(model); sumW != 150 {
		t.Errorf("undeclared occupancy must include growth (100+50); got %d", sumW)
	}
	if und.Weight() != 150 {
		t.Errorf("reservation weight must track growth; got %d", und.Weight())
	}

	// Declared: growth is a no-op.
	dec := c.Admit(ctx, model, 200, false, 1_000_000, 0)
	dec.Grow(999)
	if sumW, _ := c.Occupancy(model); sumW != 150+200 {
		t.Errorf("declared reservation must not grow; got %d, want %d", sumW, 350)
	}

	und.Release()
	dec.Release()
	if sumW, count := c.Occupancy(model); sumW != 0 || count != 0 {
		t.Fatalf("release must return all footprint incl. growth; got sumW=%d count=%d", sumW, count)
	}
}

// TestReleaseIdempotent verifies releasing twice frees the footprint exactly
// once — a double release must not over-subtract and corrupt the budget.
func TestReleaseIdempotent(t *testing.T) {
	c := admission.NewController(1.0, 0)
	ctx := context.Background()
	a := c.Admit(ctx, model, 400, false, 1000, 0)
	b := c.Admit(ctx, model, 400, false, 1000, 0)

	a.Release()
	a.Release() // second release must be a no-op
	if sumW, count := c.Occupancy(model); sumW != 400 || count != 1 {
		t.Fatalf("double release must free once; got sumW=%d count=%d, want 400/1", sumW, count)
	}
	b.Release()
	if sumW, count := c.Occupancy(model); sumW != 0 || count != 0 {
		t.Fatalf("final occupancy must be zero; got sumW=%d count=%d", sumW, count)
	}
}

// TestGrowAfterReleaseNoOp verifies a streaming growth that races in after the
// request has been released cannot leak budget.
func TestGrowAfterReleaseNoOp(t *testing.T) {
	c := admission.NewController(1.0, 0)
	r := c.Admit(context.Background(), model, 100, true, 1_000_000, 0)
	r.Release()
	r.Grow(50) // late callback after release
	if sumW, count := c.Occupancy(model); sumW != 0 || count != 0 {
		t.Fatalf("grow after release must be a no-op; got sumW=%d count=%d", sumW, count)
	}
}

// TestConcurrentAdmitReleaseNoLeak stress-tests the counter: many goroutines
// admit and release, and the final occupancy must return exactly to zero.
func TestConcurrentAdmitReleaseNoLeak(t *testing.T) {
	c := admission.NewController(1.0, 100*time.Millisecond)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r := c.Admit(ctx, model, 1000, false, 50_000, 0); r != nil {
				r.Release()
			}
		}()
	}
	wg.Wait()
	if sumW, count := c.Occupancy(model); sumW != 0 || count != 0 {
		t.Fatalf("after all admit/release pairs, occupancy must be zero; got sumW=%d count=%d", sumW, count)
	}
}
