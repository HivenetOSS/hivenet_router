// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"sync"
	"testing"

	"hivenet_router/internal/auth"
)

func keyBudget(tpd int) auth.DailyBudget {
	return auth.DailyBudget{TenantID: "t", KeyID: "k", KeyTokensPerDay: tpd}
}

func remaining(t *testing.T, l auth.RateLimiter, b auth.DailyBudget) int {
	t.Helper()
	r, err := l.RemainingTokens(b)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestDailyHold_SettlesExactUsage: the daily budget counts what the request
// used, whether the estimate held at admission was above or below it.
func TestDailyHold_SettlesExactUsage(t *testing.T) {
	for _, tc := range []struct {
		name         string
		held, actual int
	}{
		{"estimate above usage: the rest is given back", 400, 150},
		{"estimate below usage: the difference is charged", 100, 250},
		{"exact estimate", 200, 200},
	} {
		l := auth.NewInMemoryLimiter()
		b := keyBudget(1000)
		h, ok, rem, err := l.HoldTokens(b, tc.held)
		if err != nil || !ok || rem != 1000-tc.held {
			t.Fatalf("%s: hold (%v, %d, %v), want admitted with %d left", tc.name, ok, rem, err, 1000-tc.held)
		}
		if got := remaining(t, l, b); got != 1000-tc.held {
			t.Errorf("%s: %d left while held, want %d", tc.name, got, 1000-tc.held)
		}
		if ok, rem, _ := h.Settle(tc.actual); !ok || rem != 1000-tc.actual {
			t.Errorf("%s: settle (%v, %d), want allowed with %d left", tc.name, ok, rem, 1000-tc.actual)
		}
		if got := remaining(t, l, b); got != 1000-tc.actual {
			t.Errorf("%s: %d left after settle, want %d", tc.name, got, 1000-tc.actual)
		}
	}
}

// TestDailyHold_ConcurrentRequestsSeeTheHold: a held estimate counts for the
// requests admitted after it, so concurrent requests cannot together pass the
// budget; once settled for less, the rest is free again.
func TestDailyHold_ConcurrentRequestsSeeTheHold(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	b := keyBudget(100)
	first, ok, _, _ := l.HoldTokens(b, 60)
	if !ok {
		t.Fatal("first hold refused")
	}
	if _, ok, _, _ := l.HoldTokens(b, 60); ok {
		t.Fatal("second hold of 60 admitted while 60 of 100 are held")
	}
	first.Settle(10) //nolint:errcheck
	if _, ok, _, _ := l.HoldTokens(b, 60); !ok {
		t.Fatal("hold of 60 refused after the first settled at 10")
	}

	// Many at once: never more admitted than fit.
	c := auth.NewInMemoryLimiter()
	var mu sync.Mutex
	var wg sync.WaitGroup
	admitted := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, _, _ := c.HoldTokens(keyBudget(1000), 100); ok {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if admitted != 10 {
		t.Errorf("%d holds of 100 admitted on a budget of 1000, want 10", admitted)
	}
}

// TestDailyHold_ReleaseAndSettleOnce: Release frees the hold without charging;
// a settle after it still charges what was used (a request whose client left
// may still have run); a second settle charges nothing; nil holds are safe.
func TestDailyHold_ReleaseAndSettleOnce(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	b := keyBudget(1000)
	h, _, _, _ := l.HoldTokens(b, 300)
	h.Release()
	h.Release()
	if got := remaining(t, l, b); got != 1000 {
		t.Errorf("%d left after release, want 1000", got)
	}
	h.Settle(120) //nolint:errcheck
	h.Settle(120) //nolint:errcheck
	if got := remaining(t, l, b); got != 880 {
		t.Errorf("%d left after release then settle twice, want 880", got)
	}
	h.Release()
	if got := remaining(t, l, b); got != 880 {
		t.Errorf("release after settle changed the budget: %d left", got)
	}
}

// TestDailyHold_RefusedHoldsNothing: a hold that does not fit is refused and
// holds nothing, on the key's and on the tenant's budget.
func TestDailyHold_RefusedHoldsNothing(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	b := auth.DailyBudget{TenantID: "t", KeyID: "k", KeyTokensPerDay: 1000, TenantTokensPerDay: 500}
	if h, ok, _, _ := l.HoldTokens(b, 600); ok || h != nil {
		t.Fatalf("hold of 600 on a tenant budget of 500: (%v, %v), want refused and nil", h, ok)
	}
	if got := remaining(t, l, b); got != 500 {
		t.Errorf("%d left after a refused hold, want 500", got)
	}
}

// TestDailyHold_TenantUsageCountsUsage: with no budget at all the request is
// not held, and its settle still counts what it used in the tenant's usage.
func TestDailyHold_TenantUsageCountsUsage(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	b := auth.DailyBudget{TenantID: "t", KeyID: "k"}
	h, ok, rem, _ := l.HoldTokens(b, 500)
	if !ok || rem != -1 {
		t.Fatalf("unlimited hold: (%v, %d), want allowed and -1", ok, rem)
	}
	h.Settle(42) //nolint:errcheck
	if used, _ := l.TenantUsageToday("t"); used != 42 {
		t.Errorf("tenant usage %d, want the 42 used", used)
	}
}

// TestDailyHold_BadgerPersistsSettledUsage: the Badger limiter persists what
// was settled, never the hold.
func TestDailyHold_BadgerPersistsSettledUsage(t *testing.T) {
	store := &memQuotaStore{m: map[string]int64{}}
	l := auth.NewBadgerLimiter(store)
	b := keyBudget(1000)
	settled, _, _, _ := l.HoldTokens(b, 400)
	settled.Settle(150)               //nolint:errcheck
	_, _, _, _ = l.HoldTokens(b, 300) // still in flight at the restart
	l.Flush()
	restarted := auth.NewBadgerLimiter(store)
	if got := remaining(t, restarted, b); got != 850 {
		t.Errorf("%d left after a restart, want 850 (the settled 150; the hold in flight is gone)", got)
	}
}
