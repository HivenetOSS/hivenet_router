// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/shared"
)

type occRouter struct {
	store   *shared.Store
	credits *shared.Credits
	occ     *admission.Controller
	cancel  context.CancelFunc
	reach   *atomic.Value // func(string) (bool, bool)
}

// newOccFleet starts n routers sharing one occupancy pool. reclaim is how long
// a silent router keeps its credits.
func newOccFleet(t *testing.T, mr *miniredis.Miniredis, n int, reclaim time.Duration) []*occRouter {
	t.Helper()
	var rs []*occRouter
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		st, err := shared.New(shared.Config{Addrs: []string{mr.Addr()}, Prefix: "pc", RouterID: fmt.Sprintf("r%d", i), Interval: interval})
		if err != nil {
			t.Fatal(err)
		}
		st.SetPeerID(fmt.Sprintf("peer-%d", i))
		reach := &atomic.Value{}
		reach.Store(func(string) (bool, bool) { return false, false }) // no agent reports: wait for the window
		cr := st.NewCredits("occ", reclaim, func(p string) (bool, bool) {
			return reach.Load().(func(string) (bool, bool))(p)
		})
		c := admission.NewController(1, 300*time.Millisecond)
		c.SetPool(cr)
		st.Start(ctx)
		cr.Start(ctx)
		t.Cleanup(func() { cancel(); st.Close() })
		rs = append(rs, &occRouter{store: st, credits: cr, occ: c, cancel: cancel, reach: reach})
	}
	waitFor(t, 5*time.Second, "routers to see each other", func() bool {
		for _, r := range rs {
			if r.store.Size() != n {
				return false
			}
		}
		return true
	})
	return rs
}

// Three routers admitting at once never hold more than the budget, and the
// store is called far less than once per request.
func TestCreditsNeverOversubscribe(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 3, time.Minute)
	const budget, w = 2000, 100 // 20 requests site-wide
	var inflight, peak, admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 600; i++ {
		wg.Add(1)
		go func(r *occRouter) {
			defer wg.Done()
			res := r.occ.Admit(context.Background(), "m", w, false, budget, 0)
			if res == nil {
				return
			}
			admitted.Add(1)
			n := inflight.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			time.Sleep(20 * time.Millisecond)
			inflight.Add(-1)
			res.Release()
		}(rs[i%3])
		if i%50 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	wg.Wait()
	if peak.Load() > budget/w {
		t.Fatalf("peak in flight %d exceeds the budget of %d requests", peak.Load(), budget/w)
	}
	var refills int64
	for _, r := range rs {
		f, _, _ := r.credits.Stats()
		refills += f
	}
	if admitted.Load() < 100 {
		t.Fatalf("admitted only %d", admitted.Load())
	}
	if refills >= admitted.Load() {
		t.Fatalf("%d refills for %d admissions: credits save no store calls", refills, admitted.Load())
	}
}

// Undeclared output grows a reservation past the credits it took; the tick
// records the growth as held, so the other routers cannot be granted it.
func TestCreditsCountStreamedGrowth(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, time.Minute)
	const budget = 1000
	res := rs[0].occ.Admit(context.Background(), "m", 100, true, budget, 0)
	if res == nil {
		t.Fatal("first admission refused")
	}
	res.Grow(800) // the response streams 800 tokens: r0 now uses 900
	time.Sleep(3 * interval)
	if r := rs[1].occ.Admit(context.Background(), "m", 200, false, budget, 0); r != nil {
		t.Fatal("r1 admitted 200 although r0's streamed output leaves only 100")
	}
	if r := rs[1].occ.Admit(context.Background(), "m", 90, false, budget, 0); r == nil {
		t.Fatal("r1 refused 90 although 100 are free")
	}
}

// A starved router gets the others' idle credits within a few ticks.
func TestCreditsRebalanceToStarvedRouter(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 3, time.Minute)
	const budget = 1000
	for _, r := range rs[:2] {
		if res := r.occ.Admit(context.Background(), "m", 10, false, budget, 0); res != nil {
			defer res.Release()
		}
	}
	waitFor(t, 3*time.Second, "the starved router to get the idle credits", func() bool {
		if res := rs[2].occ.Admit(context.Background(), "m", 950, false, budget, 0); res != nil {
			res.Release()
			return true
		}
		return false
	})
}

// A crashed router's credits come back after the reclaim window when no
// agent report says otherwise.
func TestCreditsReclaimedAfterWindow(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, 2*time.Second)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].cancel() // crash: no more heartbeats or ticks
	if rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil {
		t.Fatal("admitted while the silent router may still be running its request")
	}
	waitFor(t, 6*time.Second, "the silent router's credits to come back", func() bool {
		return rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil
	})
}

// When the agents report that no one is connected to the silent router, its
// credits come back as soon as it is seen as down, long before the window.
func TestCreditsReclaimedEarlyWhenNoAgentReachesRouter(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, time.Hour)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	// The agents report a connection to r1 only.
	rs[1].reach.Store(func(p string) (bool, bool) { return true, p == "peer-1" })
	rs[0].cancel()
	start := time.Now()
	waitFor(t, 5*time.Second, "early reclaim", func() bool {
		return rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil
	})
	if _, _, reclaims := rs[1].credits.Stats(); reclaims == 0 {
		t.Fatal("credits came back without an early reclaim")
	}
	t.Logf("credits back after %v", time.Since(start).Round(100*time.Millisecond))
}

// A silent router the agents still reach (it lost Valkey, not the GPUs) keeps
// its credits until the window.
func TestCreditsKeptWhileAgentsReachRouter(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, time.Hour)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[1].reach.Store(func(string) (bool, bool) { return true, true })
	rs[0].cancel()
	time.Sleep(2 * time.Second)
	if rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil {
		t.Fatal("credits taken from a router the agents still reach")
	}
}

// A router evicted by the others drops the credits it believed it held and
// asks for them again, so the pool never counts them twice.
func TestEvictedRouterDropsItsCredits(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, time.Hour)
	const budget = 1000
	res := rs[0].occ.Admit(context.Background(), "m", 100, false, budget, 0)
	if res == nil {
		t.Fatal("first admission refused")
	}
	res.Release()
	// The others evict r0 (as an early reclaim would).
	mr.ZRem("hivenet:{pc}:routers", firstMember(t, mr, "r0@"))
	waitFor(t, 3*time.Second, "r0 to notice the eviction", func() bool { return rs[0].store.Evictions() > 0 })
	waitFor(t, 3*time.Second, "r1 to get the whole budget", func() bool {
		r := rs[1].occ.Admit(context.Background(), "m", 950, false, budget, 0)
		if r != nil {
			r.Release()
		}
		return r != nil
	})
}

func firstMember(t *testing.T, mr *miniredis.Miniredis, prefix string) string {
	t.Helper()
	ms, err := mr.ZMembers("hivenet:{pc}:routers")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if len(m) >= len(prefix) && m[:len(prefix)] == prefix {
			return m
		}
	}
	t.Fatalf("no live member starting with %q in %v", prefix, ms)
	return ""
}

// A router shutting down hands its credits back at once.
func TestReleaseAllHandsCreditsBack(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, time.Hour)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].cancel()
	rs[0].credits.ReleaseAll(context.Background())
	if rs[1].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("credits not back after ReleaseAll")
	}
}

// A redirect token is accepted once across the site, not once per router.
func TestFirstUseAcrossRouters(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newOccFleet(t, mr, 2, time.Hour)
	until := time.Now().Add(time.Minute)
	if ok, err := rs[0].store.FirstUse(context.Background(), "jti-1", until); err != nil || !ok {
		t.Fatalf("first use refused: %v %v", ok, err)
	}
	if ok, err := rs[1].store.FirstUse(context.Background(), "jti-1", until); err != nil || ok {
		t.Fatalf("replay on another router accepted: %v %v", ok, err)
	}
}
