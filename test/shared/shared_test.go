// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/shared"
)

const interval = 100 * time.Millisecond

type router struct {
	store  *shared.Store
	rate   *auth.InMemoryLimiter
	minute *auth.MinuteRateLimiter
	cancel context.CancelFunc
}

// newRouter starts one router's limiters wired to the shared store at addr.
func newRouter(t *testing.T, addr, id string) *router {
	t.Helper()
	st, err := shared.New(shared.Config{Addrs: []string{addr}, Prefix: "pc", RouterID: id, Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	r := &router{store: st, rate: auth.NewInMemoryLimiter(), minute: auth.NewMinuteRateLimiter()}
	r.rate.SetFleet(st)
	st.AddApplier(r.rate)
	r.minute.SetFleet(st)
	st.AddApplier(r.minute)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	st.Start(ctx)
	t.Cleanup(func() { cancel(); st.Close() })
	return r
}

// newFleet starts n routers and waits until each sees all of them.
func newFleet(t *testing.T, mr *miniredis.Miniredis, n int) []*router {
	t.Helper()
	var rs []*router
	for i := 0; i < n; i++ {
		rs = append(rs, newRouter(t, mr.Addr(), fmt.Sprintf("r%d", i)))
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

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Three routers splitting one key's traffic admit what one router would: one
// limit, not three.
func TestRPMSharedAcrossRouters(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 3)
	const rpm = 120 // burst = a full minute
	allowed := 0
	for i := 0; i < 600; i++ {
		if ok, _, _ := rs[i%3].rate.AllowRequest("t", "", rpm); ok {
			allowed++
		}
		if i%30 == 0 {
			time.Sleep(interval + interval/2)
		}
	}
	// ~3 s of traffic: one router would admit the 120 burst plus ~6 refilled.
	if allowed > rpm+15 {
		t.Fatalf("admitted %d against a shared limit of %d", allowed, rpm)
	}
	if allowed < rpm*3/4 {
		t.Fatalf("admitted only %d: the shared limit is far too strict", allowed)
	}
}

// Without sharing each router has its own bucket: the control for the test above.
func TestRPMWithoutSharingMultiplies(t *testing.T) {
	ls := []*auth.InMemoryLimiter{auth.NewInMemoryLimiter(), auth.NewInMemoryLimiter(), auth.NewInMemoryLimiter()}
	allowed := 0
	for i := 0; i < 600; i++ {
		if ok, _, _ := ls[i%3].AllowRequest("t", "", 120); ok {
			allowed++
		}
	}
	if allowed < 3*120 {
		t.Fatalf("expected three independent buckets to admit 3×120, got %d", allowed)
	}
}

// The daily budget holds across routers: the per-sync share keeps routers that
// cannot see each other's latest spend from overshooting it together.
func TestDailySharedAcrossRouters(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 3)
	const tpd, cost = 3000, 10
	spent := 0
	for i := 0; i < 2000; i++ {
		if ok, _, _ := rs[i%3].rate.AllowInputTokens("t", "", tpd, cost); ok {
			spent += cost
		}
		if i%60 == 0 {
			time.Sleep(interval + interval/2)
		}
	}
	if spent > tpd+3*cost {
		t.Fatalf("spent %d tokens against a shared daily budget of %d", spent, tpd)
	}
	if spent < tpd*3/4 {
		t.Fatalf("spent only %d of %d: the share is far too strict", spent, tpd)
	}
}

// Output is charged after the response exists: it is judged against the
// policloud total only, never refused because this router's share is spent.
func TestOutputNeverRefusedByRouterShare(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 3)
	const tpd = 3000
	// Spend this router's share for the current sync window.
	if ok, _, _ := rs[0].rate.AllowInputTokens("t", "", tpd, 900); !ok {
		t.Fatal("first admission refused")
	}
	if ok, _, _ := rs[0].rate.AllowInputTokens("t", "", tpd, 900); ok {
		t.Skip("sync window rolled over between the two calls; share not exercised")
	}
	if ok, _, _ := rs[0].rate.AllowOutputTokens("t", "", tpd, 500); !ok {
		t.Fatal("output refused although the daily budget has room")
	}
}

// Input and output tokens per minute are shared like requests per minute.
func TestTokensPerMinuteShared(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 3)
	const itpm, cost = 6000, 100 // 60 prompts per minute
	allowed := 0
	for i := 0; i < 300; i++ {
		if rs[i%3].minute.AllowInputTokens("k", "m", itpm, cost) {
			allowed++
		}
		if i%30 == 0 {
			time.Sleep(interval + interval/2)
		}
	}
	if allowed > itpm/cost+10 {
		t.Fatalf("admitted %d prompts against a shared %d per minute", allowed, itpm/cost)
	}
	// Output: one router's heavy output throttles the key on the others.
	const otpm = 1000
	rs[0].minute.ChargeOutputTokens("k", "o", otpm, otpm)
	waitFor(t, 2*time.Second, "output spent on r0 to reach r1", func() bool {
		return rs[1].minute.OutputExhausted("k", "o", otpm)
	})
}

// When Valkey is down every router enforces its share of each limit, so the
// routers together still stay within it.
func TestStoreDownEachRouterEnforcesItsShare(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 3)
	mr.Close()
	waitFor(t, 5*time.Second, "router to notice the store is down", func() bool {
		return rs[0].store.DegradedEpoch() != 0
	})
	const rpm = 120
	allowed := 0
	for i := 0; i < 200; i++ {
		if ok, _, _ := rs[0].rate.AllowRequest("t", "", rpm); ok {
			allowed++
		}
	}
	if allowed > rpm/3+2 {
		t.Fatalf("one router admitted %d while degraded, want about its share of %d", allowed, rpm/3)
	}
	if allowed < rpm/3-2 {
		t.Fatalf("one router admitted only %d while degraded, want about %d", allowed, rpm/3)
	}
}

// A router that cannot read today's policloud total refuses daily-limited
// requests (it cannot know what the others spent); keys without a daily
// limit keep working.
func TestDailyFailsClosedWithoutTotals(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	mr.Close()
	r := newRouter(t, addr, "r0")
	if ok, _, _ := r.rate.AllowInputTokens("t", "", 1000, 10); ok {
		t.Fatal("admitted a daily-limited request without the policloud total")
	}
	if ok, _, _ := r.rate.AllowInputTokens("t", "", 0, 10); !ok {
		t.Fatal("refused a key without a daily limit")
	}
}

// A router that joins (or restarts) reads today's total and the last minute of
// usage, so it does not hand out a fresh budget.
func TestJoiningRouterKnowsRecentUsage(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newFleet(t, mr, 1)[0]
	const rpm, tpd = 60, 1000
	for i := 0; i < rpm; i++ {
		a.rate.AllowRequest("t", "", rpm)
	}
	if ok, _, _ := a.rate.AllowInputTokens("t", "", tpd, 900); !ok {
		t.Fatal("first daily admission refused")
	}
	time.Sleep(3 * interval) // let r0 publish
	b := newRouter(t, mr.Addr(), "r1")
	waitFor(t, 2*time.Second, "r1 to sync", func() bool { return b.store.SyncSeq() > 0 })
	if ok, _, _ := b.rate.AllowRequest("t", "", rpm); ok {
		t.Fatal("joining router admitted a key whose minute is already spent")
	}
	if ok, _, _ := b.rate.AllowInputTokens("t", "", tpd, 200); ok {
		t.Fatal("joining router admitted beyond today's policloud total")
	}
}

// A stopped router stops counting as live, so the others' shares grow back.
func TestFleetSizeFollowsLiveRouters(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 3)
	rs[2].cancel()
	waitFor(t, 3*time.Second, "r0 to drop the stopped router", func() bool {
		return rs[0].store.Size() == 2
	})
}
