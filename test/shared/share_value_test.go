// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared_test

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"hivenet_router/internal/shared"

	"github.com/alicebob/miniredis/v2"
)

// TestShareValue: each router of the site publishes a value under a name and
// reads back one value per live router, its own included: a router's newest
// run. A router that restarts does not see its earlier run's value as a
// peer's, a router restarted several times counts once, with its current
// value, and a run that stopped publishing drops out once its value is
// older than the window.
func TestShareValue(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	start := func(id string) (*shared.Store, func()) {
		t.Helper()
		st, err := shared.New(shared.Config{Addrs: []string{mr.Addr()}, Prefix: "pc", RouterID: id, Interval: interval})
		if err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithCancel(ctx)
		st.Start(cctx)
		stop := func() { cancel(); st.Close() }
		t.Cleanup(stop)
		return st, stop
	}
	share := func(st *shared.Store, value string, window time.Duration) []string {
		t.Helper()
		var got map[string]string
		deadline := time.Now().Add(5 * time.Second)
		for {
			var err error
			if got, err = st.ShareValue(ctx, "fp", value, window); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("ShareValue: %v", err)
			}
			time.Sleep(interval)
		}
		return slices.Sorted(maps.Values(got))
	}

	a, stopA := start("a")
	b, _ := start("b")
	if got := share(a, "x", time.Minute); !slices.Equal(got, []string{"x"}) {
		t.Errorf("a alone sees %v, want [x]", got)
	}
	if got := share(b, "y", time.Minute); !slices.Equal(got, []string{"x", "y"}) {
		t.Errorf("b sees %v, want [x y]", got)
	}
	// a restarts with another value: its earlier run is not a peer.
	stopA()
	a2, _ := start("a")
	if got := share(a2, "z", time.Minute); !slices.Equal(got, []string{"y", "z"}) {
		t.Errorf("restarted a sees %v, want [y z] (not its earlier run's x)", got)
	}
	// c restarts three times, each run with its own value: the others see
	// one router c, with its newest value.
	for i, v := range []string{"c1", "c2", "c3"} {
		c, stop := start("c")
		share(c, v, time.Minute)
		if i < 2 {
			stop()
			time.Sleep(3 * time.Millisecond) // a new run ID (RouterID@start-ms)
		}
	}
	if got := share(b, "y", time.Minute); !slices.Equal(got, []string{"c3", "y", "z"}) {
		t.Errorf("b sees %v after c restarted three times, want [c3 y z]: one value per router", got)
	}
	// Outside the window, values no longer count: a2's and c's were written
	// over 20 ms ago, b's own is rewritten by this call.
	time.Sleep(20 * time.Millisecond)
	if got := share(b, "y", 5*time.Millisecond); !slices.Equal(got, []string{"y"}) {
		t.Errorf("b with a 5 ms window sees %v, want only its own [y]", got)
	}
}
