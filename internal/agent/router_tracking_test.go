// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

// One router listed under two addresses has two sessions: it stays in the
// report until the last one ends, or the other routers would think the agent
// left it and reclaim its credits while it still serves.
func TestRouterStaysReportedUntilLastSession(t *testing.T) {
	a := &Agent{}
	p := peer.ID("router-a")
	a.openSession(p)
	a.openSession(p)
	a.closeSession(p)
	if _, ok := a.routerReport()[p.String()]; !ok {
		t.Fatal("router dropped from the report while a session to it is still open")
	}
	a.closeSession(p)
	if _, ok := a.routerReport()[p.String()]; ok {
		t.Fatal("router still reported after its last session ended")
	}
}

// A request that finds its router's entry just as the router is lost is
// cancelled at once, not tracked on an entry no one will cancel.
func TestRequestOnLostRouterIsCancelled(t *testing.T) {
	a := &Agent{}
	p := peer.ID("router-a")
	ctx, end := a.trackFrom(context.Background(), p)
	defer end()
	v, _ := a.inFlightBy.Load(p)
	rr := v.(*routerRequests)
	a.cancelFrom(p)
	if ctx.Err() == nil {
		t.Fatal("request in flight not cancelled when its router was lost")
	}
	// A request that loaded rr before cancelFrom deleted it: simulate by
	// putting the dead entry back, as a concurrent LoadOrStore would see it.
	a.inFlightBy.Store(p, rr)
	late, endLate := a.trackFrom(context.Background(), p)
	defer endLate()
	if late.Err() == nil {
		t.Fatal("a request tracked on a lost router's entry was not cancelled")
	}
	if n := rr.n.Load(); n != 1 {
		t.Fatalf("lost router's entry counts %d requests, want only the first", n)
	}
}
