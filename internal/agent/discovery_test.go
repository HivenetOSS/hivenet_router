// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// newDiscoveryAgent returns an agent whose sessions do nothing but wait to be
// dropped, so the discovery rules can be tested without routers.
func newDiscoveryAgent() *Agent {
	a := &Agent{stopCh: make(chan struct{})}
	a.routers.start = func(rs *routerSession) {
		select {
		case <-rs.stop:
		case <-a.stopCh:
		}
	}
	return a
}

func (a *Agent) addRouter(addr string, seed bool) {
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	a.addRouterLocked(addr, seed, "")
}

// pid is how a list names the router whose session authenticated as id.
func pid(id string) string { return peer.ID(id).String() }

func (a *Agent) authenticated(addr, peerID string) {
	a.routers.mu.Lock()
	rs := a.routers.entries[addr].rs
	a.routers.mu.Unlock()
	a.setPeer(rs, peer.ID(peerID))
}

func (a *Agent) setUp(addr string, up bool) {
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	a.routers.entries[addr].rs.up.Store(up)
}

func (a *Agent) routerAddrs() map[string]bool {
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	out := map[string]bool{}
	for addr := range a.routers.entries {
		out[addr] = true
	}
	return out
}

func TestDiscoveryRules(t *testing.T) {
	a := newDiscoveryAgent()
	defer close(a.stopCh)
	a.addRouter("seed:1", true)

	// A listed router is added; an empty list changes nothing.
	a.learnRouters([]siteRouter{{addr: "r1:1", peerID: "p1"}})
	a.learnRouters(nil)
	if got := a.routerAddrs(); !got["seed:1"] || !got["r1:1"] {
		t.Fatalf("routers %v, want the seed and r1", got)
	}

	// Missing from the lists: kept within the grace period, and while its
	// session is up; closed after it, once down. The seed is never closed.
	now := time.Now()
	a.pruneOnce(now.Add(time.Minute))
	if !a.routerAddrs()["r1:1"] {
		t.Fatal("discovered router closed within the grace period")
	}
	a.setUp("r1:1", true)
	a.pruneOnce(now.Add(discoveredGrace + time.Second))
	if !a.routerAddrs()["r1:1"] {
		t.Fatal("discovered router closed while its session is up")
	}
	a.setUp("r1:1", false)
	a.pruneOnce(now.Add(discoveredGrace + time.Second))
	got := a.routerAddrs()
	if got["r1:1"] {
		t.Fatal("discovered router kept after the grace period with its session down")
	}
	if !got["seed:1"] {
		t.Fatal("seed router closed")
	}
}

// A seed given under another name than the router advertises gets no second
// session, even while the seed's session is down (in backoff).
func TestDiscoverySeedUnderAnotherName(t *testing.T) {
	a := newDiscoveryAgent()
	defer close(a.stopCh)
	a.addRouter("router-0.svc:1", true)
	a.authenticated("router-0.svc:1", "p0")
	a.learnRouters([]siteRouter{{addr: "10.0.0.5:1", peerID: pid("p0")}})
	if a.routerAddrs()["10.0.0.5:1"] {
		t.Fatal("second session opened to a seed's router while the seed is down")
	}

	// Listed before the seed first authenticated: the second session is
	// closed once the seed is up.
	b := newDiscoveryAgent()
	defer close(b.stopCh)
	b.addRouter("router-1.svc:1", true)
	b.learnRouters([]siteRouter{{addr: "10.0.0.6:1", peerID: pid("p1")}})
	if !b.routerAddrs()["10.0.0.6:1"] {
		t.Fatal("listed router not added")
	}
	b.authenticated("router-1.svc:1", "p1")
	b.setUp("router-1.svc:1", true)
	b.pruneOnce(time.Now())
	if b.routerAddrs()["10.0.0.6:1"] {
		t.Fatal("second session to a router a seed reaches was kept")
	}
}

// A router listed under a new address (same peer) is dialed there once its
// old session is down, and the old entry ages out.
func TestDiscoveryRouterMoves(t *testing.T) {
	a := newDiscoveryAgent()
	defer close(a.stopCh)
	a.learnRouters([]siteRouter{{addr: "old:1", peerID: pid("p1")}})
	a.authenticated("old:1", "p1")
	a.setUp("old:1", true)
	a.learnRouters([]siteRouter{{addr: "new:1", peerID: pid("p1")}})
	if a.routerAddrs()["new:1"] {
		t.Fatal("second session opened while the old address still serves the router")
	}
	a.setUp("old:1", false)
	a.learnRouters([]siteRouter{{addr: "new:1", peerID: pid("p1")}})
	if !a.routerAddrs()["new:1"] {
		t.Fatal("new address not dialed once the old session is down")
	}
	a.routers.mu.Lock()
	a.routers.entries["old:1"].lastListed = time.Now().Add(-discoveredGrace - time.Second)
	a.routers.mu.Unlock()
	a.pruneOnce(time.Now())
	if got := a.routerAddrs(); got["old:1"] || !got["new:1"] {
		t.Fatalf("routers %v, want only the new address", got)
	}
}

// The cap holds across lists, not only within one.
func TestDiscoveryBoundsTheTotal(t *testing.T) {
	a := newDiscoveryAgent()
	defer close(a.stopCh)
	for l := 0; l < 3; l++ {
		var list []siteRouter
		for i := 0; i < maxDiscovered/2+1; i++ {
			list = append(list, siteRouter{addr: fmt.Sprintf("r%d-%d:1", l, i), peerID: fmt.Sprintf("p%d-%d", l, i)})
		}
		a.learnRouters(list)
	}
	if n := len(a.routerAddrs()); n != maxDiscovered {
		t.Fatalf("%d discovered routers kept, want %d", n, maxDiscovered)
	}
}

// A list longer than maxDiscovered is cut to it.
func TestDiscoveryBoundsTheList(t *testing.T) {
	a := newDiscoveryAgent()
	defer close(a.stopCh)
	var list []siteRouter
	for i := 0; i < maxDiscovered+10; i++ {
		list = append(list, siteRouter{addr: fmt.Sprintf("r%d:1", i), peerID: fmt.Sprintf("p%d", i)})
	}
	a.learnRouters(list)
	if n := len(a.routerAddrs()); n != maxDiscovered {
		t.Fatalf("%d routers taken from a list of %d, want %d", n, len(list), maxDiscovered)
	}
}
