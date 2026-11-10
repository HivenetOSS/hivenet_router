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
	a.addRouter("seed:1", true, "")

	// A listed router is added; an empty list changes nothing.
	a.learnRouters([]siteRouter{{addr: "r1:1", peerID: "p1"}})
	a.learnRouters(nil)
	if got := a.routerAddrs(); !got["seed:1"] || !got["r1:1"] {
		t.Fatalf("routers %v, want the seed and r1", got)
	}

	// A router this agent has a session with under another address (a seed
	// given as a DNS name) gets no second session.
	a.openSession(peer.ID("p2"))
	a.learnRouters([]siteRouter{{addr: "r2-other-name:1", peerID: peer.ID("p2").String()}})
	if a.routerAddrs()["r2-other-name:1"] {
		t.Fatal("second session opened to a router already served")
	}

	// Missing from the lists: kept within the grace period, and while its
	// session is up; closed after it, once down. The seed is never closed.
	now := time.Now()
	a.pruneOnce(now.Add(time.Minute))
	if !a.routerAddrs()["r1:1"] {
		t.Fatal("discovered router closed within the grace period")
	}
	a.routers.mu.Lock()
	a.routers.entries["r1:1"].rs.up.Store(true)
	a.routers.mu.Unlock()
	a.pruneOnce(now.Add(discoveredGrace + time.Second))
	if !a.routerAddrs()["r1:1"] {
		t.Fatal("discovered router closed while its session is up")
	}
	a.routers.mu.Lock()
	a.routers.entries["r1:1"].rs.up.Store(false)
	a.routers.mu.Unlock()
	a.pruneOnce(now.Add(discoveredGrace + time.Second))
	got := a.routerAddrs()
	if got["r1:1"] {
		t.Fatal("discovered router kept after the grace period with its session down")
	}
	if !got["seed:1"] {
		t.Fatal("seed router closed")
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
