// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	p2phttp "github.com/libp2p/go-libp2p/p2p/http"
)

// Router discovery. The agent starts from its --routers list (the seeds).
// Every router of a site that shares a store hands the agent the site's live
// routers, in the authentication response and in every heartbeat reply. The
// agent opens a session with each router it does not have one with, and
// closes a discovered one only once it has been missing from every list for
// discoveredGrace and its session is not up. Seeds are never closed.

// discoveredGrace is how long a discovered router may be missing from the
// lists before its session is closed (only while that session is down). A
// list can be short for a while (the store unreachable, a router restarting).
const discoveredGrace = 2 * time.Minute

// maxDiscovered bounds the routers taken from the lists.
const maxDiscovered = 32

// siteRouter is one entry of a router's list.
type siteRouter struct {
	addr   string // gRPC address
	peerID string // libp2p peer ID
}

// routerEntry is one router address the agent keeps a session with.
type routerEntry struct {
	rs         *routerSession
	seed       bool
	peerID     string    // advertised peer ID (discovered entries)
	lastListed time.Time // last list that named it (discovered entries)
}

// routerSet holds the router sessions: the seeds and the discovered routers.
type routerSet struct {
	mu       sync.Mutex
	entries  map[string]*routerEntry // gRPC address → entry
	node     host.Host
	httpHost *p2phttp.Host
	wg       sync.WaitGroup
	start    func(*routerSession) // runs a session; tests replace it
}

// startRouters opens a session with each seed and keeps the set for the
// routers discovered later.
func (a *Agent) startRouters(seeds []string, node host.Host, httpHost *p2phttp.Host) {
	a.routers.mu.Lock()
	if a.routers.entries == nil {
		a.routers.entries = map[string]*routerEntry{}
	}
	a.routers.node, a.routers.httpHost = node, httpHost
	a.routers.mu.Unlock()
	for _, addr := range seeds {
		a.addRouter(addr, true, "")
	}
	go a.pruneRouters()
}

// waitRouters blocks until every session has ended (the agent stopped).
func (a *Agent) waitRouters() { a.routers.wg.Wait() }

// addRouter opens a session with addr unless one is open already.
func (a *Agent) addRouter(addr string, seed bool, peerID string) {
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	if a.routers.entries == nil {
		a.routers.entries = map[string]*routerEntry{}
	}
	if _, ok := a.routers.entries[addr]; ok {
		return
	}
	e := &routerEntry{rs: &routerSession{addr: addr, stop: make(chan struct{})}, seed: seed, peerID: peerID, lastListed: time.Now()}
	a.routers.entries[addr] = e
	if !seed {
		log.Infof("Router %s discovered (peer %s): opening a session", addr, shortID(peerID))
	}
	node, httpHost, start := a.routers.node, a.routers.httpHost, a.routers.start
	if start == nil {
		start = func(rs *routerSession) { a.runRouterSession(rs, node, httpHost) }
	}
	a.routers.wg.Add(1)
	go func() {
		defer a.routers.wg.Done()
		start(e.rs)
	}()
}

// learnRouters takes a router's list of the site's live routers. An empty list
// changes nothing: it may only mean the store is unreachable.
func (a *Agent) learnRouters(list []siteRouter) {
	if len(list) == 0 {
		return
	}
	if len(list) > maxDiscovered {
		list = list[:maxDiscovered]
	}
	now := time.Now()
	for _, sr := range list {
		if sr.addr == "" || sr.peerID == "" {
			continue
		}
		a.routers.mu.Lock()
		known := false
		for _, e := range a.routers.entries {
			if e.rs.addr == sr.addr || e.peerID == sr.peerID {
				e.lastListed, known = now, true
			}
		}
		a.routers.mu.Unlock()
		// A router this agent already has a session with, under another
		// address (a seed given as a DNS name), needs no second one.
		if known || a.hasSession(sr.peerID) {
			continue
		}
		a.addRouter(sr.addr, false, sr.peerID)
	}
}

// hasSession reports whether a session with the router peer is registered.
func (a *Agent) hasSession(peerID string) bool {
	a.sessionsMu.Lock()
	defer a.sessionsMu.Unlock()
	for p, n := range a.sessions {
		if n > 0 && p.String() == peerID {
			return true
		}
	}
	return false
}

// pruneRouters closes the sessions of discovered routers missing from the
// lists for discoveredGrace whose session is down.
func (a *Agent) pruneRouters() {
	t := time.NewTicker(discoveredGrace / 4)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			a.pruneOnce(time.Now())
		}
	}
}

func (a *Agent) pruneOnce(now time.Time) {
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	for addr, e := range a.routers.entries {
		if e.seed || now.Sub(e.lastListed) < discoveredGrace || e.rs.up.Load() {
			continue
		}
		log.Infof("Router %s no longer listed by the site: closing its session", addr)
		close(e.rs.stop)
		delete(a.routers.entries, addr)
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[len(id)-8:]
	}
	return id
}
