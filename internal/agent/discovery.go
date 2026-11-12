// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	p2phttp "github.com/libp2p/go-libp2p/p2p/http"
)

// Router discovery. The agent starts from its --routers list (the seeds).
// Every router of a site that shares a store hands the agent the site's live
// routers, in the authentication response and in every heartbeat reply. The
// agent opens a session with each router it does not have one with. A router
// is matched by address, or by peer ID against the entry of a seed or of a
// session that is up (a seed given as a DNS name, the router listed under its
// pod IP). A discovered router is closed once it has been missing from every
// list for discoveredGrace and its session is down, or at once when a seed
// that is up reaches the same router. Seeds are never closed.

// discoveredGrace is how long a discovered router may be missing from the
// lists before its session is closed (only while that session is down). A
// list can be short for a while (the store unreachable, a router restarting).
const discoveredGrace = 2 * time.Minute

// maxDiscovered bounds the discovered routers the agent keeps at once.
const maxDiscovered = 32

// siteRouter is one entry of a router's list.
type siteRouter struct {
	addr   string // gRPC address
	peerID string // libp2p peer ID
}

// routerEntry is one router address the agent keeps a session with.
type routerEntry struct {
	rs   *routerSession
	seed bool
	// peerID is the router's peer ID: the one its session authenticated with,
	// else the one the list advertised. Empty for a seed not yet reached.
	peerID     string
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
	a.routers.node, a.routers.httpHost = node, httpHost
	for _, addr := range seeds {
		a.addRouterLocked(addr, true, "")
	}
	a.routers.mu.Unlock()
	go a.pruneRouters()
}

// waitRouters blocks until every session has ended (the agent stopped).
func (a *Agent) waitRouters() { a.routers.wg.Wait() }

// addRouterLocked opens a session with addr unless one is open already.
// Caller holds a.routers.mu.
func (a *Agent) addRouterLocked(addr string, seed bool, peerID string) {
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

// setPeer records the peer ID a session authenticated with, so a list that
// names the router under another address matches its entry.
func (a *Agent) setPeer(rs *routerSession, p peer.ID) {
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	if e := a.routers.entries[rs.addr]; e != nil && e.rs == rs {
		e.peerID = p.String()
	}
}

// sessionUp marks a session registered and serving. A seed that comes up
// closes at once the discovered session to the same router (opened while the
// seed was down, for example a router back with a new peer ID): the router
// keeps one session per agent, so the two would keep replacing each other
// until the next prune.
func (a *Agent) sessionUp(rs *routerSession) {
	rs.up.Store(true)
	a.pruneOnce(time.Now())
}

// learnRouters takes a router's list of the site's live routers. An empty list
// changes nothing: it may only mean the store is unreachable.
func (a *Agent) learnRouters(list []siteRouter) {
	if len(list) == 0 {
		return
	}
	now := time.Now()
	a.routers.mu.Lock()
	defer a.routers.mu.Unlock()
	for _, sr := range list {
		if sr.addr == "" || sr.peerID == "" {
			continue
		}
		if e := a.routers.entries[sr.addr]; e != nil {
			e.lastListed = now
			continue
		}
		// Only the address keeps an entry listed: a router listed under a
		// new address lets the old entry age out.
		if a.servedLocked(sr.peerID) {
			continue
		}
		if a.discoveredLocked() >= maxDiscovered {
			log.Warnf("Router %s listed, but the agent already keeps %d discovered routers: not opening a session", sr.addr, maxDiscovered)
			continue
		}
		a.addRouterLocked(sr.addr, false, sr.peerID)
	}
}

// servedLocked reports whether an entry under another address already reaches
// the router: a seed (it reconnects on its own), or an entry whose session is
// up. A discovered entry whose session is down does not count: the router may
// have moved to the address now listed. Caller holds a.routers.mu.
func (a *Agent) servedLocked(peerID string) bool {
	for _, e := range a.routers.entries {
		if e.peerID == peerID && (e.seed || e.rs.up.Load()) {
			return true
		}
	}
	return false
}

// seedUpLocked reports whether a seed whose session is up reaches the router.
// Caller holds a.routers.mu.
func (a *Agent) seedUpLocked(peerID string) bool {
	for _, e := range a.routers.entries {
		if e.seed && e.peerID == peerID && e.rs.up.Load() {
			return true
		}
	}
	return false
}

func (a *Agent) discoveredLocked() int {
	n := 0
	for _, e := range a.routers.entries {
		if !e.seed {
			n++
		}
	}
	return n
}

// pruneRouters closes the sessions of discovered routers missing from the
// lists for discoveredGrace whose session is down, and of those a seed that is
// up already reaches.
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
		if e.seed {
			continue
		}
		switch {
		case e.peerID != "" && a.seedUpLocked(e.peerID):
			log.Infof("Router %s is also reached through a seed: closing the second session", addr)
		case now.Sub(e.lastListed) >= discoveredGrace && !e.rs.up.Load():
			log.Infof("Router %s no longer listed by the site: closing its session", addr)
		default:
			continue
		}
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
