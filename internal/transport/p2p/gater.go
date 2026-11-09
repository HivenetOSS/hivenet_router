// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package p2p

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// RouterGater is the agent's libp2p connection gater. It lets the agent talk
// to the routers it authenticated against, and to nobody else.
//
// The agent only ever needs one kind of libp2p peer: its router. The router
// forwards inference over the connection the agent opened, so an agent never
// has to accept a connection from anyone. Without this gate, any peer able to
// reach the agent's libp2p port could open an inference stream directly and
// bypass the router's API keys, quotas and policy.
//
// Allowed peers are learned from the gRPC auth responses (each router's peer
// ID). An agent connected to several routers adds each one as its session
// comes up (AddRouter) and removes it when the session ends (RemoveRouter).
type RouterGater struct {
	mu      sync.RWMutex
	allowed map[peer.ID]int // sessions that allow each router
}

// NewRouterGater returns a gater that allows no peer until SetRouters is called.
func NewRouterGater() *RouterGater {
	return &RouterGater{allowed: map[peer.ID]int{}}
}

// SetRouters replaces the set of allowed router peer IDs.
func (g *RouterGater) SetRouters(ids ...peer.ID) {
	allowed := make(map[peer.ID]int, len(ids))
	for _, id := range ids {
		allowed[id] = 1
	}
	g.mu.Lock()
	g.allowed = allowed
	g.mu.Unlock()
}

// AddRouter allows one more router, keeping the others. Each call is one
// session; the router stays allowed until every session has removed it (two
// addresses can reach the same router).
func (g *RouterGater) AddRouter(id peer.ID) {
	g.mu.Lock()
	g.allowed[id]++
	g.mu.Unlock()
}

// RemoveRouter ends one session's allowance for a router.
func (g *RouterGater) RemoveRouter(id peer.ID) {
	g.mu.Lock()
	if g.allowed[id] <= 1 {
		delete(g.allowed, id)
	} else {
		g.allowed[id]--
	}
	g.mu.Unlock()
}

// Allowed reports whether p is one of the routers this agent authenticated against.
func (g *RouterGater) Allowed(p peer.ID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.allowed[p]
	return ok
}

// InterceptPeerDial allows outbound dials to allowed routers only.
func (g *RouterGater) InterceptPeerDial(p peer.ID) bool { return g.Allowed(p) }

// InterceptAddrDial allows outbound dials to allowed routers only.
func (g *RouterGater) InterceptAddrDial(p peer.ID, _ multiaddr.Multiaddr) bool {
	return g.Allowed(p)
}

// InterceptAccept lets an inbound connection reach the security handshake; the
// remote peer is not known yet. InterceptSecured decides once it is.
func (g *RouterGater) InterceptAccept(network.ConnMultiaddrs) bool { return true }

// InterceptSecured drops any connection whose authenticated peer is not an allowed router.
func (g *RouterGater) InterceptSecured(_ network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	return g.Allowed(p)
}

// InterceptUpgraded re-checks the peer once the connection is fully upgraded.
func (g *RouterGater) InterceptUpgraded(c network.Conn) (bool, control.DisconnectReason) {
	return g.Allowed(c.RemotePeer()), 0
}
