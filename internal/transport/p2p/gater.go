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
// Allowed peers are learned from the gRPC auth response (the router's peer
// ID). SetRouters replaces the whole set on every new session, so today an
// agent trusts exactly one router. An agent connected to several routers at
// once will need an additive method; SetRouters alone would drop the others.
type RouterGater struct {
	mu      sync.RWMutex
	allowed map[peer.ID]struct{}
}

// NewRouterGater returns a gater that allows no peer until SetRouters is called.
func NewRouterGater() *RouterGater {
	return &RouterGater{allowed: map[peer.ID]struct{}{}}
}

// SetRouters replaces the set of allowed router peer IDs.
func (g *RouterGater) SetRouters(ids ...peer.ID) {
	allowed := make(map[peer.ID]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	g.mu.Lock()
	g.allowed = allowed
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
