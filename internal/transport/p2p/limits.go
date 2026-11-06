// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package p2p

import (
	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

// UnlimitedStreams lifts libp2p's stream counts from base while keeping its
// connection, memory and file-descriptor limits.
//
// Every request the router forwards is one stream to an agent. libp2p's
// defaults cap streams per protocol (about 2,000 outbound across all peers)
// and per peer (a few hundred), which would cap a router at a few thousand
// requests in flight and an agent at a few hundred, below what a site of
// GPUs serves. Both ends only accept peers they authenticated (the router
// checks the agent's JWT session, the agent's gater allows only its routers),
// and occupancy admission bounds the work a router forwards, so the stream
// counts are left to those limits.
func UnlimitedStreams(base rcmgr.ConcreteLimitConfig) rcmgr.ConcreteLimitConfig {
	open := rcmgr.ResourceLimits{
		Streams:         rcmgr.Unlimited,
		StreamsInbound:  rcmgr.Unlimited,
		StreamsOutbound: rcmgr.Unlimited,
	}
	return rcmgr.PartialLimitConfig{
		System:              open,
		Transient:           open,
		ServiceDefault:      open,
		ServicePeerDefault:  open,
		ProtocolDefault:     open,
		ProtocolPeerDefault: open,
		PeerDefault:         open,
	}.Build(base)
}

// agentResourceManager is the agent's resource manager: libp2p's default
// limits with the stream counts lifted (see UnlimitedStreams).
func agentResourceManager() (network.ResourceManager, error) {
	limits := rcmgr.DefaultLimits
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(UnlimitedStreams(limits.AutoScale())))
}
