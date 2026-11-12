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
// Every request the router forwards is one stream it opens to an agent.
// libp2p's defaults cap streams per protocol (about 2,000 outbound across all
// peers) and per peer (a few hundred), which would cap a router at a few
// thousand requests in flight and an agent at a few hundred, below what a site
// of GPUs serves. Occupancy admission bounds the work a router forwards.
//
// inbound lifts the streams other peers open too. Only a host that accepts
// nothing but authenticated peers may lift them: the agent, whose gater lets
// in only its routers. The router accepts any peer before it checks the
// agent's session, so it lifts outbound streams only and keeps libp2p's
// inbound limits.
func UnlimitedStreams(base rcmgr.ConcreteLimitConfig, inbound bool) rcmgr.ConcreteLimitConfig {
	open := rcmgr.ResourceLimits{
		Streams:         rcmgr.Unlimited,
		StreamsOutbound: rcmgr.Unlimited,
	}
	if inbound {
		open.StreamsInbound = rcmgr.Unlimited
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
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(UnlimitedStreams(limits.AutoScale(), true)))
}
