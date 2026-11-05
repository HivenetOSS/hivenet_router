// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestPeerConnected checks the check a heartbeat makes before it marks an
// agent healthy: only a peer with a live connection counts.
func TestPeerConnected(t *testing.T) {
	routerHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer routerHost.Close() //nolint:errcheck
	agentHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer agentHost.Close() //nolint:errcheck

	r := &Router{p2pHost: routerHost}
	if r.peerConnected(agentHost.ID()) {
		t.Fatal("peer with no connection reported connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := agentHost.Connect(ctx, peer.AddrInfo{ID: routerHost.ID(), Addrs: routerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	if !r.peerConnected(agentHost.ID()) {
		t.Fatal("connected peer reported not connected")
	}

	// The agent's connection closes: a heartbeat handled now must not mark
	// it healthy.
	if err := routerHost.Network().ClosePeer(agentHost.ID()); err != nil {
		t.Fatal(err)
	}
	if r.peerConnected(agentHost.ID()) {
		t.Fatal("peer reported connected after its connection closed")
	}

	if !(&Router{}).peerConnected(agentHost.ID()) {
		t.Error("without a libp2p host every peer should count as connected")
	}
}
