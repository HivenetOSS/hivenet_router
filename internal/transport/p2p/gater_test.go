// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	manet "github.com/multiformats/go-multiaddr/net"
)

func randomID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestRouterGaterAllowsOnlySetRouters(t *testing.T) {
	g := NewRouterGater()
	r1, r2, stranger := randomID(t), randomID(t), randomID(t)

	if g.Allowed(r1) {
		t.Fatal("a new gater must allow nobody")
	}
	g.SetRouters(r1, r2)
	if !g.Allowed(r1) || !g.Allowed(r2) {
		t.Fatal("routers set with SetRouters must be allowed")
	}
	if g.Allowed(stranger) || g.Allowed("") {
		t.Fatal("peers that are not routers must be refused")
	}
	g.SetRouters(r2)
	if g.Allowed(r1) {
		t.Fatal("SetRouters must replace the previous set")
	}
}

// newTestHost starts a plain libp2p host on loopback.
func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// TestAgentHostRefusesPeersOtherThanRouter checks the agent's libp2p node end to
// end: it listens on loopback only, lets its router connect, and refuses any
// other peer after the security handshake.
func TestAgentHostRefusesPeersOtherThanRouter(t *testing.T) {
	gater := NewRouterGater()
	agent, err := NewHost(nil, 0, gater)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { agent.Close() })

	for _, a := range agent.Addrs() {
		ip, err := manet.ToIP(a)
		if err == nil && !ip.IsLoopback() {
			t.Fatalf("agent must listen on loopback only, got %s", a)
		}
	}

	router := newTestHost(t)
	stranger := newTestHost(t)
	gater.SetRouters(router.ID())

	agentInfo := peer.AddrInfo{ID: agent.ID(), Addrs: agent.Addrs()}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := stranger.Connect(ctx, agentInfo); err == nil {
		// The dial can report success before the agent's gater closes the
		// connection; the agent must not keep it.
		time.Sleep(200 * time.Millisecond)
		if len(agent.Network().ConnsToPeer(stranger.ID())) != 0 {
			t.Fatal("agent kept a connection from a peer that is not its router")
		}
	}
	if err := router.Connect(ctx, agentInfo); err != nil {
		t.Fatalf("router must be able to connect: %v", err)
	}
	if len(agent.Network().ConnsToPeer(router.ID())) == 0 {
		t.Fatal("agent must keep the router's connection")
	}
}
