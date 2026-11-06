// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	pb "hivenet_router/proto"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestHeartbeat_DisconnectedAgentStaysUnhealthy checks that a heartbeat
// handled after the agent's connection closed changes nothing: the agent
// stays out of the routing pool and its LastSeen does not move.
func TestHeartbeat_DisconnectedAgentStaysUnhealthy(t *testing.T) {
	routerHost := newTestHost(t)
	agentHost := newTestHost(t)

	r := &Router{sessionManager: auth.NewSessionManager(time.Hour), agents: NewAgentRegistry(), p2pHost: routerHost}
	token := r.sessionManager.CreateSession("agent-x", &pb.AgentMetadata{Model: "m", Capacity: 1})
	if !r.sessionManager.LinkPeerID(token, agentHost.ID()) {
		t.Fatal("LinkPeerID failed")
	}
	a := domain.NewAgent(agentHost.ID(), domain.AgentMetadata{Model: "m", Capacity: 1}, token)
	r.agents.Register(a)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := agentHost.Connect(ctx, peer.AddrInfo{ID: routerHost.ID(), Addrs: routerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "router sees the connection", func() bool { return r.peerConnected(agentHost.ID()) })

	// The connection closes, and the disconnect marks the agent unhealthy.
	if err := routerHost.Network().ClosePeer(agentHost.ID()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "router sees the disconnect", func() bool { return !r.peerConnected(agentHost.ID()) })
	a.SetHealthy(false)
	a.RLock()
	lastSeen := a.LastSeen
	a.RUnlock()

	// Its last heartbeat is handled only now.
	rec := httptest.NewRecorder()
	body := fmt.Sprintf(`{"session_token":%q,"backend_healthy":true}`, token)
	r.handleAgentHeartbeat(rec, httptest.NewRequest(http.MethodPost, "/heartbeat", strings.NewReader(body)))

	if rec.Code != http.StatusConflict {
		t.Errorf("status %d, want 409", rec.Code)
	}
	if a.IsHealthy() {
		t.Error("a heartbeat after the disconnect marked the agent healthy")
	}
	a.RLock()
	moved := !a.LastSeen.Equal(lastSeen)
	a.RUnlock()
	if moved {
		t.Error("a heartbeat after the disconnect moved LastSeen")
	}
}

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() }) //nolint:errcheck
	return h
}

// eventually waits up to 5s for cond: a connection is added and removed on
// each side of it on its own schedule.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestInstallAgent_DisconnectedAgentIsUnhealthy checks that an agent whose
// registration is handled after its connection closed is installed out of
// the routing pool, and one still connected is installed healthy.
func TestInstallAgent_DisconnectedAgentIsUnhealthy(t *testing.T) {
	routerHost := newTestHost(t)
	agentHost := newTestHost(t)
	r := &Router{agents: NewAgentRegistry(), p2pHost: routerHost}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := agentHost.Connect(ctx, peer.AddrInfo{ID: routerHost.ID(), Addrs: routerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "router sees the connection", func() bool { return r.peerConnected(agentHost.ID()) })

	a := domain.NewAgent(agentHost.ID(), domain.AgentMetadata{Model: "m", Capacity: 1}, "t1")
	if !r.installAgent(a) || !a.IsHealthy() {
		t.Fatal("a connected agent was not installed healthy")
	}

	if err := routerHost.Network().ClosePeer(agentHost.ID()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "router sees the disconnect", func() bool { return !r.peerConnected(agentHost.ID()) })

	b := domain.NewAgent(agentHost.ID(), domain.AgentMetadata{Model: "m", Capacity: 1}, "t2")
	if r.installAgent(b) {
		t.Error("installAgent reported a closed connection as up")
	}
	if got := r.agents.Get(agentHost.ID()); got != b || got.IsHealthy() {
		t.Error("an agent registered after its disconnect is in the routing pool")
	}
}
