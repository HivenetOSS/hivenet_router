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

	"github.com/libp2p/go-libp2p/core/peer"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	pb "hivenet_router/proto"
	"hivenet_router/test/testutil"
)

// TestHeartbeat_RecoveryPublishesRegistered pins the counterpart to the
// healthMonitor's "unregistered": when an agent that was out of the pool comes
// back on a heartbeat (no fresh /register — the stall-and-resume case), it must
// publish a "registered" event so a watcher that stamped the replica lost does
// not stay wrong until the next snapshot. A steady-state heartbeat must not
// republish.
func TestHeartbeat_RecoveryPublishesRegistered(t *testing.T) {
	routerHost := newTestHost(t)
	agentHost := newTestHost(t)

	r := &Router{
		sessionManager:       auth.NewSessionManager(time.Hour),
		agents:               NewAgentRegistry(),
		p2pHost:              routerHost,
		storage:              testutil.NoopStorage{},
		metrics:              metrics.NewRouterMetrics(),
		registrationNotifier: NewRegistrationNotifier(),
		executor:             &policy.Executor{},
	}
	token := r.sessionManager.CreateSession("agent-rec", &pb.AgentMetadata{Model: "m", Capacity: 1})
	if !r.sessionManager.LinkPeerID(token, agentHost.ID()) {
		t.Fatal("LinkPeerID failed")
	}
	a := domain.NewAgent(agentHost.ID(), domain.AgentMetadata{
		Model:        "m",
		Capacity:     1,
		DeploymentID: "dep-rec",
		ReplicaID:    "rep-rec",
	}, token)
	r.agents.Register(a)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := agentHost.Connect(ctx, peer.AddrInfo{ID: routerHost.ID(), Addrs: routerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "router sees the connection", func() bool { return r.peerConnected(agentHost.ID()) })

	sub := r.registrationNotifier.Subscribe()
	defer r.registrationNotifier.Unsubscribe(sub)

	// The healthMonitor (or a disconnect) took the agent out of the pool; the
	// agent resumes heartbeating on the same connection without re-registering.
	a.SetHealthy(false)

	heartbeat := func() int {
		rec := httptest.NewRecorder()
		body := fmt.Sprintf(`{"session_token":%q,"backend_healthy":true}`, token)
		r.handleAgentHeartbeat(rec, httptest.NewRequest(http.MethodPost, "/heartbeat", strings.NewReader(body)))
		return rec.Code
	}

	if code := heartbeat(); code != http.StatusOK {
		t.Fatalf("recovery heartbeat status = %d, want 200", code)
	}
	select {
	case ev := <-sub.Events:
		if ev.EventType != domain.RegistrationRegistered {
			t.Fatalf("event type = %q, want %q", ev.EventType, domain.RegistrationRegistered)
		}
		if ev.DeploymentID != "dep-rec" || ev.ReplicaID != "rep-rec" {
			t.Fatalf("event join key = (%q, %q), want (dep-rec, rep-rec)", ev.DeploymentID, ev.ReplicaID)
		}
		if ev.AgentID != agentHost.ID().String() {
			t.Fatalf("event agent = %q, want %q", ev.AgentID, agentHost.ID())
		}
		if ev.Timestamp.IsZero() {
			t.Fatal("event has a zero timestamp")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the recovery heartbeat did not publish a registered event within 3s")
	}

	// Still healthy on the next heartbeat: no second "registered".
	if code := heartbeat(); code != http.StatusOK {
		t.Fatalf("steady-state heartbeat status = %d, want 200", code)
	}
	select {
	case ev := <-sub.Events:
		t.Fatalf("republished registered on a steady-state heartbeat: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}
