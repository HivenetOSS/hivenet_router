// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/test/testutil"
)

// TestHealthMonitor_UnhealthyFlipPublishesUnregistered pins the
// silent-disconnect contract: when the healthMonitor marks an agent
// unhealthy on a missed heartbeat, it publishes an "unregistered"
// registration event (same shape as the disconnect handler's and the
// reaper's), so the registration watcher learns within the heartbeat
// timeout instead of waiting for the next 30s snapshot tick. The event
// fires on the healthy→unhealthy transition only — later ticks for the
// same stale agent must not republish.
func TestHealthMonitor_UnhealthyFlipPublishesUnregistered(t *testing.T) {
	const (
		tick           = 20 * time.Millisecond
		unhealthyAfter = 50 * time.Millisecond
	)
	cfg := &config.Config{
		HealthCheckInterval: tick,
		UnhealthyAfter:      unhealthyAfter,
		RemoveAfter:         time.Hour, // keep the agent: only the flip, not the removal, is under test
	}
	m := metrics.NewRouterMetrics()
	stor := testutil.NoopStorage{}
	r := &Router{
		cfg:                  cfg,
		agents:               NewAgentRegistry(),
		storage:              stor,
		metrics:              m,
		counters:             metrics.NewUniversalCounterStore(stor, m),
		sessionManager:       auth.NewSessionManager(time.Hour),
		registrationNotifier: NewRegistrationNotifier(),
	}

	id := peer.ID("agent-silent-1")
	a := domain.NewAgent(id, domain.AgentMetadata{
		Model:        "m",
		DeploymentID: "dep-1",
		ReplicaID:    "rep-1",
	}, "tok")
	// Backdate the last heartbeat past UnhealthyAfter so the first tick
	// sees a missed heartbeat (the silent-disconnect case: the agent is
	// listed but no longer answering).
	a.Lock()
	a.LastSeen = time.Now().Add(-time.Second)
	a.Unlock()
	r.agents.Register(a)

	sub := r.registrationNotifier.Subscribe()
	defer r.registrationNotifier.Unsubscribe(sub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.healthMonitor(ctx)
	}()

	// The transition must publish exactly one "unregistered" event with
	// the disconnect handler's payload shape.
	select {
	case ev := <-sub.Events:
		if ev.EventType != domain.RegistrationUnregistered {
			t.Fatalf("event type = %q, want %q", ev.EventType, domain.RegistrationUnregistered)
		}
		if ev.DeploymentID != "dep-1" || ev.ReplicaID != "rep-1" {
			t.Fatalf("event join key = (%q, %q), want (dep-1, rep-1)", ev.DeploymentID, ev.ReplicaID)
		}
		if ev.AgentID != id.String() {
			t.Fatalf("event agent = %q, want %q", ev.AgentID, id.String())
		}
		if ev.Timestamp.IsZero() {
			t.Fatal("event has a zero timestamp")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the healthy→unhealthy transition did not publish an unregistered event within 3s")
	}

	// A second tick (the agent is stale for longer now) must NOT
	// republish: the event fires on the transition only.
	select {
	case ev := <-sub.Events:
		t.Fatalf("republished event on a non-transition tick: %+v", ev)
	case <-time.After(3 * tick):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("healthMonitor did not stop when its context was cancelled")
	}
}
