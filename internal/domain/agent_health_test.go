// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"testing"
	"time"
)

// TestMarkUnhealthyIfStale pins the health monitor's check-and-flip contract:
// the flip only lands when the agent is still stale under its own lock, so a
// heartbeat that refreshed LastSeen (or re-marked it healthy) between the
// caller's snapshot and the call is never clobbered.
func TestMarkUnhealthyIfStale(t *testing.T) {
	const staleAfter = 50 * time.Millisecond

	t.Run("fresh agent is left healthy", func(t *testing.T) {
		a := NewAgent("p", AgentMetadata{}, "tok")
		stale, wasHealthy, _ := a.MarkUnhealthyIfStale(staleAfter)
		if stale {
			t.Fatal("reported a fresh agent as stale")
		}
		if !wasHealthy || !a.IsHealthy() {
			t.Fatal("a fresh agent was marked unhealthy")
		}
	})

	t.Run("stale healthy agent flips and reports the transition", func(t *testing.T) {
		a := NewAgent("p", AgentMetadata{}, "tok")
		a.Lock()
		a.LastSeen = time.Now().Add(-time.Second)
		a.Unlock()

		stale, wasHealthy, lastSeen := a.MarkUnhealthyIfStale(staleAfter)
		if !stale {
			t.Fatal("did not report a stale agent as stale")
		}
		if !wasHealthy {
			t.Fatal("did not report the healthy→unhealthy transition")
		}
		if a.IsHealthy() {
			t.Fatal("stale agent stayed healthy")
		}
		if time.Since(lastSeen) < time.Second {
			t.Fatalf("returned lastSeen %v, want the stale timestamp", lastSeen)
		}
	})

	t.Run("stale already-unhealthy agent does not repeat the transition", func(t *testing.T) {
		a := NewAgent("p", AgentMetadata{}, "tok")
		a.SetHealthy(false)
		a.Lock()
		a.LastSeen = time.Now().Add(-time.Second)
		a.Unlock()

		stale, wasHealthy, _ := a.MarkUnhealthyIfStale(staleAfter)
		if !stale || wasHealthy {
			t.Fatalf("stale=%v wasHealthy=%v, want true/false", stale, wasHealthy)
		}
	})

	t.Run("heartbeat that refreshed LastSeen is not clobbered", func(t *testing.T) {
		a := NewAgent("p", AgentMetadata{}, "tok")
		// The scan saw a stale snapshot, but a heartbeat landed first and
		// refreshed LastSeen. The flip must observe the fresh timestamp and
		// leave the agent healthy.
		a.UpdateLastSeen()
		stale, wasHealthy, _ := a.MarkUnhealthyIfStale(staleAfter)
		if stale {
			t.Fatal("clobbered a heartbeat that refreshed LastSeen")
		}
		if !wasHealthy || !a.IsHealthy() {
			t.Fatal("a heartbeating agent was marked unhealthy")
		}
	})
}
