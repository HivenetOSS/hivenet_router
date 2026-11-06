// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "testing"

// Before any report the load is this router's own in-flight count.
func TestLoadWithoutReport(t *testing.T) {
	a := NewAgent("p", AgentMetadata{}, "")
	a.TryAcquireSlot()
	a.TryAcquireSlot()
	if got := a.GetLoad(); got != 2 {
		t.Fatalf("load %d, want 2", got)
	}
}

// The agent's report covers every router; requests sent after it are added.
func TestLoadFollowsAgentReport(t *testing.T) {
	a := NewAgent("p", AgentMetadata{}, "")
	a.TryAcquireSlot() // ours: 1
	a.SetReportedLoad(10)
	if got := a.GetLoad(); got != 10 {
		t.Fatalf("load %d after a report of 10, want 10", got)
	}
	a.TryAcquireSlot()
	a.TryAcquireSlot()
	if got := a.GetLoad(); got != 12 {
		t.Fatalf("load %d, want report 10 + 2 sent since", got)
	}
	a.SetReportedLoad(0) // the others finished
	if got := a.GetLoad(); got != 3 {
		t.Fatalf("load %d, want our own 3 in flight", got)
	}
}

// Without a declared capacity nothing caps the slots; a positive one still does.
func TestCapacityOnlyWhenDeclared(t *testing.T) {
	a := NewAgent("p", AgentMetadata{}, "")
	for i := 0; i < 100; i++ {
		if !a.TryAcquireSlot() {
			t.Fatalf("slot %d refused without a declared capacity", i)
		}
	}
	b := NewAgent("q", AgentMetadata{Capacity: 1}, "")
	b.SetReportedLoad(1)
	if b.TryAcquireSlot() {
		t.Fatal("slot granted although the reported load fills the declared capacity")
	}
}
