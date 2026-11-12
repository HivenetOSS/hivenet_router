// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"testing"
	"time"
)

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

// An agent that reports less often than the default keeps its report fresh
// for three of its own intervals, so its router list is not always stale.
func TestReportFreshnessFollowsAgentInterval(t *testing.T) {
	a := NewAgent("p", AgentMetadata{}, "")
	a.SetRouterReport(map[string]int64{"r1": 0})
	a.routersAt = a.routersAt.Add(-4 * time.Second) // a report 4 s old
	if fresh, _ := a.ConnectedTo("r1"); fresh {
		t.Fatal("a 4 s old report is fresh at the default 500 ms interval")
	}
	a.SetReportEvery(2 * time.Second)
	if fresh, connected := a.ConnectedTo("r1"); !fresh || !connected {
		t.Fatal("a 4 s old report from an agent reporting every 2 s is stale")
	}
	if got := a.ReportMaxAge(); got != 6*time.Second {
		t.Fatalf("ReportMaxAge = %v, want 6s", got)
	}
}

// A very long reporting interval does not keep a report in use for more than
// reportMaxAgeCap: a stale "not connected" report must not outlive a reconnect
// for long.
func TestReportFreshnessIsCapped(t *testing.T) {
	a := NewAgent("p", AgentMetadata{}, "")
	a.SetReportEvery(time.Hour)
	if got := a.ReportMaxAge(); got != reportMaxAgeCap {
		t.Fatalf("ReportMaxAge = %v, want %v", got, reportMaxAgeCap)
	}
}
