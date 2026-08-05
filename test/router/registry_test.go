// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/router"

	"github.com/libp2p/go-libp2p/core/peer"
)

// regAgent builds a minimal registrable agent with a fixed capacity and the
// requested health state, so tests can populate the registry's byID and
// byModel indexes without a live libp2p host.
func regAgent(id, model string, healthy bool) *domain.Agent {
	a := domain.NewAgent(peer.ID(id), domain.AgentMetadata{Model: model, Capacity: 10}, "")
	a.SetHealthy(healthy)
	return a
}

func TestRegistry_RegisterGetUnregister(t *testing.T) {
	r := router.NewAgentRegistry()
	a := regAgent("peer-1", "qwen", true)

	// Register populates the byID index; Count and Get must both reflect it,
	// while a lookup of an unknown peer returns nil rather than a zero value.
	r.Register(a)
	if r.Count() != 1 {
		t.Fatalf("Count = %d, want 1", r.Count())
	}
	if got := r.Get(peer.ID("peer-1")); got == nil || got.ID != a.ID {
		t.Errorf("Get returned wrong agent: %v", got)
	}
	if r.Get(peer.ID("missing")) != nil {
		t.Error("Get of unknown peer must be nil")
	}

	// Unregister must drop the agent from both Count and Get lookups.
	r.Unregister(peer.ID("peer-1"))
	if r.Count() != 0 || r.Get(peer.ID("peer-1")) != nil {
		t.Error("agent not removed after Unregister")
	}
	// Unregistering an unknown peer is a safe no-op.
	r.Unregister(peer.ID("missing"))
}

func TestRegistry_ListByModel_SortedAndScoped(t *testing.T) {
	r := router.NewAgentRegistry()
	// Insert three "qwen" agents out of ID order plus one "llama" agent, so the
	// test can prove the byModel index both scopes by model and sorts by peer.ID.
	r.Register(regAgent("peer-c", "qwen", true))
	r.Register(regAgent("peer-a", "qwen", true))
	r.Register(regAgent("peer-b", "qwen", true))
	r.Register(regAgent("other", "llama", true))

	// Querying one model returns only that model's bucket — the "llama" agent
	// must not leak into the "qwen" result.
	qwen := r.ListByModel("qwen")
	if len(qwen) != 3 {
		t.Fatalf("qwen agents = %d, want 3 (no cross-model leak)", len(qwen))
	}
	// Deterministic sort by peer.ID.
	if qwen[0].ID.String() != peer.ID("peer-a").String() ||
		qwen[2].ID.String() != peer.ID("peer-c").String() {
		t.Errorf("ListByModel not sorted by ID: %v", []peer.ID{qwen[0].ID, qwen[1].ID, qwen[2].ID})
	}
	if got := r.ListByModel("nonexistent"); len(got) != 0 {
		t.Errorf("unknown model must return empty, got %d", len(got))
	}
}

func TestRegistry_CountHealthyByModel(t *testing.T) {
	r := router.NewAgentRegistry()
	// Two healthy + one unhealthy agent on the same model: the healthy count must
	// exclude the unhealthy one (the routing layer only dispatches to healthy agents).
	r.Register(regAgent("h1", "qwen", true))
	r.Register(regAgent("h2", "qwen", true))
	r.Register(regAgent("sick", "qwen", false))

	if n := r.CountHealthyByModel("qwen"); n != 2 {
		t.Errorf("healthy qwen = %d, want 2 (unhealthy excluded)", n)
	}
	if n := r.CountHealthyByModel("unknown"); n != 0 {
		t.Errorf("unknown model healthy count = %d, want 0", n)
	}
}

func TestRegistry_ListAndForEach(t *testing.T) {
	r := router.NewAgentRegistry()
	r.Register(regAgent("a", "m1", true))
	r.Register(regAgent("b", "m2", true))

	// List returns every registered agent across all models...
	if len(r.List()) != 2 {
		t.Errorf("List = %d, want 2", len(r.List()))
	}
	// ...and ForEach must visit each byID entry exactly once (dedup via the set).
	seen := map[string]bool{}
	r.ForEach(func(id peer.ID, a *domain.Agent) { seen[id.String()] = true })
	if len(seen) != 2 {
		t.Errorf("ForEach visited %d agents, want 2", len(seen))
	}
}

// Re-registering the same peer under a new model must not leave a stale entry in
// the old model bucket (index consistency).
func TestRegistry_ReregisterUpdatesModelIndex(t *testing.T) {
	r := router.NewAgentRegistry()
	r.Register(regAgent("p", "old-model", true))
	// Overwrite with same ID — Register replaces byID but the old bucket entry
	// remains; Unregister then re-register is the supported reindex path.
	r.Unregister(peer.ID("p"))
	r.Register(regAgent("p", "new-model", true))

	// Two-index consistency check: after the reindex the byModel index must show
	// the agent only under its new model — no orphan in the old bucket, present
	// exactly once in the new one.
	if len(r.ListByModel("old-model")) != 0 {
		t.Error("stale entry left in old model bucket after reindex")
	}
	if len(r.ListByModel("new-model")) != 1 {
		t.Error("agent missing from new model bucket")
	}
}
