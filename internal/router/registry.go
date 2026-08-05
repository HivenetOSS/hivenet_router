// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"sort"
	"sync"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
)

// AgentRegistry stores and manages all connected agents in memory.
// It maintains two indexes:
//   - byID:    peer.ID → *Agent        — O(1) Get, Unregister
//   - byModel: model   → peer.ID → *Agent — O(1) ListByModel (no cross-model scan)
//
// Both indexes are updated atomically under a single RWMutex so readers always
// see a consistent view. Each agent belongs to exactly one model bucket.
type AgentRegistry struct {
	byID    map[peer.ID]*domain.Agent            // primary index
	byModel map[string]map[peer.ID]*domain.Agent // secondary index: model → agents
	mu      sync.RWMutex
}

// NewAgentRegistry creates a new agent registry.
func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		byID:    make(map[peer.ID]*domain.Agent),
		byModel: make(map[string]map[peer.ID]*domain.Agent),
	}
}

// Register adds an agent to both indexes.
func (r *AgentRegistry) Register(agent *domain.Agent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byID[agent.ID] = agent

	model := agent.Metadata.Model
	if r.byModel[model] == nil {
		r.byModel[model] = make(map[peer.ID]*domain.Agent)
	}
	r.byModel[model][agent.ID] = agent
}

// Unregister removes an agent from both indexes by peer ID.
func (r *AgentRegistry) Unregister(id peer.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if agent, ok := r.byID[id]; ok {
		delete(r.byModel[agent.Metadata.Model], id)
		delete(r.byID, id)
	}
}

// Get retrieves an agent by peer ID.
func (r *AgentRegistry) Get(id peer.ID) *domain.Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.byID[id]
}

// ListByModel returns all agents registered for the given model.
// Only agents serving that exact model are returned — no cross-model scan.
// The returned slice is sorted by peer.ID so callers receive a deterministic
// order on every call, regardless of Go's map iteration randomness. This matters
// for strategies that depend on stable ordering (round-robin, fair tie-breaking).
// The caller receives a snapshot slice; the registry is not held during iteration.
func (r *AgentRegistry) ListByModel(model string) []*domain.Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bucket := r.byModel[model]
	agents := make([]*domain.Agent, 0, len(bucket))
	for _, agent := range bucket {
		agents = append(agents, agent)
	}
	sort.Slice(agents, func(i, j int) bool {
		return agents[i].ID < agents[j].ID
	})
	return agents
}

// List returns all registered agents across all models.
// Prefer ListByModel when the model is known — it avoids a cross-model scan.
func (r *AgentRegistry) List() []*domain.Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	agents := make([]*domain.Agent, 0, len(r.byID))
	for _, agent := range r.byID {
		agents = append(agents, agent)
	}
	return agents
}

// Count returns the total number of registered agents across all models.
func (r *AgentRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID)
}

// CountHealthyByModel returns the number of currently healthy agents serving
// the given model. Used by per-model rate limiting to compute the effective
// ceiling as requests_per_minute_per_replica × healthy_replicas at admission
// time. An unknown model returns 0; the caller treats that as "no backends
// available" and skips quota (the routing layer surfaces its own clearer error
// downstream).
func (r *AgentRegistry) CountHealthyByModel(model string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, agent := range r.byModel[model] {
		if agent.IsHealthy() {
			n++
		}
	}
	return n
}

// ForEach executes a function for each agent across all models.
func (r *AgentRegistry) ForEach(fn func(peer.ID, *domain.Agent)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, agent := range r.byID {
		fn(id, agent)
	}
}
