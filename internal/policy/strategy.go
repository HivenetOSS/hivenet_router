// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import "hivenet_router/internal/domain"

// Strategy selects exactly one agent from a non-empty slice of scored candidates.
// Implementations must not mutate the candidates slice.
// To add a new strategy: implement this interface and register it in registry below.
type Strategy interface {
	Select(candidates []ScoredCandidate, req SelectRequest) *domain.Agent
}

// SelectRequest carries what a strategy may know about the request it routes.
type SelectRequest struct {
	// Model is the requested model.
	Model string
	// Prefix holds the request's anchors when a step of its policy routes on
	// them (see PrefixStrategy); nil otherwise.
	Prefix *PrefixRequest
}

// PrefixStrategy is implemented by a strategy that routes on the request's
// prefix anchors: the processor computes them only when it does.
type PrefixStrategy interface {
	WantsPrefix() bool
}

// Committer is implemented by a strategy that records its choice once the
// executor has acquired a slot on the chosen agent.
type Committer interface {
	Commit(req SelectRequest, chosen *domain.Agent)
}

// registry maps strategy names to their implementations.
// Validate checks names against this map at policy load time.
var registry = map[string]Strategy{
	"least-loaded": leastLoadedStrategy{},
	// "lowest-srtt":  lowestSRTTStrategy{},   // add when implemented
	// "round-robin":  roundRobinStrategy{},   // add when implemented
	// "prefix-aware" is built per policy step from its prefix_aware block
	// (Executor.resolve), so it is not a registry entry.
}

// Get returns the Strategy registered for name, or nil if not found.
func Get(name string) Strategy {
	return registry[name]
}

// leastLoadedStrategy picks the candidate with the lowest load (see loadRatio).
// It is the default strategy and the only one implemented in v1.
// Ties are broken by iteration order (non-deterministic for equal load).
type leastLoadedStrategy struct{}

func (leastLoadedStrategy) Select(candidates []ScoredCandidate, _ SelectRequest) *domain.Agent {
	if len(candidates) == 0 {
		// Contract violation: executor must never call Select with an empty slice.
		// Panic with a clear message to surface this as a programming error during testing.
		panic("policy: Select called with empty candidates slice — this is a bug in the executor")
	}
	// A ratio and a raw count do not compare: if any candidate declares no
	// capacity, every candidate is ranked by its count.
	byCount := false
	for _, c := range candidates {
		if c.Agent.Metadata.Capacity <= 0 {
			byCount = true
			break
		}
	}
	best := candidates[0].Agent
	bestRatio := loadRatio(best, byCount)

	for _, c := range candidates[1:] {
		if r := loadRatio(c.Agent, byCount); r < bestRatio {
			best = c.Agent
			bestRatio = r
		}
	}
	return best
}

// loadRatio ranks an agent for least-loaded: load/capacity when the agent
// declares a capacity and byCount is false, else its load in requests
// (replicas of one model are alike, so the absolute count compares them
// fairly). The load is the larger
// of this router's in-flight requests and the agent's own report, so several
// routers spread work by the agent's real load.
func loadRatio(a *domain.Agent, byCount bool) float64 {
	if a.Metadata.Capacity > 0 && !byCount {
		return float64(a.GetLoad()) / float64(a.Metadata.Capacity)
	}
	return float64(a.GetLoad())
}
