// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import "hivenet_router/internal/domain"

// Strategy selects exactly one agent from a non-empty slice of scored candidates.
// Implementations must not mutate the candidates slice.
// To add a new strategy: implement this interface and register it in registry below.
type Strategy interface {
	Select(candidates []ScoredCandidate) *domain.Agent
}

// registry maps strategy names to their implementations.
// Validate checks names against this map at policy load time.
var registry = map[string]Strategy{
	"least-loaded": leastLoadedStrategy{},
	// "lowest-srtt":  lowestSRTTStrategy{},   // add when implemented
	// "round-robin":  roundRobinStrategy{},   // add when implemented
	// "prefix-aware": prefixAwareStrategy{},  // add when implemented
}

// Get returns the Strategy registered for name, or nil if not found.
func Get(name string) Strategy {
	return registry[name]
}

// leastLoadedStrategy picks the candidate with the lowest active_requests/capacity ratio.
// It is the default strategy and the only one implemented in v1.
// Ties are broken by iteration order (non-deterministic for equal load).
type leastLoadedStrategy struct{}

func (leastLoadedStrategy) Select(candidates []ScoredCandidate) *domain.Agent {
	if len(candidates) == 0 {
		// Contract violation: executor must never call Select with an empty slice.
		// Panic with a clear message to surface this as a programming error during testing.
		panic("policy: Select called with empty candidates slice — this is a bug in the executor")
	}
	best := candidates[0].Agent
	bestRatio := loadRatio(best)

	for _, c := range candidates[1:] {
		if r := loadRatio(c.Agent); r < bestRatio {
			best = c.Agent
			bestRatio = r
		}
	}
	return best
}

// loadRatio returns active_requests/capacity.
// Agents with capacity ≤ 0 are ranked last (ratio = 1.0).
func loadRatio(a *domain.Agent) float64 {
	if a.Metadata.Capacity <= 0 {
		return 1.0
	}
	return float64(a.GetLoad()) / float64(a.Metadata.Capacity)
}
