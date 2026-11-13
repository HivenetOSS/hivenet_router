// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"math"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Defaults and bounds of the prefix_aware block.
const (
	DefaultPrefixLoadFactor   = 1.25
	DefaultPrefixMinLoad      = 4
	DefaultPrefixVirtualNodes = 100
	DefaultPrefixKVThreshold  = 0.80
	DefaultPrefixBytes        = 4096
)

// Outcomes of a prefix-aware selection, the label of PrefixMetrics.
const (
	PrefixOutcomeHome     = "home"     // the key's first agent on the ring
	PrefixOutcomeSpill    = "spill"    // a later agent on the ring: the earlier ones were busy or excluded
	PrefixOutcomeFallback = "fallback" // every candidate over the bound: least-loaded
	PrefixOutcomeNoKey    = "no_key"   // the request had no text to hash: least-loaded
)

// PrefixMetrics observes prefix-aware selections. Implemented by
// *metrics.RouterMetrics; may be nil (e.g. in unit tests).
type PrefixMetrics interface {
	PrefixRouted(model, outcome string)
}

// prefixAwareStrategy routes requests that share a prompt prefix to the same
// agent, so its prefix cache is reused: consistent hashing with bounded loads
// (CHWBL). The routing key picks a point on a ring of the model's agents; the
// walk clockwise from it takes the first candidate whose load is within the
// bound (at least MinLoad) and, when the engine reports it, whose KV cache is
// below kvThreshold.
// When none is, it falls back to least-loaded.
type prefixAwareStrategy struct {
	PrefixAwareParams // resolved: no zero field
	rings             *ringCache
	metrics           PrefixMetrics
}

func (e *Executor) newPrefixAware(p *PrefixAwareParams) prefixAwareStrategy {
	return prefixAwareStrategy{PrefixAwareParams: p.resolved(), rings: &e.rings, metrics: e.prefixMetrics}
}

// resolved returns p with every unset field at its default.
func (p *PrefixAwareParams) resolved() PrefixAwareParams {
	var r PrefixAwareParams
	if p != nil {
		r = *p
	}
	if r.LoadFactor == 0 {
		r.LoadFactor = DefaultPrefixLoadFactor
	}
	if r.MinLoad == 0 {
		r.MinLoad = DefaultPrefixMinLoad
	}
	if r.VirtualNodes == 0 {
		r.VirtualNodes = DefaultPrefixVirtualNodes
	}
	if r.KVThreshold == 0 {
		r.KVThreshold = DefaultPrefixKVThreshold
	}
	if r.PrefixBytes == 0 {
		r.PrefixBytes = DefaultPrefixBytes
	}
	return r
}

// PrefixBytes implements KeyedStrategy.
func (s prefixAwareStrategy) PrefixBytes() int { return s.PrefixAwareParams.PrefixBytes }

func (s prefixAwareStrategy) Select(candidates []ScoredCandidate, req SelectRequest) *domain.Agent {
	if len(candidates) == 0 {
		panic("policy: Select called with empty candidates slice — this is a bug in the executor")
	}
	if req.Key == nil {
		s.observe(req.Model, PrefixOutcomeNoKey)
		return leastLoadedStrategy{}.Select(candidates, req)
	}

	pool := req.Pool
	if len(pool) == 0 {
		pool = make([]*domain.Agent, len(candidates))
		for i, c := range candidates {
			pool[i] = c.Agent
		}
	}
	ring := s.rings.get(req.Model, s.VirtualNodes, pool)

	byID := make(map[peer.ID]*ScoredCandidate, len(candidates))
	total := 0
	for i := range candidates {
		byID[candidates[i].Agent.ID] = &candidates[i]
		total += candidates[i].Agent.GetLoad()
	}
	// The CHWBL bound: no agent takes more than load_factor times its fair
	// share of the requests in flight, this one included — but never less
	// than min_load, so low traffic keeps its affinity.
	bound := max(int(math.Ceil(s.LoadFactor*float64(total+1)/float64(len(candidates)))), s.MinLoad)

	var chosen *domain.Agent
	outcome := PrefixOutcomeHome
	ring.walk(keyPoint(req.Key), func(id peer.ID) bool {
		c, ok := byID[id]
		if ok && c.Agent.GetLoad() < bound && !s.kvFull(c.Snapshot) {
			chosen = c.Agent
			return true
		}
		outcome = PrefixOutcomeSpill
		return false
	})
	if chosen == nil {
		s.observe(req.Model, PrefixOutcomeFallback)
		return leastLoadedStrategy{}.Select(candidates, req)
	}
	s.observe(req.Model, outcome)
	return chosen
}

// kvFull reports whether the agent's KV cache is too full to cache another
// prefix: newly cached blocks would likely be evicted before reuse. An engine
// that does not report it never counts as full.
func (s prefixAwareStrategy) kvFull(snap AgentSnapshot) bool {
	return snap.KVCacheUtilization != nil && *snap.KVCacheUtilization >= s.KVThreshold
}

func (s prefixAwareStrategy) observe(model, outcome string) {
	if s.metrics != nil {
		s.metrics.PrefixRouted(model, outcome)
	}
}
