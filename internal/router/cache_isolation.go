// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"strings"

	"hivenet_router/internal/domain"
)

// cacheIsolationGaps returns the generation paths an agent serves without
// per-tenant prefix-cache isolation. Only LLM agents are checked, and only
// while the router isolates tenants (--cache-salt): otherwise nothing is
// promised, so nothing is a gap.
func (r *Router) cacheIsolationGaps(a *domain.Agent) []string {
	if r.salter == nil || a.Metadata.Capability != domain.CapabilityLLM {
		return nil
	}
	return r.salter.Gaps(a.Metadata.Engine)
}

// checkCacheIsolation warns when a newly registered LLM agent leaves a path
// without prefix-cache isolation, and refreshes hivenet_cache_isolation_gaps.
func (r *Router) checkCacheIsolation(a *domain.Agent) {
	if gaps := r.cacheIsolationGaps(a); len(gaps) > 0 {
		log.Warnf("Agent %s (engine %q, model %s) has no per-tenant prefix-cache isolation on %s: on a shared engine, another tenant can infer prompts from cache timing",
			shortID(a.ID), a.Metadata.Engine, a.Metadata.Model, strings.Join(gaps, ", "))
	}
	r.refreshCacheIsolationGaps()
}

// refreshCacheIsolationGaps recounts the agents with a gap per model and
// engine. Called when an agent registers or is removed.
func (r *Router) refreshCacheIsolationGaps() {
	if r.salter == nil || r.metrics == nil {
		return
	}
	counts := map[[2]string]int{}
	for _, a := range r.agents.List() {
		if len(r.cacheIsolationGaps(a)) > 0 {
			counts[[2]string{a.Metadata.Model, a.Metadata.Engine}]++
		}
	}
	r.metrics.SetCacheIsolationGaps(counts)
}
