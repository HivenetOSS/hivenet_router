// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"strings"

	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/domain"
)

// cacheIsolationGaps returns the generation paths an agent serves without
// per-tenant prefix-cache isolation. Only LLM agents are checked, and only
// while the router promises isolation: with --cache-salt, the paths its
// engine (at its version) cannot isolate; on a multi-tenant router whose
// isolation was switched off, every generation path.
func (r *Router) cacheIsolationGaps(a *domain.Agent) []string {
	if a.Metadata.Capability != domain.CapabilityLLM {
		return nil
	}
	switch {
	case r.salter != nil:
		return r.salter.Gaps(a.Metadata.Engine, a.Metadata.EngineVersion)
	case cacheSaltOptedOut(r.cfg):
		return cachesalt.LLMPaths
	}
	return nil
}

// promisesCacheIsolation reports whether gaps are counted at all: with
// --cache-salt, or on a multi-tenant router whose isolation was switched off.
func (r *Router) promisesCacheIsolation() bool {
	return r.salter != nil || cacheSaltOptedOut(r.cfg)
}

// checkCacheIsolation warns when a newly registered LLM agent leaves a path
// without prefix-cache isolation.
func (r *Router) checkCacheIsolation(a *domain.Agent) {
	if gaps := r.cacheIsolationGaps(a); len(gaps) > 0 {
		version := a.Metadata.EngineVersion
		if version == "" {
			version = "unknown"
		}
		log.Warnf("Agent %s (engine %q version %s, model %s) has no per-tenant prefix-cache isolation on %s: on a shared engine, another tenant can infer prompts from cache timing",
			shortID(a.ID), a.Metadata.Engine, version, a.Metadata.Model, strings.Join(gaps, ", "))
	}
}

// cacheIsolationGapCounts counts the registered agents with a gap, per model
// and engine, for hivenet_cache_isolation_gaps. The metric calls it at every
// scrape, so it always reflects the current registry.
func (r *Router) cacheIsolationGapCounts() map[[2]string]int {
	counts := map[[2]string]int{}
	for _, a := range r.agents.List() {
		if len(r.cacheIsolationGaps(a)) > 0 {
			counts[[2]string{a.Metadata.Model, a.Metadata.Engine}]++
		}
	}
	return counts
}
