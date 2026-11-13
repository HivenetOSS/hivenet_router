// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"crypto/sha256"
	"testing"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
)

type outcomeRecorder map[string]int

func (o outcomeRecorder) PrefixRouted(_, outcome string) { o[outcome]++ }

func candidatesOf(agents ...*domain.Agent) []ScoredCandidate {
	out := make([]ScoredCandidate, len(agents))
	for i, a := range agents {
		out[i] = ScoredCandidate{Agent: a}
	}
	return out
}

func f64p(v float64) *float64 { return &v }

func newPrefixStrategy(rec outcomeRecorder) prefixAwareStrategy {
	e := &Executor{prefixMetrics: rec}
	return e.newPrefixAware(nil)
}

// The key's first agent wins while it is within the bound and its KV cache is
// below kv_threshold; a full KV cache sends the request to the next agent.
func TestPrefixAware_KVThreshold(t *testing.T) {
	rec := outcomeRecorder{}
	s := newPrefixStrategy(rec)
	agents := []*domain.Agent{llmAgent("a"), llmAgent("b"), llmAgent("c")}
	key := sha256.Sum256([]byte("k"))
	req := SelectRequest{Key: key[:], Model: "m", Pool: agents}

	cands := candidatesOf(agents...)
	home := s.Select(cands, req)
	if rec[PrefixOutcomeHome] != 1 {
		t.Fatalf("outcomes %v, want one home", rec)
	}

	for i := range cands {
		if cands[i].Agent == home {
			cands[i].Snapshot.KVCacheUtilization = f64p(DefaultPrefixKVThreshold)
		} else {
			cands[i].Snapshot.KVCacheUtilization = f64p(0.1)
		}
	}
	if got := s.Select(cands, req); got == home {
		t.Error("an agent with a full KV cache was chosen")
	}
	if rec[PrefixOutcomeSpill] != 1 {
		t.Errorf("outcomes %v, want one spill", rec)
	}
}

// Below min_load an agent is never too busy: a few requests in flight (or
// just finished, still in the agent's report) keep the key on its agent.
// With min_load 1 the bound is CHWBL's alone.
func TestPrefixAware_MinLoad(t *testing.T) {
	agents := []*domain.Agent{llmAgent("a"), llmAgent("b"), llmAgent("c")}
	key := sha256.Sum256([]byte("k"))
	req := SelectRequest{Key: key[:], Model: "m", Pool: agents}
	e := &Executor{}
	home := e.newPrefixAware(nil).Select(candidatesOf(agents...), req)
	for range DefaultPrefixMinLoad - 1 {
		home.TryAcquireSlot()
	}
	if got := e.newPrefixAware(nil).Select(candidatesOf(agents...), req); got != home {
		t.Errorf("home with %d in flight left for %s", home.GetLoad(), got.ID)
	}
	if got := e.newPrefixAware(&PrefixAwareParams{MinLoad: 1}).Select(candidatesOf(agents...), req); got == home {
		t.Error("min_load 1: home over the CHWBL bound was still chosen")
	}
}

// Every candidate over the bound or full → least-loaded; no key → least-loaded.
func TestPrefixAware_FallbackAndNoKey(t *testing.T) {
	rec := outcomeRecorder{}
	s := newPrefixStrategy(rec)
	busy, idle := llmAgent("busy"), llmAgent("idle")
	for range 3 {
		busy.TryAcquireSlot()
	}
	cands := candidatesOf(busy, idle)
	for i := range cands {
		cands[i].Snapshot.KVCacheUtilization = f64p(0.95)
	}
	key := sha256.Sum256([]byte("k"))
	if got := s.Select(cands, SelectRequest{Key: key[:], Model: "m"}); got != idle {
		t.Errorf("fallback chose %s, want the least-loaded agent", got.ID)
	}
	if got := s.Select(candidatesOf(busy, idle), SelectRequest{Model: "m"}); got != idle {
		t.Errorf("no key chose %s, want the least-loaded agent", got.ID)
	}
	if rec[PrefixOutcomeFallback] != 1 || rec[PrefixOutcomeNoKey] != 1 {
		t.Errorf("outcomes %v, want one fallback and one no_key", rec)
	}
}

// The ring is rebuilt only when the agent set changes.
func TestRingCache_RebuildsOnlyWhenTheSetChanges(t *testing.T) {
	var c ringCache
	a, b := llmAgent("a"), llmAgent("b")
	r1 := c.get("m", 10, []*domain.Agent{a, b})
	if r2 := c.get("m", 10, []*domain.Agent{b, a}); r2 != r1 {
		t.Error("same set in another order rebuilt the ring")
	}
	r3 := c.get("m", 10, []*domain.Agent{a, b, llmAgent("c")})
	if r3 == r1 || r3.agents != 3 || len(r3.points) != 30 {
		t.Errorf("new agent: ring %p (old %p), %d agents, %d points", r3, r1, r3.agents, len(r3.points))
	}
	var visited []peer.ID
	r3.walk(0, func(id peer.ID) bool { visited = append(visited, id); return false })
	if len(visited) != 3 {
		t.Errorf("walk visited %v, want each of the 3 agents once", visited)
	}
}
