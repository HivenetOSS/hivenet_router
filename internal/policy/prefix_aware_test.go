// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"context"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/prefixindex"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

type outcomes map[string]int

func (o outcomes) PrefixRouted(_, outcome string) { o[outcome]++ }

type agentList map[string][]*domain.Agent

func (l agentList) ListByModel(model string) []*domain.Agent { return l[model] }

func llm(id string) *domain.Agent {
	return domain.NewAgent(peer.ID(id), domain.AgentMetadata{Model: "m", Capability: domain.CapabilityLLM}, "")
}

func prefixExecutor(ix PrefixIndex, rec PrefixMetrics, agents ...*domain.Agent) *Executor {
	stor := testutil.NoopStorage{}
	e := NewExecutor(agentList{"m": agents}, NewEvaluator(stor, metrics.NewUniversalCounterStore(stor, nil)),
		&Policy{RoutingPolicy: PolicyStep{Strategy: "prefix-aware", MaxTries: 3}}, 3, 0)
	e.SetPrefixIndex(ix)
	e.SetPrefixMetrics(rec)
	return e
}

// route selects for anchors (with sizes ~1k tokens each) and releases the slot.
func route(t *testing.T, e *Executor, anchors []uint64) *domain.Agent {
	t.Helper()
	s := e.NewSession("m", domain.CapabilityLLM)
	sizes := make([]int, len(anchors))
	for i := range sizes {
		sizes[i] = 1000
	}
	s.SetPrefix(&PrefixRequest{Anchors: anchors, Sizes: sizes})
	a, err := s.Select(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.ReleasePrefill()
	a.DecrementLoad()
	return a
}

func blocks(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i + 1)
	}
	return out
}

// The agent that holds the prompt's blocks wins over an idle one.
func TestPrefixAware_HitWins(t *testing.T) {
	ix, rec := prefixindex.New(prefixindex.Config{}), outcomes{}
	a, b := llm("a"), llm("b")
	e := prefixExecutor(ix, rec, a, b)
	ix.Learn("m", b.ID, []uint64{100, 101}, []int{20, 31}, blocks(31), time.Minute)
	for range 5 {
		if got := route(t, e, []uint64{100, 101, 102}); got != b {
			t.Fatalf("chose %s, want b (holds the prompt)", got.ID)
		}
	}
	if rec[PrefixOutcomeHit] != 5 {
		t.Errorf("outcomes %v", rec)
	}
}

// Without knowledge, two routers (two executors) break ties the same way,
// and different texts spread over the agents.
func TestPrefixAware_TieIsTheSameOnEveryRouter(t *testing.T) {
	agents := []*domain.Agent{llm("a"), llm("b"), llm("c"), llm("d")}
	r1 := prefixExecutor(prefixindex.New(prefixindex.Config{}), nil, agents...)
	r2 := prefixExecutor(prefixindex.New(prefixindex.Config{}), nil, agents...)
	used := map[peer.ID]bool{}
	for i := range 100 {
		anchors := []uint64{uint64(1000 + i)}
		x, y := route(t, r1, anchors), route(t, r2, anchors)
		if x != y {
			t.Fatalf("text %d: router 1 chose %s, router 2 %s", i, x.ID, y.ID)
		}
		used[x.ID] = true
	}
	if len(used) < 3 {
		t.Errorf("100 new texts reached only %d agents", len(used))
	}
}

// New text sent to an agent leaves a pending note: a copy that arrives before
// the agent reports goes to the same agent.
func TestPrefixAware_PendingKeepsCopiesTogether(t *testing.T) {
	ix, rec := prefixindex.New(prefixindex.Config{}), outcomes{}
	e := prefixExecutor(ix, rec, llm("a"), llm("b"), llm("c"))
	anchors := []uint64{7, 8}
	first := route(t, e, anchors)
	// Load the chosen agent a little: the pending credit must still win.
	first.TryAcquireSlot()
	defer first.DecrementLoad()
	if got := route(t, e, anchors); got != first {
		t.Errorf("copy went to %s, first to %s", got.ID, first.ID)
	}
	if rec[PrefixOutcomePending] != 1 {
		t.Errorf("outcomes %v", rec)
	}
}

// The prefill queue counts: an agent holding a small prefix behind a long
// queue loses to an idle one.
func TestPrefixAware_PrefillQueue(t *testing.T) {
	ix := prefixindex.New(prefixindex.Config{})
	a, b := llm("a"), llm("b")
	e := prefixExecutor(ix, nil, a, b)
	ix.Learn("m", a.ID, []uint64{100}, []int{2}, blocks(2), time.Minute) // 128 tokens cached on a
	e.prefill.add(a.ID, 50_000)                                          // 5 s of prefill queued on a
	if got := route(t, e, []uint64{100, 101}); got != b {
		t.Errorf("chose %s, want b: a's queue costs more than its cache saves", got.ID)
	}
}

// Busy agents are excluded whatever they hold; all busy -> least-loaded.
func TestPrefixAware_Busy(t *testing.T) {
	ix, rec := prefixindex.New(prefixindex.Config{}), outcomes{}
	s := (&Executor{prefixIndex: ix, prefixMetrics: rec}).newPrefixAware(nil)
	a, b := llm("a"), llm("b")
	ix.Learn("m", a.ID, []uint64{100}, []int{10}, blocks(10), time.Minute)
	full, fine := 0.95, 0.1
	cands := []ScoredCandidate{{Agent: a, Snapshot: AgentSnapshot{KVCacheUtilization: &full}}, {Agent: b, Snapshot: AgentSnapshot{KVCacheUtilization: &fine}}}
	req := SelectRequest{Model: "m", Prefix: &PrefixRequest{Anchors: []uint64{100}, Sizes: []int{640}}}
	if got := s.Select(cands, req); got != b {
		t.Errorf("chose %s, want b (a's KV cache is full)", got.ID)
	}
	cands[1].Snapshot.KVCacheUtilization = &full
	s.Select(cands, req)
	if got := s.Select(cands, SelectRequest{Model: "m"}); got == nil {
		t.Fatal("no anchors: want least-loaded")
	}
	if rec[PrefixOutcomeBusy] != 1 || rec[PrefixOutcomeNoAnchor] != 1 {
		t.Errorf("outcomes %v", rec)
	}
}

// The session asks for anchors only when a step routes on them.
func TestSession_WantsPrefix(t *testing.T) {
	stor := testutil.NoopStorage{}
	ev := NewEvaluator(stor, metrics.NewUniversalCounterStore(stor, nil))
	plain := NewExecutor(agentList{}, ev, &Policy{RoutingPolicy: PolicyStep{Strategy: "least-loaded"}}, 3, 0)
	if plain.NewSession("m", domain.CapabilityLLM).WantsPrefix() {
		t.Error("least-loaded policy must not want anchors")
	}
	chain := NewExecutor(agentList{}, ev, &Policy{RoutingPolicy: PolicyStep{Strategy: "least-loaded"},
		FallbackChain: []FallbackStep{{PolicyStep: PolicyStep{Strategy: "prefix-aware"}}}}, 3, 0)
	if !chain.NewSession("m", domain.CapabilityLLM).WantsPrefix() {
		t.Error("a prefix-aware fallback step must want anchors")
	}
}

func TestPrefixAware_Validation(t *testing.T) {
	ok := "routing_policy:\n  strategy: prefix-aware\n  prefix_aware:\n    kv_threshold: 0.9\n    max_waiting: 4\n    busy_factor: 3\n    tie_epsilon: 0.1\n    prefill_rate: 20000\n    predicted_ttl: 60s\n    pending_ttl: 2s\n"
	p, err := LoadBytes([]byte(ok))
	if err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if pa := p.RoutingPolicy.PrefixAware; pa == nil || pa.PrefillRate != 20000 || pa.PredictedTTL != time.Minute {
		t.Errorf("parsed %+v", pa)
	}
	for _, tc := range []struct{ yaml, want string }{
		{"strategy: least-loaded\n  prefix_aware:\n    max_waiting: 2", "only valid with strategy: prefix-aware"},
		{"strategy: prefix-aware\n  prefix_aware:\n    kv_threshold: 1.5", "kv_threshold"},
		{"strategy: prefix-aware\n  prefix_aware:\n    busy_factor: 0.5", "busy_factor"},
		{"strategy: prefix-aware\n  prefix_aware:\n    prefill_rate: 10", "prefill_rate"},
		{"strategy: prefix-aware\n  prefix_aware:\n    pending_ttl: 5m", "pending_ttl"},
	} {
		_, err := LoadBytes([]byte("routing_policy:\n  " + tc.yaml + "\n"))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: error %v, want one about %s", tc.yaml, err, tc.want)
		}
	}
}

// Requests that already finished do not make an agent busy: a burst of short
// requests raises GetLoad until the agent's next report, not InFlight.
func TestPrefixAware_FinishedRequestsAreNotBusy(t *testing.T) {
	ix := prefixindex.New(prefixindex.Config{})
	home, other := llm("home"), llm("other")
	home.SetReportedLoad(0)
	for range 20 {
		home.TryAcquireSlot()
		home.DecrementLoad()
	}
	if home.GetLoad() <= busyFloor || home.InFlight() != 0 {
		t.Fatalf("setup: GetLoad %d, InFlight %d", home.GetLoad(), home.InFlight())
	}
	ix.Learn("m", home.ID, []uint64{100}, []int{10}, blocks(10), time.Minute)
	s := (&Executor{prefixIndex: ix}).newPrefixAware(nil)
	req := SelectRequest{Model: "m", Prefix: &PrefixRequest{Anchors: []uint64{100, 101}, Sizes: []int{640, 50}}}
	if got := s.Select([]ScoredCandidate{{Agent: home}, {Agent: other}}, req); got != home {
		t.Errorf("chose %s: an idle agent that holds the prompt was treated as busy", got.ID)
	}
}
