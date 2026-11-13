// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

var prefixPolicy = &policy.Policy{RoutingPolicy: policy.PolicyStep{Strategy: "prefix-aware", MaxTries: 3}}

func llmAgents(n int) []*domain.Agent {
	out := make([]*domain.Agent, n)
	for i := range out {
		out[i] = makeAgent(fmt.Sprintf("agent-%02d", i), domain.AgentMetadata{Model: "m", Capability: domain.CapabilityLLM})
	}
	return out
}

func routingKey(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

// route selects for key and releases the slot at once (an idle fleet).
func route(t *testing.T, exec *policy.Executor, key []byte) string {
	t.Helper()
	s := exec.NewSession("m", domain.CapabilityLLM)
	s.SetKey(key)
	a, err := s.Select(context.Background())
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	a.DecrementLoad()
	return a.ID.String()
}

// Two routers that serve the same agents pick the same agent for a key,
// whatever order their registries list the agents in.
func TestPrefixAware_RoutersAgree(t *testing.T) {
	agents := llmAgents(5)
	reversed := slices.Clone(agents)
	slices.Reverse(reversed)
	r1 := newExec(&stubAgentLister{agents: map[string][]*domain.Agent{"m": agents}}, &stubStorage{}, prefixPolicy)
	r2 := newExec(&stubAgentLister{agents: map[string][]*domain.Agent{"m": reversed}}, &stubStorage{}, prefixPolicy)

	used := map[string]bool{}
	for i := range 200 {
		k := routingKey(fmt.Sprint("conversation ", i))
		a, b := route(t, r1, k), route(t, r2, k)
		if a != b {
			t.Fatalf("key %d: router 1 picked %s, router 2 picked %s", i, a, b)
		}
		used[a] = true
	}
	if len(used) != len(agents) {
		t.Errorf("200 keys reached %d of %d agents", len(used), len(agents))
	}
}

// When an agent leaves, only its keys move; the others keep their agent.
func TestPrefixAware_AgentLeavingMovesOnlyItsKeys(t *testing.T) {
	agents := llmAgents(5)
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{"m": agents}}
	exec := newExec(lister, &stubStorage{}, prefixPolicy)

	const keys = 2000
	before := make([]string, keys)
	for i := range keys {
		before[i] = route(t, exec, routingKey(fmt.Sprint("k", i)))
	}
	gone := agents[2].ID.String()
	lister.agents["m"] = slices.Delete(slices.Clone(agents), 2, 3)

	moved := 0
	for i := range keys {
		after := route(t, exec, routingKey(fmt.Sprint("k", i)))
		if before[i] == gone {
			moved++
			continue
		}
		if after != before[i] {
			t.Fatalf("key %d moved from %s to %s although its agent stayed", i, before[i], after)
		}
	}
	if share := float64(moved) / keys; share < 0.1 || share > 0.3 {
		t.Errorf("the leaving agent held %.0f%% of the keys, want about 20%%", share*100)
	}
}

// A hot key does not pile onto one agent: each agent stays within the bound,
// and the key's first agent gets the largest share.
func TestPrefixAware_HotKeySpillsWithinTheBound(t *testing.T) {
	agents := llmAgents(4)
	exec := newExec(&stubAgentLister{agents: map[string][]*domain.Agent{"m": agents}}, &stubStorage{}, prefixPolicy)
	key := routingKey("one very popular system prompt")
	home := route(t, exec, key)

	const inFlight = 40
	for range inFlight {
		s := exec.NewSession("m", domain.CapabilityLLM)
		s.SetKey(key)
		if _, err := s.Select(context.Background()); err != nil { // slots kept: requests in flight
			t.Fatal(err)
		}
	}
	bound := int(math.Ceil(policy.DefaultPrefixLoadFactor * inFlight / float64(len(agents))))
	maxLoad, homeLoad := 0, 0
	for _, a := range agents {
		maxLoad = max(maxLoad, a.GetLoad())
		if a.ID.String() == home {
			homeLoad = a.GetLoad()
		}
	}
	if maxLoad > bound {
		t.Errorf("an agent holds %d requests, bound %d", maxLoad, bound)
	}
	if homeLoad != maxLoad {
		t.Errorf("home agent holds %d, another holds %d", homeLoad, maxLoad)
	}
}

// After a failed forward, the retry takes the next agent on the ring: the
// same one on every router.
func TestPrefixAware_RetryTakesTheNextAgent(t *testing.T) {
	agents := llmAgents(4)
	exec := newExec(&stubAgentLister{agents: map[string][]*domain.Agent{"m": agents}}, &stubStorage{}, prefixPolicy)
	key := routingKey("retry me")

	order := func() []string {
		s := exec.NewSession("m", domain.CapabilityLLM)
		s.SetKey(key)
		var ids []string
		for range 3 {
			a, err := s.Select(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			a.DecrementLoad()
			s.RecordFailure(a.ID)
			ids = append(ids, a.ID.String())
		}
		return ids
	}
	first, second := order(), order()
	if !slices.Equal(first, second) {
		t.Errorf("retry order differs: %v then %v", first, second)
	}
	if len(slices.Compact(slices.Sorted(slices.Values(first)))) != 3 {
		t.Errorf("retries reused an agent: %v", first)
	}
}

func TestPrefixAware_Validation(t *testing.T) {
	ok := "routing_policy:\n  strategy: prefix-aware\n  prefix_aware:\n    load_factor: 2\n    min_load: 8\n    virtual_nodes: 150\n    kv_threshold: 0.85\n    prefix_bytes: 8192\n"
	p, err := policy.LoadBytes([]byte(ok))
	if err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if pa := p.RoutingPolicy.PrefixAware; pa == nil || pa.LoadFactor != 2 || pa.MinLoad != 8 || pa.VirtualNodes != 150 || pa.KVThreshold != 0.85 || pa.PrefixBytes != 8192 {
		t.Errorf("prefix_aware parsed as %+v", pa)
	}
	if _, err := policy.LoadBytes([]byte("routing_policy:\n  strategy: prefix-aware\n")); err != nil {
		t.Errorf("prefix-aware without a block rejected: %v", err)
	}

	for _, tc := range []struct{ yaml, want string }{
		{"strategy: least-loaded\n  prefix_aware:\n    load_factor: 2", "only valid with strategy: prefix-aware"},
		{"strategy: prefix-aware\n  prefix_aware:\n    load_factor: 0.5", "load_factor"},
		{"strategy: prefix-aware\n  prefix_aware:\n    min_load: -1", "min_load"},
		{"strategy: prefix-aware\n  prefix_aware:\n    virtual_nodes: 5", "virtual_nodes"},
		{"strategy: prefix-aware\n  prefix_aware:\n    kv_threshold: 1.5", "kv_threshold"},
		{"strategy: prefix-aware\n  prefix_aware:\n    prefix_bytes: 100", "prefix_bytes"},
	} {
		_, err := policy.LoadBytes([]byte("routing_policy:\n  " + tc.yaml + "\n"))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: error %v, want one about %s", tc.yaml, err, tc.want)
		}
	}
}
