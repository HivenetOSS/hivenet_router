// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package policy_test — pure-logic tests for the three routing layers:
// MatchesFilter (layer 1), PassesGates/violates (layer 2), the least-loaded
// Strategy (layer 3), and policy validation/loading.
// Reuses helpers makeAgent and f64 defined in exhaustion_log_test.go.
package policy_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

// --- Layer 1: MatchesFilter ----------------------------------------------

// TestMatchesFilter is the eligibility gate: an agent must satisfy every set
// filter field (empty fields are wildcards; Tags require ALL listed tags).
func TestMatchesFilter(t *testing.T) {
	agent := makeAgent("a", domain.AgentMetadata{
		Region: "eu", Engine: "vllm", Organization: "acme", Machine: "gpu1",
		GPUModel: "RTX4090", Tags: []string{"prod", "fast"},
	})

	tests := []struct {
		name   string
		filter policy.MatchFilter
		want   bool
	}{
		{"empty matches all", policy.MatchFilter{}, true},
		{"region match", policy.MatchFilter{Region: "eu"}, true},
		{"region mismatch", policy.MatchFilter{Region: "us"}, false},
		{"engine mismatch", policy.MatchFilter{Engine: "ollama"}, false},
		{"org mismatch", policy.MatchFilter{Organization: "other"}, false},
		{"machine mismatch", policy.MatchFilter{Machine: "gpu2"}, false},
		{"gpu model match", policy.MatchFilter{GPUModel: "RTX4090"}, true},
		{"gpu model mismatch", policy.MatchFilter{GPUModel: "RTX5090"}, false},
		{"all tags present", policy.MatchFilter{Tags: []string{"prod", "fast"}}, true},
		{"missing tag", policy.MatchFilter{Tags: []string{"prod", "gpu"}}, false},
		{"combined all match", policy.MatchFilter{Region: "eu", Engine: "vllm", Tags: []string{"prod"}}, true},
		{"combined one fails", policy.MatchFilter{Region: "eu", Engine: "ollama"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := policy.MatchesFilter(agent, tt.filter); got != tt.want {
				t.Errorf("MatchesFilter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesFilter_CaseSensitive(t *testing.T) {
	agent := makeAgent("a", domain.AgentMetadata{Region: "EU"})
	if policy.MatchesFilter(agent, policy.MatchFilter{Region: "eu"}) {
		t.Error("region match must be case-sensitive")
	}
}

// --- Layer 2: PassesGates / violates -------------------------------------

// TestPassesGates covers exclude_if threshold rules: PassesGates returns true
// when the agent survives (no gate violated). Each operator (gt/lt/gte/lte) is
// checked against a single snapshot value of 0.9 to pin boundary behaviour.
func TestPassesGates(t *testing.T) {
	kv := 0.9
	snap := policy.AgentSnapshot{KVCacheUtilization: &kv}

	// gt: exclude when value > threshold.
	if policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {GT: f64(0.8)}}) {
		t.Error("0.9 > 0.8 must be excluded (gate fails)")
	}
	if !policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {GT: f64(0.95)}}) {
		t.Error("0.9 > 0.95 is false → passes")
	}
	// lt.
	if !policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {LT: f64(0.5)}}) {
		t.Error("0.9 < 0.5 is false → passes")
	}
	if policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {LT: f64(0.95)}}) {
		t.Error("0.9 < 0.95 → excluded")
	}
	// gte / lte boundary.
	if policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {GTE: f64(0.9)}}) {
		t.Error("0.9 >= 0.9 → excluded")
	}
	if policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {LTE: f64(0.9)}}) {
		t.Error("0.9 <= 0.9 → excluded")
	}
}

// A nil metric value silently passes the gate (gate skipped for that agent).
func TestPassesGates_NilMetricPasses(t *testing.T) {
	snap := policy.AgentSnapshot{} // KVCache is nil (e.g. non-vLLM agent)
	if !policy.PassesGates(snap, map[string]policy.ThresholdRule{"kv_cache_utilization": {GT: f64(0.1)}}) {
		t.Error("nil metric must pass the gate (skipped)")
	}
	// Unknown field also resolves to nil → skipped.
	if !policy.PassesGates(snap, map[string]policy.ThresholdRule{"nonexistent_field": {GT: f64(0.1)}}) {
		t.Error("unknown gate field must be skipped")
	}
}

// Multiple gates: any single violation excludes the agent.
func TestPassesGates_MultipleGates(t *testing.T) {
	kv, temp := 0.5, 90.0
	snap := policy.AgentSnapshot{KVCacheUtilization: &kv, GPUTemperatureC: &temp}
	gates := map[string]policy.ThresholdRule{
		"kv_cache_utilization": {GT: f64(0.8)}, // 0.5 ok
		"gpu_temperature_c":    {GT: f64(85)},  // 90 > 85 → excluded
	}
	if policy.PassesGates(snap, gates) {
		t.Error("one violated gate must exclude the agent")
	}
}

// --- Layer 3: least-loaded strategy --------------------------------------

// loaded builds a candidate whose live utilization is load/capacity by
// acquiring `load` concurrency slots, so the strategy can rank by real load.
func loaded(id string, capacity, load int) policy.ScoredCandidate {
	a := makeAgent(id, domain.AgentMetadata{Capacity: capacity})
	for i := 0; i < load; i++ {
		a.TryAcquireSlot()
	}
	return policy.ScoredCandidate{Agent: a}
}

func TestLeastLoadedStrategy(t *testing.T) {
	s := policy.Get("least-loaded")
	if s == nil {
		t.Fatal("least-loaded strategy must be registered")
	}
	// a: 8/10=0.8, b: 2/10=0.2 (winner), c: 5/10=0.5
	cands := []policy.ScoredCandidate{
		loaded("a", 10, 8),
		loaded("b", 10, 2),
		loaded("c", 10, 5),
	}
	got := s.Select(cands)
	if got == nil || got.ID.String() != cands[1].Agent.ID.String() {
		t.Errorf("expected least-loaded agent b to win, got %v", got)
	}
}

func TestStrategy_UnknownReturnsNil(t *testing.T) {
	if policy.Get("round-robin") != nil {
		t.Error("unimplemented strategy must return nil from Get")
	}
}

func TestLeastLoaded_EmptyPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Select with empty candidates must panic (executor contract violation)")
		}
	}()
	policy.Get("least-loaded").Select(nil)
}

// --- Validation / loading -------------------------------------------------

// TestDefaultPolicyValid guards that the shipped default policy always validates.
func TestDefaultPolicyValid(t *testing.T) {
	if err := policy.Validate(policy.Default()); err != nil {
		t.Errorf("Default() policy must validate, got %v", err)
	}
}

// TestValidate_Errors asserts each malformed policy is rejected with a specific,
// actionable message (unimplemented strategies are gated here, not at runtime).
func TestValidate_Errors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			"missing strategy",
			"routing_policy:\n  match: {}\n",
			"strategy is required",
		},
		{
			"unknown strategy",
			"routing_policy:\n  strategy: magic\n",
			"unknown strategy",
		},
		{
			"engine-specific not implemented",
			"routing_policy:\n  strategy: lowest-queue\n  match:\n    engine: vllm\n",
			"not yet implemented",
		},
		{
			"exclude_if unknown field",
			"routing_policy:\n  strategy: least-loaded\n  exclude_if:\n    bogus_field:\n      gt: 1\n",
			"unknown field",
		},
		{
			"exclude_if no operator",
			"routing_policy:\n  strategy: least-loaded\n  exclude_if:\n    srtt: {}\n",
			"exactly one operator",
		},
		{
			"exclude_if two operators",
			"routing_policy:\n  strategy: least-loaded\n  exclude_if:\n    srtt:\n      gt: 1\n      lt: 2\n",
			"only one operator",
		},
		{
			"fallback_provider missing engine",
			"routing_policy:\n  strategy: least-loaded\nfallback_provider:\n  model: gpt-4\n",
			"fallback_provider.engine is required",
		},
		{
			"fallback_provider missing model",
			"routing_policy:\n  strategy: least-loaded\nfallback_provider:\n  engine: openai\n",
			"fallback_provider.model is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := policy.LoadBytes([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestLoadBytes_Valid(t *testing.T) {
	y := `
routing_policy:
  match:
    region: eu
    engine: vllm
  exclude_if:
    kv_cache_utilization:
      gt: 0.9
    gpu_temperature_c:
      gte: 85
  strategy: least-loaded
  max_tries: 5
fallback_chain:
  - name: any-region
    strategy: least-loaded
fallback_provider:
  engine: openai
  model: gpt-4o
`
	p, err := policy.LoadBytes([]byte(y))
	if err != nil {
		t.Fatalf("valid policy failed to load: %v", err)
	}
	if p.RoutingPolicy.Strategy != "least-loaded" || p.RoutingPolicy.MaxTries != 5 {
		t.Errorf("routing policy parsed wrong: %+v", p.RoutingPolicy)
	}
	if p.RoutingPolicy.Match.Region != "eu" {
		t.Errorf("match not parsed: %+v", p.RoutingPolicy.Match)
	}
	if len(p.FallbackChain) != 1 || p.FallbackChain[0].Name != "any-region" {
		t.Errorf("fallback chain parsed wrong: %+v", p.FallbackChain)
	}
	if p.FallbackProvider == nil || p.FallbackProvider.Engine != "openai" {
		t.Errorf("fallback provider parsed wrong: %+v", p.FallbackProvider)
	}
	rule := p.RoutingPolicy.ExcludeIf["kv_cache_utilization"]
	if rule.GT == nil || *rule.GT != 0.9 {
		t.Errorf("exclude_if gt not parsed: %+v", rule)
	}
}

func TestLoadBytes_BadYAML(t *testing.T) {
	if _, err := policy.LoadBytes([]byte("routing_policy: [not-a-map")); err == nil {
		t.Error("malformed YAML must error")
	}
}
