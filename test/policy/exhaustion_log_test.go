// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package policy_test contains black-box tests for the routing policy engine.
// All tests exercise only exported symbols so they remain valid regardless of
// internal refactors.
package policy_test

import (
	"context"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/storage"
	"hivenet_router/test/testutil"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

func init() {
	// Make WARN-level policy logs visible in test output.
	logging.SetLogLevel("policy", "warn")
}

// ── stubs ─────────────────────────────────────────────────────────────────────

// stubStorage is a no-op RoutingStorage (see testutil.NoopStorage). Inject
// engineMetrics / hwSnapshot to simulate specific agent readings for exclude_if
// gate tests; all other methods come from the embedded no-op base.
type stubStorage struct {
	testutil.NoopStorage
	engineMetrics map[peer.ID]*domain.BackendMetrics
	hwSnapshot    map[peer.ID]*domain.HardwareSnapshot
}

func (s *stubStorage) GetHardwareSnapshot(id peer.ID) (*domain.HardwareSnapshot, error) {
	if s.hwSnapshot != nil {
		return s.hwSnapshot[id], nil
	}
	return nil, nil
}

func (s *stubStorage) GetEnginePunctual(id peer.ID) (*domain.BackendMetrics, error) {
	if s.engineMetrics != nil {
		return s.engineMetrics[id], nil
	}
	return nil, nil
}

var _ storage.RoutingStorage = (*stubStorage)(nil)

// stubAgentLister returns a fixed agent pool keyed by model name.
type stubAgentLister struct {
	agents map[string][]*domain.Agent
}

func (l *stubAgentLister) ListByModel(model string) []*domain.Agent {
	return l.agents[model]
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeAgent(id string, meta domain.AgentMetadata) *domain.Agent {
	return domain.NewAgent(peer.ID(id), meta, "")
}

// newExec builds a minimal Executor suitable for unit tests.
func newExec(lister policy.AgentLister, stor storage.RoutingStorage, pol *policy.Policy) *policy.Executor {
	counters := metrics.NewUniversalCounterStore(stor, nil)
	evaluator := policy.NewEvaluator(stor, counters)
	return policy.NewExecutor(lister, evaluator, pol, 3, 0)
}

func f64(v float64) *float64 { return &v }

// ── exhaustion log tests ──────────────────────────────────────────────────────

// TestExhaustionLog_MatchFail verifies gate[4] match FAIL when agents have
// engine="" but the policy requires engine="vllm".
//
// Expected output (look for WARN lines):
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: gate[4] match [engine(got=\"\",want=\"vllm\")=2]"
func TestExhaustionLog_MatchFail(t *testing.T) {
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"Qwen-3.6-35B": {
			makeAgent("peer-1", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "", Capability: domain.CapabilityLLM}),
			makeAgent("peer-2", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "", Capability: domain.CapabilityLLM}),
		},
	}}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Match:    policy.MatchFilter{Engine: "vllm"},
			Strategy: "least-loaded",
			MaxTries: 2,
		},
	}

	exec := newExec(lister, &stubStorage{}, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-match-fail", "test-tenant")
}

// TestExhaustionLog_HealthFail verifies gate[2] health FAIL when all agents
// are marked unhealthy.
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: gate[2] health [unhealthy=2]"
func TestExhaustionLog_HealthFail(t *testing.T) {
	a1 := makeAgent("peer-1", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	a2 := makeAgent("peer-2", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	a1.SetHealthy(false)
	a2.SetHealthy(false)

	lister := &stubAgentLister{agents: map[string][]*domain.Agent{"Qwen-3.6-35B": {a1, a2}}}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Match:    policy.MatchFilter{Engine: "vllm"},
			Strategy: "least-loaded",
			MaxTries: 2,
		},
	}

	exec := newExec(lister, &stubStorage{}, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-health-fail", "test-tenant")
}

// TestExhaustionLog_ExcludeIfFail verifies gate[7] exclude_if FAIL when
// kv_cache_utilization exceeds the configured threshold.
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: gate[7] exclude_if [kv_cache_utilization=2]"
func TestExhaustionLog_ExcludeIfFail(t *testing.T) {
	p1, p2 := peer.ID("peer-1"), peer.ID("peer-2")
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"Qwen-3.6-35B": {
			makeAgent(string(p1), domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM}),
			makeAgent(string(p2), domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM}),
		},
	}}
	stor := &stubStorage{
		engineMetrics: map[peer.ID]*domain.BackendMetrics{
			p1: {KVCacheUtilization: f64(0.97)},
			p2: {KVCacheUtilization: f64(0.98)},
		},
	}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Match:     policy.MatchFilter{Engine: "vllm"},
			ExcludeIf: map[string]policy.ThresholdRule{"kv_cache_utilization": {GT: f64(0.95)}},
			Strategy:  "least-loaded",
			MaxTries:  2,
		},
	}

	exec := newExec(lister, stor, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-excludeif-fail", "test-tenant")
}

// TestExhaustionLog_FallbackChain verifies the full 3-step fallback chain,
// each step failing at a different gate.
//
//   - routing_policy: match engine="vllm" → gate[4] match FAIL (agents have engine="")
//   - relaxed:        match region="UAE"  → gate[4] match FAIL (agents have region="")
//   - last-resort:    exclude_if kv_cache → gate[7] exclude_if FAIL (kv_cache=0.97)
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy→relaxed→last-resort]  tried=3/3  reason="routing_policy: gate[4] match [engine(got=\"\",want=\"vllm\")=2] | relaxed: gate[4] match [region(got=\"\",want=\"UAE\")=2] | last-resort: gate[7] exclude_if [kv_cache_utilization=2]"
func TestExhaustionLog_FallbackChain(t *testing.T) {
	p1, p2 := peer.ID("peer-1"), peer.ID("peer-2")
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"Qwen-3.6-35B": {
			makeAgent(string(p1), domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "", Region: "", Capability: domain.CapabilityLLM}),
			makeAgent(string(p2), domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "", Region: "", Capability: domain.CapabilityLLM}),
		},
	}}
	stor := &stubStorage{
		engineMetrics: map[peer.ID]*domain.BackendMetrics{
			p1: {KVCacheUtilization: f64(0.97)},
			p2: {KVCacheUtilization: f64(0.98)},
		},
	}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Match:    policy.MatchFilter{Engine: "vllm"},
			Strategy: "least-loaded",
			MaxTries: 2,
		},
		FallbackChain: []policy.FallbackStep{
			{
				Name: "relaxed",
				PolicyStep: policy.PolicyStep{
					Match:    policy.MatchFilter{Region: "UAE"},
					Strategy: "least-loaded",
					MaxTries: 2,
				},
			},
			{
				Name: "last-resort",
				PolicyStep: policy.PolicyStep{
					Match:     policy.MatchFilter{},
					ExcludeIf: map[string]policy.ThresholdRule{"kv_cache_utilization": {GT: f64(0.95)}},
					Strategy:  "least-loaded",
					MaxTries:  1,
				},
			},
		},
	}

	exec := newExec(lister, stor, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-chain", "test-tenant")
}

// TestExhaustionLog_ModelNotFound verifies gate[1] model_filter FAIL when no
// agents are registered for the requested model.
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: gate[1] model_filter [model_not_registered]"
func TestExhaustionLog_ModelNotFound(t *testing.T) {
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{}} // no agents for any model
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Strategy: "least-loaded",
			MaxTries: 2,
		},
	}

	exec := newExec(lister, &stubStorage{}, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-model-not-found", "test-tenant")
}

// TestExhaustionLog_CapabilityMismatch verifies gate[3] capability FAIL when
// agents are registered for the model but serve a different capability type.
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: gate[3] capability [capability_mismatch=2]"
func TestExhaustionLog_CapabilityMismatch(t *testing.T) {
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"Qwen-3.6-35B": {
			makeAgent("peer-1", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityEmbedding}),
			makeAgent("peer-2", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityEmbedding}),
		},
	}}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Strategy: "least-loaded",
			MaxTries: 2,
		},
	}

	exec := newExec(lister, &stubStorage{}, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-capability-mismatch", "test-tenant")
}

// TestExhaustionLog_TagMismatch verifies gate[4] match FAIL via tag filter,
// exercising the tag(missing=...,got=[...]) format introduced when filterMissReason
// was updated to include the agent's actual tag list.
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: gate[4] match [tag(missing=\"ChainTrust\",got=[])=2]"
func TestExhaustionLog_TagMismatch(t *testing.T) {
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"Qwen-3.6-35B": {
			makeAgent("peer-1", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM}),
			makeAgent("peer-2", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM}),
		},
	}}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Match:    policy.MatchFilter{Tags: []string{"gpu-node"}},
			Strategy: "least-loaded",
			MaxTries: 2,
		},
	}

	exec := newExec(lister, &stubStorage{}, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)
	_, err := session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-tag-mismatch", "test-tenant")
}

// TestExhaustionLog_MaxTries verifies the max_tries trigger path: agents are
// healthy and pass all gates, but every forward attempt fails and the try budget
// is exhausted before a successful response is returned.
//
// Expected output:
//
//	policy_exhausted  chain=[routing_policy]  tried=1/1  reason="routing_policy: max_tries exhausted"
func TestExhaustionLog_MaxTries(t *testing.T) {
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"Qwen-3.6-35B": {
			makeAgent("peer-1", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM}),
			makeAgent("peer-2", domain.AgentMetadata{Model: "Qwen-3.6-35B", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM}),
		},
	}}
	pol := &policy.Policy{
		RoutingPolicy: policy.PolicyStep{
			Match:    policy.MatchFilter{Engine: "vllm"},
			Strategy: "least-loaded",
			MaxTries: 2,
		},
	}

	exec := newExec(lister, &stubStorage{}, pol)
	session := exec.NewSession("Qwen-3.6-35B", domain.CapabilityLLM)

	// Simulate two forward failures to exhaust the MaxTries=2 budget.
	a1, err := session.Select(context.Background())
	if err != nil {
		t.Fatalf("first select: expected agent, got %v", err)
	}
	session.RecordFailure(a1.ID)

	a2, err := session.Select(context.Background())
	if err != nil {
		t.Fatalf("second select: expected agent, got %v", err)
	}
	session.RecordFailure(a2.ID) // stepTries=2 >= MaxTries=2 → advanceStep("max_tries")

	_, err = session.Select(context.Background())
	if err == nil {
		t.Fatal("expected exhaustion error after max_tries, got nil")
	}
	session.EmitExhaustionLogs("chatcmpl-max-tries", "test-tenant")
}
