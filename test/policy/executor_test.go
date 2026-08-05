// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy_test

import (
	"context"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// newEvaluatorFor wires an Evaluator over a stub storage; nil metrics are fine
// since Snapshot only reads engine/hardware metrics, it does not emit.
func newEvaluatorFor(stor storage.RoutingStorage) *policy.Evaluator {
	return policy.NewEvaluator(stor, metrics.NewUniversalCounterStore(stor, nil))
}

// TestExecutor_SelectHappyPath covers the success path (all existing executor
// tests exercise exhaustion/errors): a healthy matching agent is returned.
func TestExecutor_SelectHappyPath(t *testing.T) {
	agent := makeAgent("peer-1", domain.AgentMetadata{
		Model: "m", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM,
	})
	// Single vLLM agent for model "m"; policy matches engine=vllm then least-loaded.
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{"m": {agent}}}
	pol := &policy.Policy{RoutingPolicy: policy.PolicyStep{
		Match: policy.MatchFilter{Engine: "vllm"}, Strategy: "least-loaded", MaxTries: 3,
	}}

	// Full pipeline (match → gates → strategy → acquire) must return the one agent.
	exec := newExec(lister, &stubStorage{}, pol)
	got, err := exec.NewSession("m", domain.CapabilityLLM).Select(context.Background())
	if err != nil {
		t.Fatalf("Select returned error on happy path: %v", err)
	}
	if got == nil || got.ID.String() != agent.ID.String() {
		t.Errorf("Select returned wrong agent: %v", got)
	}
}

// TestExecutor_SelectPicksLeastLoaded verifies the strategy is applied across the
// eligible pool.
func TestExecutor_SelectPicksLeastLoaded(t *testing.T) {
	busy := makeAgent("busy", domain.AgentMetadata{Model: "m", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	idle := makeAgent("idle", domain.AgentMetadata{Model: "m", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	for i := 0; i < 8; i++ {
		busy.TryAcquireSlot() // load 0.8 vs idle 0.0
	}
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{"m": {busy, idle}}}
	pol := &policy.Policy{RoutingPolicy: policy.PolicyStep{
		Match: policy.MatchFilter{Engine: "vllm"}, Strategy: "least-loaded", MaxTries: 3,
	}}

	got, err := newExec(lister, &stubStorage{}, pol).NewSession("m", domain.CapabilityLLM).Select(context.Background())
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	if got.ID.String() != idle.ID.String() {
		t.Errorf("expected least-loaded 'idle' agent, got %q", got.ID.String())
	}
}

// TestExecutor_RecordFailureAdvances drives Select→RecordFailure repeatedly until
// the step is exhausted, covering RecordFailure/advanceStep on the success-first path.
func TestExecutor_RecordFailureExhausts(t *testing.T) {
	agent := makeAgent("peer-1", domain.AgentMetadata{Model: "m", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{"m": {agent}}}
	pol := &policy.Policy{RoutingPolicy: policy.PolicyStep{
		Match: policy.MatchFilter{Engine: "vllm"}, Strategy: "least-loaded", MaxTries: 2,
	}}
	sess := newExec(lister, &stubStorage{}, pol).NewSession("m", domain.CapabilityLLM)

	// First select succeeds, then we report failure and retry until exhausted.
	// MaxTries=2 with only one agent → the 5-iteration loop must hit exhaustion
	// (error) before completing; the agent is retried until the budget runs out.
	var lastErr error
	for i := 0; i < 5; i++ {
		a, err := sess.Select(context.Background())
		if err != nil {
			lastErr = err
			break
		}
		sess.RecordFailure(a.ID)
	}
	if lastErr == nil {
		t.Error("expected the session to exhaust after max_tries failures")
	}
}

// TestEvaluator_Snapshot verifies the per-agent snapshot assembly: live capacity
// utilization, engine metrics passthrough, and 0–100 → 0.0–1.0 hardware normalisation.
func TestEvaluator_Snapshot(t *testing.T) {
	pid := "snap-peer"
	agent := makeAgent(pid, domain.AgentMetadata{Model: "m", Capacity: 4, Engine: "vllm"})
	agent.TryAcquireSlot()
	agent.TryAcquireSlot() // load 2/4 = 0.5

	// Stub feeds raw engine metrics (already fractions) and hardware in native
	// units (percent 0–100, VRAM bytes) so Snapshot's normalisation can be checked.
	stor := &stubStorage{
		engineMetrics: map[peer.ID]*domain.BackendMetrics{
			peer.ID(pid): {KVCacheUtilization: f64(0.6), WaitingRequests: f64(3)},
		},
		hwSnapshot: map[peer.ID]*domain.HardwareSnapshot{
			peer.ID(pid): {
				CPU:    domain.CPUMetric{UsagePercent: 50},
				Memory: domain.MemoryMetric{UsedPercent: 80},
				GPUs: []domain.GPUMetric{
					{TemperatureC: 70, UtilPercent: 90, VRAMUsedBytes: 5, VRAMTotalBytes: 10},
				},
			},
		},
	}

	ev := newEvaluatorFor(stor)
	snap := ev.Snapshot(peer.ID(pid), agent)

	if snap.CapacityUtilization == nil || *snap.CapacityUtilization != 0.5 {
		t.Errorf("CapacityUtilization = %v, want 0.5", snap.CapacityUtilization)
	}
	if snap.KVCacheUtilization == nil || *snap.KVCacheUtilization != 0.6 {
		t.Errorf("KVCacheUtilization = %v, want 0.6", snap.KVCacheUtilization)
	}
	if snap.WaitingRequests == nil || *snap.WaitingRequests != 3 {
		t.Errorf("WaitingRequests = %v, want 3", snap.WaitingRequests)
	}
	// Hardware normalised to fractions.
	if snap.CPUUsagePercent == nil || *snap.CPUUsagePercent != 0.5 {
		t.Errorf("CPUUsagePercent = %v, want 0.5 (50/100)", snap.CPUUsagePercent)
	}
	if snap.MemoryUsedPercent == nil || *snap.MemoryUsedPercent != 0.8 {
		t.Errorf("MemoryUsedPercent = %v, want 0.8", snap.MemoryUsedPercent)
	}
	if snap.GPUUtilPercent == nil || *snap.GPUUtilPercent != 0.9 {
		t.Errorf("GPUUtilPercent = %v, want 0.9", snap.GPUUtilPercent)
	}
	if snap.GPUVRAMUsedPercent == nil || *snap.GPUVRAMUsedPercent != 0.5 {
		t.Errorf("GPUVRAMUsedPercent = %v, want 0.5 (5/10)", snap.GPUVRAMUsedPercent)
	}
	if snap.GPUTemperatureC == nil || *snap.GPUTemperatureC != 70 {
		t.Errorf("GPUTemperatureC = %v, want 70 (absolute, not normalised)", snap.GPUTemperatureC)
	}
}
