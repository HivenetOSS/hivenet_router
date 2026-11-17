// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"math"
	"testing"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
)

func agentGPU(id, gpu string) *domain.Agent {
	return domain.NewAgent(peer.ID(id), domain.AgentMetadata{Model: "m", Engine: "vllm", GPUModel: gpu, Capability: domain.CapabilityLLM}, "")
}

func reported(a *domain.Agent, rate, prefillN, step, decodeN float64) ScoredCandidate { // prefillN: prefill seconds
	return reportedS(a, rate, prefillN, step, decodeN)
}

func reportedS(a *domain.Agent, rate, prefillN, step, decodeN float64) ScoredCandidate {
	s := AgentSnapshot{PrefillSeconds: prefillN, DecodeSamples: decodeN}
	if rate > 0 {
		s.PrefillTokensPerSecond = &rate
	}
	if step > 0 {
		s.DecodeStepSeconds = &step
	}
	return ScoredCandidate{Agent: a, Snapshot: s}
}

// The replicas of a group are pooled, weighted by what each report covers
// (prefill seconds, decode tokens): total tokens over total seconds;
// a replica with no report (idle, or avoided) takes the group's figures.
func TestLatencyModel_PoolsTheGroup(t *testing.T) {
	m := &latencyModel{}
	a, b, idle := agentGPU("a", "RTX5090"), agentGPU("b", "RTX5090"), agentGPU("idle", "RTX5090")
	figs := m.learn([]ScoredCandidate{reported(a, 20000, 30, 0.010, 900), reported(b, 10000, 10, 0.030, 100), reported(idle, 0, 0, 0, 0)})
	f := figs[groupOf(idle)]
	if math.Abs(f.prefillRate-17500) > 1e-6 || math.Abs(f.decodeStep-0.012) > 1e-9 {
		t.Errorf("pooled %+v, want 17500 tok/s and 12 ms (weighted)", f)
	}
}

// A group nobody reports for now keeps its last figures: they describe the
// hardware, not a moment.
func TestLatencyModel_KeepsLastFigures(t *testing.T) {
	m := &latencyModel{}
	a := agentGPU("a", "RTX5090")
	m.learn([]ScoredCandidate{reported(a, 15000, 5, 0.02, 50)})
	f := m.learn([]ScoredCandidate{reported(a, 0, 0, 0, 0)})[groupOf(a)]
	if f.prefillRate != 15000 || f.decodeStep != 0.02 {
		t.Errorf("idle group: %+v, want the last figures 15000 / 0.02", f)
	}
}

// Different GPUs (or engines) behind one model are separate groups.
func TestLatencyModel_GroupsBySpeedClass(t *testing.T) {
	m := &latencyModel{}
	fast, slow := agentGPU("fast", "RTXPRO6000"), agentGPU("slow", "RTX5090")
	figs := m.learn([]ScoredCandidate{reported(fast, 40000, 10, 0.01, 10), reported(slow, 10000, 10, 0.02, 10)})
	if figs[groupOf(fast)].prefillRate != 40000 || figs[groupOf(slow)].prefillRate != 10000 {
		t.Errorf("groups mixed: %+v", figs)
	}
}

func TestLatencyModel_Bounds(t *testing.T) {
	m := &latencyModel{}
	a := agentGPU("a", "")
	f := m.learn([]ScoredCandidate{reported(a, 1e12, 1, 10, 1)})[groupOf(a)]
	if f.prefillRate != maxLearnedPrefillRate || f.decodeStep != maxLearnedDecodeStep {
		t.Errorf("unbounded: %+v", f)
	}
}

// The prefix-aware cost uses the learned figures; a policy's prefill_rate
// wins; and an agent's own (old) average never sets its decode cost.
func TestPrefixAware_LearnedSpeeds(t *testing.T) {
	a := agentGPU("a", "RTX5090")
	figs := map[latencyGroup]latencyFigures{groupOf(a): {prefillRate: 25000, decodeStep: 0.015}}
	learned := prefixAwareStrategy{PrefixAwareParams: (*PrefixAwareParams)(nil).resolved()}
	if r, s := learned.speeds(figs, a); r != 25000 || s != 0.015 {
		t.Errorf("learned: %v %v", r, s)
	}
	set := prefixAwareStrategy{PrefixAwareParams: (&PrefixAwareParams{PrefillRate: 5000}).resolved(), prefillRateSet: true}
	if r, _ := set.speeds(figs, a); r != 5000 {
		t.Errorf("policy prefill_rate: %v, want 5000", r)
	}
	if r, s := learned.speeds(nil, a); r != DefaultPrefixPrefillRate || s != defaultITLSeconds {
		t.Errorf("nothing learned: %v %v, want the defaults", r, s)
	}
	running, oldAvg := 10.0, 0.5
	c := ScoredCandidate{Agent: a, Snapshot: AgentSnapshot{RunningRequests: &running, AvgITLSeconds: &oldAvg}}
	if got := decodePressure(c, 0.015); math.Abs(got-0.15) > 1e-9 {
		t.Errorf("decode pressure %v, want 10 × 15 ms (the group's step, not the agent's 0.5 s)", got)
	}
}
