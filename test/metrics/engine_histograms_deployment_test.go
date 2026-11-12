// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"math"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
)

// histogramFamily returns the gathered metric family by name, or nil.
func histogramFamily(t *testing.T, m *metrics.RouterMetrics, name string) *dto.MetricFamily {
	t.Helper()
	families, err := m.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// labelValue returns the value of label name on metric, or "" with ok=false.
func labelValue(metric *dto.Metric, name string) (string, bool) {
	for _, l := range metric.GetLabel() {
		if l.GetName() == name {
			return l.GetValue(), true
		}
	}
	return "", false
}

func snapshot(sum float64, count uint64) *domain.HistogramSnapshot {
	return &domain.HistogramSnapshot{
		Sum:   sum,
		Count: count,
		Buckets: []domain.HistogramBucket{
			{Le: 0.5, Count: count / 2},
			{Le: 1, Count: count},
			{Le: math.Inf(1), Count: count},
		},
	}
}

// TestEngineHistograms_CarryDeploymentID pins that every engine histogram is
// labelled with the agent's deployment_id, so a consumer can read one
// deployment's engine latency (e.g. its median TTFT) by selecting on it.
func TestEngineHistograms_CarryDeploymentID(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		TTFTHistogram:             snapshot(3, 10),
		ITLHistogram:              snapshot(0.4, 20),
		PromptTokensHistogram:     snapshot(500, 10),
		GenerationTokensHistogram: snapshot(800, 10),
		DecodeTimeHistogram:       snapshot(16, 10),
	})

	for _, name := range []string{
		"hivenet_agent_engine_ttft_seconds",
		"hivenet_agent_engine_itl_seconds",
		"hivenet_agent_engine_request_prompt_tokens",
		"hivenet_agent_engine_request_generation_tokens",
		"hivenet_agent_engine_request_decode_time_seconds",
	} {
		f := histogramFamily(t, m, name)
		if f == nil {
			t.Fatalf("%s not emitted", name)
		}
		if len(f.GetMetric()) != 1 {
			t.Fatalf("%s: got %d series, want 1", name, len(f.GetMetric()))
		}
		got, ok := labelValue(f.GetMetric()[0], "deployment_id")
		if !ok || got != "dep-1" {
			t.Errorf("%s deployment_id = %q (present=%v), want %q", name, got, ok, "dep-1")
		}
	}
}

// TestEngineHistograms_DecodeTimeReexported pins the decode-time histogram's
// values pass through unchanged — its _sum is the denominator of a
// deployment's measured output speed.
func TestEngineHistograms_DecodeTimeReexported(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		DecodeTimeHistogram: snapshot(16, 10),
	})
	f := histogramFamily(t, m, "hivenet_agent_engine_request_decode_time_seconds")
	if f == nil {
		t.Fatal("decode-time histogram not emitted")
	}
	h := f.GetMetric()[0].GetHistogram()
	if h.GetSampleSum() != 16 || h.GetSampleCount() != 10 {
		t.Errorf("decode-time sum/count = %v/%v, want 16/10", h.GetSampleSum(), h.GetSampleCount())
	}
}

// TestEngineHistograms_NoDecodeTimeNoSeries pins that an engine that does not
// report decode time (SGLang, llama.cpp) emits no decode series — never a zero
// histogram a consumer would read as "infinitely fast".
func TestEngineHistograms_NoDecodeTimeNoSeries(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "sglang", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		TTFTHistogram: snapshot(3, 10),
	})
	if f := histogramFamily(t, m, "hivenet_agent_engine_request_decode_time_seconds"); f != nil && len(f.GetMetric()) > 0 {
		t.Errorf("decode-time series emitted without a decode snapshot: %d", len(f.GetMetric()))
	}
	if histogramFamily(t, m, "hivenet_agent_engine_ttft_seconds") == nil {
		t.Error("ttft not emitted alongside a missing decode histogram")
	}
}

// TestEngineHistograms_DeploymentsStaySeparate pins that two agents of
// different deployments yield distinct series — the property that makes a
// per-deployment selector correct.
func TestEngineHistograms_DeploymentsStaySeparate(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{TTFTHistogram: snapshot(3, 10)})
	m.AgentEnginePunctualUpdated("peer-2", "qwen", "vllm", "acme", "gpu-box", "dep-2", &domain.BackendMetrics{TTFTHistogram: snapshot(9, 30)})

	f := histogramFamily(t, m, "hivenet_agent_engine_ttft_seconds")
	if f == nil || len(f.GetMetric()) != 2 {
		t.Fatalf("want 2 TTFT series, got %v", f)
	}
	seen := map[string]uint64{}
	for _, metric := range f.GetMetric() {
		dep, _ := labelValue(metric, "deployment_id")
		seen[dep] = metric.GetHistogram().GetSampleCount()
	}
	if seen["dep-1"] != 10 || seen["dep-2"] != 30 {
		t.Errorf("per-deployment counts = %v, want dep-1:10 dep-2:30", seen)
	}
}

// TestEngineHistograms_UnregisterClearsDeploymentSeries pins that agent
// removal drops its histograms (no zombie series under the new label).
func TestEngineHistograms_UnregisterClearsDeploymentSeries(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		TTFTHistogram:       snapshot(3, 10),
		DecodeTimeHistogram: snapshot(16, 10),
	})
	m.AgentEnginePunctualUnregistered("peer-1", "qwen", "vllm", "acme", "gpu-box")
	for _, name := range []string{"hivenet_agent_engine_ttft_seconds", "hivenet_agent_engine_request_decode_time_seconds"} {
		if f := histogramFamily(t, m, name); f != nil && len(f.GetMetric()) > 0 {
			t.Errorf("%s still emitted after unregister", name)
		}
	}
}

// TestEngineHistograms_EmptyDeploymentIDIsUnset pins that an agent with no
// deployment id (HIVENET_ROUTER_DEPLOYMENT_ID not set) is labelled "unset" —
// the value the tenant metrics use — not "", which PromQL reads as an absent
// label, so the two families could not be selected or matched alike.
func TestEngineHistograms_EmptyDeploymentIDIsUnset(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "", &domain.BackendMetrics{TTFTHistogram: snapshot(3, 10)})
	f := histogramFamily(t, m, "hivenet_agent_engine_ttft_seconds")
	if f == nil || len(f.GetMetric()) != 1 {
		t.Fatal("ttft not emitted for an agent without a deployment id")
	}
	if got, _ := labelValue(f.GetMetric()[0], "deployment_id"); got != "unset" {
		t.Errorf("deployment_id = %q, want %q", got, "unset")
	}
}

// TestEngineHistograms_DeploymentChangeLeavesNoZombie is the regression for a
// peer whose registration metadata changes between updates: re-registration
// under another deployment, or an in-flight update in the 2s grace window
// still carrying the old session's labels. The peer must keep ONE series set,
// relabelled to the latest metadata, and unregister must clear it whatever
// labels it last had — never a frozen series for the old deployment.
func TestEngineHistograms_DeploymentChangeLeavesNoZombie(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{TTFTHistogram: snapshot(3, 10)})
	// Same peer, new deployment — and a grace-window update mixing an old
	// session's model with the new deployment id.
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-2", &domain.BackendMetrics{TTFTHistogram: snapshot(6, 20)})
	m.AgentEnginePunctualUpdated("peer-1", "qwen-old", "vllm", "acme", "gpu-box", "dep-2", &domain.BackendMetrics{})

	f := histogramFamily(t, m, "hivenet_agent_engine_ttft_seconds")
	if f == nil || len(f.GetMetric()) != 1 {
		t.Fatalf("want 1 TTFT series for the peer, got %v", f)
	}
	if got, _ := labelValue(f.GetMetric()[0], "deployment_id"); got != "dep-2" {
		t.Errorf("deployment_id = %q, want the latest %q", got, "dep-2")
	}
	if got := f.GetMetric()[0].GetHistogram().GetSampleCount(); got != 20 {
		t.Errorf("sample count = %d, want the latest snapshot's 20", got)
	}

	// Unregister with whatever labels the caller holds clears the peer.
	m.AgentEnginePunctualUnregistered("peer-1", "qwen", "vllm", "acme", "gpu-box")
	if f := histogramFamily(t, m, "hivenet_agent_engine_ttft_seconds"); f != nil && len(f.GetMetric()) > 0 {
		t.Errorf("ZOMBIE: %d TTFT series still emitted after unregister", len(f.GetMetric()))
	}
}
