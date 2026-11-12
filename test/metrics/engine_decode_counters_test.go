// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
)

// f64 returns a pointer to v for BackendMetrics' optional fields.
func f64(v float64) *float64 { return &v }

// decodeCounterValue returns the counter value of the first metric in family,
// or nil when the family carries no series.
func decodeCounterValue(t *testing.T, m *metrics.RouterMetrics, name string) *float64 {
	t.Helper()
	f := histogramFamily(t, m, name)
	if f == nil || len(f.GetMetric()) == 0 {
		return nil
	}
	v := f.GetMetric()[0].GetCounter().GetValue()
	return &v
}

// TestEngineDecodeCounters_CarryDeploymentID pins that both decode-phase
// counters are emitted with the full histogram label set (including
// deployment_id) and the engine's cumulative values verbatim — so
// rate(tokens) / rate(seconds) is the deployment's measured output speed on
// every engine, and the counters select exactly like the histograms.
func TestEngineDecodeCounters_CarryDeploymentID(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		DecodeTokensTotal:  f64(790),
		DecodeSecondsTotal: f64(16),
	})

	for _, name := range []string{
		"hivenet_agent_engine_decode_tokens_total",
		"hivenet_agent_engine_decode_seconds_total",
	} {
		f := histogramFamily(t, m, name)
		if f == nil {
			t.Fatalf("%s not emitted", name)
		}
		if len(f.GetMetric()) != 1 {
			t.Fatalf("%s: got %d series, want 1", name, len(f.GetMetric()))
		}
		dep, ok := labelValue(f.GetMetric()[0], "deployment_id")
		if !ok || dep != "dep-1" {
			t.Errorf("%s deployment_id = %q (present=%v), want %q", name, dep, ok, "dep-1")
		}
	}
	if v := decodeCounterValue(t, m, "hivenet_agent_engine_decode_tokens_total"); v == nil || *v != 790 {
		t.Errorf("decode_tokens_total = %v, want 790", v)
	}
	if v := decodeCounterValue(t, m, "hivenet_agent_engine_decode_seconds_total"); v == nil || *v != 16 {
		t.Errorf("decode_seconds_total = %v, want 16", v)
	}
}

// TestEngineDecodeCounters_NoSeriesWhenAbsent pins that an engine that reports
// no decode counters (SGLang/llama.cpp builds without the source metrics)
// emits no decode series — never a zero counter a consumer would read as
// "no tokens yet".
func TestEngineDecodeCounters_NoSeriesWhenAbsent(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		TTFTHistogram: snapshot(3, 10),
	})
	for _, name := range []string{
		"hivenet_agent_engine_decode_tokens_total",
		"hivenet_agent_engine_decode_seconds_total",
	} {
		if v := decodeCounterValue(t, m, name); v != nil {
			t.Errorf("%s emitted without a decode counter: %v", name, *v)
		}
	}
}

// TestEngineDecodeCounters_EmptyDeploymentIDIsUnset pins that an agent with no
// deployment id is labelled "unset" on the counters, the same value the
// histograms and the tenant metrics use, so all three families select alike.
func TestEngineDecodeCounters_EmptyDeploymentIDIsUnset(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "", &domain.BackendMetrics{
		DecodeTokensTotal:  f64(100),
		DecodeSecondsTotal: f64(8),
	})
	f := histogramFamily(t, m, "hivenet_agent_engine_decode_tokens_total")
	if f == nil || len(f.GetMetric()) != 1 {
		t.Fatal("decode counter not emitted for an agent without a deployment id")
	}
	if got, _ := labelValue(f.GetMetric()[0], "deployment_id"); got != "unset" {
		t.Errorf("deployment_id = %q, want %q", got, "unset")
	}
}

// TestEngineDecodeCounters_DeploymentsStaySeparate pins that two peers of
// different deployments yield distinct decode-counter series, so a per-
// deployment output-speed query stays correct.
func TestEngineDecodeCounters_DeploymentsStaySeparate(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		DecodeTokensTotal: f64(100),
	})
	m.AgentEnginePunctualUpdated("peer-2", "qwen", "vllm", "acme", "gpu-box", "dep-2", &domain.BackendMetrics{
		DecodeTokensTotal: f64(300),
	})
	f := histogramFamily(t, m, "hivenet_agent_engine_decode_tokens_total")
	if f == nil || len(f.GetMetric()) != 2 {
		t.Fatalf("want 2 decode-token series, got %v", f)
	}
	seen := map[string]float64{}
	for _, metric := range f.GetMetric() {
		dep, _ := labelValue(metric, "deployment_id")
		seen[dep] = metric.GetCounter().GetValue()
	}
	if seen["dep-1"] != 100 || seen["dep-2"] != 300 {
		t.Errorf("per-deployment decode tokens = %v, want dep-1:100 dep-2:300", seen)
	}
}

// TestEngineDecodeCounters_UnregisterClears pins that agent removal drops the
// decode-counter series (no zombie series under the new labels).
func TestEngineDecodeCounters_UnregisterClears(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		DecodeTokensTotal:  f64(100),
		DecodeSecondsTotal: f64(8),
	})
	m.AgentEnginePunctualUnregistered("peer-1", "qwen", "vllm", "acme", "gpu-box")
	for _, name := range []string{
		"hivenet_agent_engine_decode_tokens_total",
		"hivenet_agent_engine_decode_seconds_total",
	} {
		if v := decodeCounterValue(t, m, name); v != nil {
			t.Errorf("%s still emitted after unregister: %v", name, *v)
		}
	}
}

// TestEngineDecodeCounters_RelabelKeepsOneSeries is the regression for a peer
// whose registration metadata changes between updates: the peer must keep ONE
// series set, relabelled to the latest deployment, whatever labels it last
// carried — never a frozen series for the old deployment.
func TestEngineDecodeCounters_RelabelKeepsOneSeries(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-1", &domain.BackendMetrics{
		DecodeTokensTotal: f64(100),
	})
	m.AgentEnginePunctualUpdated("peer-1", "qwen", "vllm", "acme", "gpu-box", "dep-2", &domain.BackendMetrics{
		DecodeTokensTotal: f64(200),
	})
	f := histogramFamily(t, m, "hivenet_agent_engine_decode_tokens_total")
	if f == nil || len(f.GetMetric()) != 1 {
		t.Fatalf("want 1 decode-token series for the peer, got %v", f)
	}
	if got, _ := labelValue(f.GetMetric()[0], "deployment_id"); got != "dep-2" {
		t.Errorf("deployment_id = %q, want the latest %q", got, "dep-2")
	}
	if v := f.GetMetric()[0].GetCounter().GetValue(); v != 200 {
		t.Errorf("decode_tokens_total = %v, want the latest snapshot's 200", v)
	}
}
