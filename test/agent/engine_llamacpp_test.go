// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package agent_test — LlamaCPPEngine adapter tests against an httptest
// backend. llama.cpp exposes no TTFT or ITL histogram, so the scrape must leave
// those fields nil; the decode-phase counters come from its predicted-token
// counters.
package agent_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"hivenet_router/internal/agent"
)

// TestLlamaCPP_ScrapeMetrics pins that the agent reads the decode-phase
// counters from llama.cpp's predicted-token counters (the universal
// output-speed numerator/denominator) and leaves TTFT/ITL nil, since llama.cpp
// exposes no time-to-first-token or inter-token-latency histogram.
func TestLlamaCPP_ScrapeMetrics(t *testing.T) {
	metricsText := `llamacpp:kv_cache_usage_ratio 0.3
llamacpp:requests_processing 1
llamacpp:requests_deferred 0
llamacpp:predicted_tokens_seconds 12.5
llamacpp:prompt_tokens_seconds 40.0
llamacpp:tokens_predicted_total 2500
llamacpp:tokens_predicted_seconds_total 20.0
`
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			io.WriteString(w, metricsText)
		}
	})

	m, err := (&agent.LlamaCPPEngine{}).ScrapeMetrics(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("ScrapeMetrics: %v", err)
	}

	assertF := func(name string, got *float64, want float64) {
		if got == nil {
			t.Errorf("%s is nil", name)
		} else if *got != want {
			t.Errorf("%s = %v, want %v", name, *got, want)
		}
	}
	assertF("KVCacheUtilization", m.KVCacheUtilization, 0.3)
	assertF("RunningRequests", m.RunningRequests, 1)
	assertF("WaitingRequests", m.WaitingRequests, 0)
	assertF("PredictedTokensPerSecond", m.PredictedTokensPerSecond, 12.5)
	assertF("PromptTokensPerSecond", m.PromptTokensPerSecond, 40.0)

	// Decode counters: predicted-token counters re-exported verbatim.
	assertF("DecodeTokensTotal", m.DecodeTokensTotal, 2500)
	assertF("DecodeSecondsTotal", m.DecodeSecondsTotal, 20.0)

	// TTFT/ITL unsupported on llama.cpp — every TTFT/ITL field stays nil.
	if m.AvgTTFTSeconds != nil || m.P90TTFTSeconds != nil {
		t.Errorf("TTFT fields set on llama.cpp: avg=%v p90=%v", m.AvgTTFTSeconds, m.P90TTFTSeconds)
	}
	if m.AvgITLSeconds != nil || m.P90ITLSeconds != nil {
		t.Errorf("ITL fields set on llama.cpp: avg=%v p90=%v", m.AvgITLSeconds, m.P90ITLSeconds)
	}
	if m.TTFTHistogram != nil || m.ITLHistogram != nil {
		t.Error("TTFT/ITL histograms set on llama.cpp")
	}
}

// TestLlamaCPP_ScrapeMetrics_NoDecodeCounters pins that an engine build without
// the predicted-token counters leaves the decode counters nil (no series).
func TestLlamaCPP_ScrapeMetrics_NoDecodeCounters(t *testing.T) {
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			io.WriteString(w, "llamacpp:requests_processing 1\n")
		}
	})
	m, err := (&agent.LlamaCPPEngine{}).ScrapeMetrics(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("ScrapeMetrics: %v", err)
	}
	if m.DecodeTokensTotal != nil || m.DecodeSecondsTotal != nil {
		t.Errorf("decode counters = %v/%v, want nil/nil", m.DecodeTokensTotal, m.DecodeSecondsTotal)
	}
}
