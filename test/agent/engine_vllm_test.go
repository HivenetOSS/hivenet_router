// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package agent_test — VLLMEngine adapter tests against an httptest backend:
// health, model discovery, chat forwarding, and the /metrics scrape which drives
// the full Prometheus text parser, histogram average, and P90 quantile estimation.
package agent_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/domain"
)

// vllmBackend spins up an in-process HTTP server driven by the caller-supplied
// handler so each test can fake exactly the backend responses it needs; the
// server (and its matching client) are torn down automatically via t.Cleanup.
func vllmBackend(t *testing.T, h http.HandlerFunc) (string, *http.Client) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL, srv.Client()
}

func TestVLLM_Name(t *testing.T) {
	if (&agent.VLLMEngine{}).Name() != "vllm" {
		t.Error("Name must be vllm")
	}
}

func TestVLLM_WaitForReady(t *testing.T) {
	e := &agent.VLLMEngine{}

	// Backend answers /health with 200 → readiness probe must succeed.
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	})
	if err := e.WaitForReady(context.Background(), url, cl); err != nil {
		t.Errorf("healthy backend: %v", err)
	}

	// Backend that always returns 503 → readiness probe must surface an error
	// so the agent won't advertise an unloaded/unavailable model.
	url2, cl2 := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	if err := e.WaitForReady(context.Background(), url2, cl2); err == nil {
		t.Error("503 backend must be reported not-ready")
	}
}

func TestVLLM_DiscoverModels(t *testing.T) {
	e := &agent.VLLMEngine{}

	// /v1/models returns two entries → discovery must yield both ids in order.
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":[{"id":"qwen-3"},{"id":"llama-4"}]}`)
	})
	models, err := e.DiscoverModels(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("DiscoverModels: %v", err)
	}
	if len(models) != 2 || models[0] != "qwen-3" || models[1] != "llama-4" {
		t.Errorf("models = %v, want [qwen-3 llama-4]", models)
	}

	// Empty data → error.
	url2, cl2 := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":[]}`)
	})
	if _, err := e.DiscoverModels(context.Background(), url2, cl2); err == nil {
		t.Error("empty model list must error")
	}
}

// TestVLLM_ScrapeMetrics exercises the whole /metrics parse chain: scalar gauges,
// histogram average (sum/count), and P90 via linear interpolation over buckets.
func TestVLLM_ScrapeMetrics(t *testing.T) {
	// Fake Prometheus text-exposition payload mimicking a real vLLM /metrics
	// scrape. It carries four scalar gauges (kv-cache, running/waiting/preemptions)
	// plus one classic Prometheus histogram (time_to_first_token_seconds) expressed
	// as cumulative _bucket counts + _sum + _count. The bucket layout is chosen so
	// the average and P90 land on exact, hand-verifiable values:
	//   buckets (cumulative): le=0.1→0, le=0.5→5, le=1.0→10, le=+Inf→10
	//   sum=3.0, count=10
	// Average = sum/count = 3.0/10 = 0.3 (asserted exactly below).
	// P90: target rank = 0.9*count = 9. Cumulative counts show 5 obs at ≤0.5 and
	// 10 at ≤1.0, so rank 9 sits inside the (0.5,1.0] bucket. Linear interpolation
	// within that bucket gives 0.5 + (1.0-0.5)*(9-5)/(10-5) = 0.9, hence the (0.5,1.0]
	// range assertion (kept as a range so any equivalent interpolation passes).
	metricsText := `# HELP vllm:kv_cache_usage_perc KV cache
vllm:kv_cache_usage_perc 0.42
vllm:num_requests_running 3
vllm:num_requests_waiting 1
vllm:num_preemptions_total 7
vllm:time_to_first_token_seconds_bucket{le="0.1"} 0
vllm:time_to_first_token_seconds_bucket{le="0.5"} 5
vllm:time_to_first_token_seconds_bucket{le="1.0"} 10
vllm:time_to_first_token_seconds_bucket{le="+Inf"} 10
vllm:time_to_first_token_seconds_sum 3.0
vllm:time_to_first_token_seconds_count 10
`
	// Serve the canned payload at the scrape path the engine polls.
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			io.WriteString(w, metricsText)
		}
	})

	m, err := (&agent.VLLMEngine{}).ScrapeMetrics(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("ScrapeMetrics: %v", err)
	}

	// assertF dereferences an optional (*float64) metric and checks it against the
	// expected value, failing (not panicking) when the field was left nil.
	assertF := func(name string, got *float64, want float64) {
		if got == nil {
			t.Errorf("%s is nil", name)
		} else if *got != want {
			t.Errorf("%s = %v, want %v", name, *got, want)
		}
	}
	assertF("KVCacheUtilization", m.KVCacheUtilization, 0.42)
	assertF("RunningRequests", m.RunningRequests, 3)
	assertF("WaitingRequests", m.WaitingRequests, 1)
	assertF("PreemptionsTotal", m.PreemptionsTotal, 7)
	// Average = sum/count = 3.0/10 = 0.3.
	assertF("AvgTTFTSeconds", m.AvgTTFTSeconds, 0.3)

	// P90: rank 9 falls in the (0.5, 1.0] bucket → interpolated value in that range.
	if m.P90TTFTSeconds == nil {
		t.Fatal("P90TTFTSeconds is nil")
	}
	if p := *m.P90TTFTSeconds; !(p > 0.5 && p <= 1.0) {
		t.Errorf("P90TTFTSeconds = %v, want within (0.5, 1.0]", p)
	}
}

func TestVLLM_ForwardChat(t *testing.T) {
	e := &agent.VLLMEngine{}

	// Success path: backend echoes a canned completion plus a custom response
	// header, letting us verify both body pass-through and header propagation.
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			w.Header().Set("X-Backend", "vllm")
			io.WriteString(w, `{"id":"c1","object":"chat.completion"}`)
		}
	})
	req := domain.ChatRequest{Model: "m", RawBytes: []byte(`{"model":"m","messages":[]}`)}
	body, hdr, err := e.ForwardChat(context.Background(), url, cl, "m", req)
	if err != nil {
		t.Fatalf("ForwardChat: %v", err)
	}
	if hdr.Get("X-Backend") != "vllm" {
		t.Errorf("response headers not propagated: %v", hdr)
	}
	if string(body) == "" {
		t.Error("empty response body")
	}

	// Non-200 → BackendError carrying the status.
	url2, cl2 := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, `{"error":"upstream"}`)
	})
	_, _, err = e.ForwardChat(context.Background(), url2, cl2, "m", req)
	var be *agent.BackendError
	if !errors.As(err, &be) {
		t.Fatalf("expected *agent.BackendError, got %T (%v)", err, err)
	}
	if be.HTTPStatus != http.StatusBadGateway {
		t.Errorf("BackendError status = %d, want 502", be.HTTPStatus)
	}
}
