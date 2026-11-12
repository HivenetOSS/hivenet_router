// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package agent_test — SGLangEngine adapter tests against an httptest backend.
// SGLang splits its latency histograms across is_streaming label variants, so
// these fixtures exercise the variant-merging parse path that the single-series
// vLLM fixtures cannot.
package agent_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"hivenet_router/internal/agent"
)

// TestSGLang_ScrapeMetrics pins that the agent ships SGLang's TTFT and ITL
// histograms with the is_streaming variants summed at each le bound (so the
// router re-exports one series, not one per variant), plus the scalar
// sum/count and the decode-phase counters.
func TestSGLang_ScrapeMetrics(t *testing.T) {
	// TTFT: two is_streaming variants.
	//   streaming:   buckets le=0.5→2, le=1.0→4, +Inf→4; sum=2.4, count=4
	//   non-streaming: buckets le=0.5→1, le=1.0→3, +Inf→3; sum=1.8, count=3
	// Merged: le=0.5→3, le=1.0→7; sum=4.2, count=7. Average = 4.2/7 = 0.6.
	// ITL (per-token inter_token_latency_seconds, no is_streaming label):
	// buckets le=0.05→2, +Inf→7; sum=0.28, count=7. The per-request
	// request_time_per_output_token_seconds is present with other values and
	// must be ignored.
	// Lengths: prompt_tokens_histogram le=128→5, +Inf→7, sum=700, count=7;
	// generation_tokens_histogram le=128→6, +Inf→7, sum=800, count=7.
	// Decode counters: generation_tokens_total 500+300=800, num_requests_total
	// 4+3=7 → decode tokens = 800-7 = 793. e2e sum 20+10=30, TTFT sum 4.2 →
	// decode seconds = 30-4.2 = 25.8.
	metricsText := `sglang:token_usage 0.5
sglang:max_total_num_tokens 100000
sglang:num_running_reqs 3
sglang:num_queue_reqs 1
sglang:time_to_first_token_seconds_bucket{is_streaming="true",le="0.5"} 2
sglang:time_to_first_token_seconds_bucket{is_streaming="true",le="1.0"} 4
sglang:time_to_first_token_seconds_bucket{is_streaming="true",le="+Inf"} 4
sglang:time_to_first_token_seconds_bucket{is_streaming="false",le="0.5"} 1
sglang:time_to_first_token_seconds_bucket{is_streaming="false",le="1.0"} 3
sglang:time_to_first_token_seconds_bucket{is_streaming="false",le="+Inf"} 3
sglang:time_to_first_token_seconds_sum{is_streaming="true"} 2.4
sglang:time_to_first_token_seconds_sum{is_streaming="false"} 1.8
sglang:time_to_first_token_seconds_count{is_streaming="true"} 4
sglang:time_to_first_token_seconds_count{is_streaming="false"} 3
sglang:inter_token_latency_seconds_bucket{le="0.05"} 2
sglang:inter_token_latency_seconds_bucket{le="+Inf"} 7
sglang:inter_token_latency_seconds_sum 0.28
sglang:inter_token_latency_seconds_count 7
sglang:request_time_per_output_token_seconds_bucket{is_streaming="true",le="0.05"} 1
sglang:request_time_per_output_token_seconds_bucket{is_streaming="true",le="+Inf"} 1
sglang:request_time_per_output_token_seconds_sum{is_streaming="true"} 9
sglang:request_time_per_output_token_seconds_count{is_streaming="true"} 1
sglang:prompt_tokens_histogram_bucket{le="128"} 5
sglang:prompt_tokens_histogram_bucket{le="+Inf"} 7
sglang:prompt_tokens_histogram_sum 700
sglang:prompt_tokens_histogram_count 7
sglang:generation_tokens_histogram_bucket{le="128"} 6
sglang:generation_tokens_histogram_bucket{le="+Inf"} 7
sglang:generation_tokens_histogram_sum 800
sglang:generation_tokens_histogram_count 7
sglang:generation_tokens_total{is_streaming="true"} 500
sglang:generation_tokens_total{is_streaming="false"} 300
sglang:num_requests_total{is_streaming="true"} 4
sglang:num_requests_total{is_streaming="false"} 3
sglang:e2e_request_latency_seconds_sum{is_streaming="true"} 20.0
sglang:e2e_request_latency_seconds_sum{is_streaming="false"} 10.0
`
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			io.WriteString(w, metricsText)
		}
	})

	m, err := (&agent.SGLangEngine{}).ScrapeMetrics(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("ScrapeMetrics: %v", err)
	}

	// Scalar gauges read the first-occurrence value (single series here).
	assertF := func(name string, got *float64, want float64) {
		if got == nil {
			t.Errorf("%s is nil", name)
		} else if *got != want {
			t.Errorf("%s = %v, want %v", name, *got, want)
		}
	}
	assertF("KVCacheUtilization", m.KVCacheUtilization, 0.5)
	assertF("RunningRequests", m.RunningRequests, 3)
	assertF("WaitingRequests", m.WaitingRequests, 1)
	// Merged TTFT average = (2.4+1.8)/(4+3) = 4.2/7 = 0.6.
	assertF("AvgTTFTSeconds", m.AvgTTFTSeconds, 4.2/7)

	// Merged TTFT histogram: one point per le bound, variants summed.
	if m.TTFTHistogram == nil {
		t.Fatal("TTFTHistogram is nil")
	}
	if m.TTFTHistogram.Count != 7 || m.TTFTHistogram.Sum != 4.2 {
		t.Errorf("TTFT sum/count = %v/%v, want 4.2/7", m.TTFTHistogram.Sum, m.TTFTHistogram.Count)
	}
	gotLe := map[float64]uint64{}
	for _, b := range m.TTFTHistogram.Buckets {
		gotLe[b.Le] = b.Count
	}
	if gotLe[0.5] != 3 || gotLe[1.0] != 7 {
		t.Errorf("TTFT merged buckets = %v, want le=0.5→3 le=1.0→7", gotLe)
	}

	// ITL histogram from the per-token series.
	if m.ITLHistogram == nil {
		t.Fatal("ITLHistogram is nil")
	}
	if m.ITLHistogram.Count != 7 || m.ITLHistogram.Sum != 0.28 {
		t.Errorf("ITL sum/count = %v/%v, want 0.28/7", m.ITLHistogram.Sum, m.ITLHistogram.Count)
	}
	itlLe := map[float64]uint64{}
	for _, b := range m.ITLHistogram.Buckets {
		itlLe[b.Le] = b.Count
	}
	if itlLe[0.05] != 2 {
		t.Errorf("ITL merged buckets = %v, want le=0.05→2", itlLe)
	}

	// Length histograms.
	if m.PromptTokensHistogram == nil || m.PromptTokensHistogram.Count != 7 || m.PromptTokensHistogram.Sum != 700 {
		t.Errorf("PromptTokensHistogram = %+v, want sum 700 count 7", m.PromptTokensHistogram)
	}
	if m.GenerationTokensHistogram == nil || m.GenerationTokensHistogram.Count != 7 || m.GenerationTokensHistogram.Sum != 800 {
		t.Errorf("GenerationTokensHistogram = %+v, want sum 800 count 7", m.GenerationTokensHistogram)
	}

	// Decode counters: tokens = 800-7, seconds = 30-4.2.
	if m.DecodeTokensTotal == nil {
		t.Fatal("DecodeTokensTotal is nil")
	} else if *m.DecodeTokensTotal != 793 {
		t.Errorf("DecodeTokensTotal = %v, want 793", *m.DecodeTokensTotal)
	}
	if m.DecodeSecondsTotal == nil {
		t.Fatal("DecodeSecondsTotal is nil")
	} else if *m.DecodeSecondsTotal != 25.8 {
		t.Errorf("DecodeSecondsTotal = %v, want 25.8", *m.DecodeSecondsTotal)
	}
}

// TestSGLang_ScrapeMetrics_NoDecodeCounters pins that when the engine reports
// no generation/request/e2e series, the decode counters stay nil (no series).
func TestSGLang_ScrapeMetrics_NoDecodeCounters(t *testing.T) {
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			io.WriteString(w, "sglang:num_running_reqs 1\n")
		}
	})
	m, err := (&agent.SGLangEngine{}).ScrapeMetrics(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("ScrapeMetrics: %v", err)
	}
	if m.DecodeTokensTotal != nil || m.DecodeSecondsTotal != nil {
		t.Errorf("decode counters = %v/%v, want nil/nil", m.DecodeTokensTotal, m.DecodeSecondsTotal)
	}
}

// sglangDecodeText is a minimal SGLang /metrics body for the decode-seconds
// tests: e2e and TTFT sums plus the request counter.
func sglangDecodeText(e2eSum, ttftSum, requests string) string {
	return "sglang:generation_tokens_total 1000\n" +
		"sglang:num_requests_total " + requests + "\n" +
		"sglang:e2e_request_latency_seconds_sum " + e2eSum + "\n" +
		"sglang:time_to_first_token_seconds_sum " + ttftSum + "\n"
}

// TestSGLang_DecodeSecondsNeverDecreases pins that decode seconds, exported
// as a Prometheus counter, never go down while the engine runs. SGLang adds a
// request's TTFT at its first token but its e2e latency only when it
// finishes, so e2e sum − TTFT sum drops when a request starts and none
// finishes; Prometheus would read that drop as a reset.
func TestSGLang_DecodeSecondsNeverDecreases(t *testing.T) {
	body := sglangDecodeText("20", "1", "10")
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			io.WriteString(w, body)
		}
	})
	e := &agent.SGLangEngine{}
	scrape := func() float64 {
		t.Helper()
		m, err := e.ScrapeMetrics(context.Background(), url, cl)
		if err != nil || m.DecodeSecondsTotal == nil {
			t.Fatalf("ScrapeMetrics: m=%v err=%v", m, err)
		}
		return *m.DecodeSecondsTotal
	}

	if got := scrape(); got != 19 {
		t.Fatalf("first scrape = %v, want 19", got)
	}
	// A request reached its first token (TTFT +0.8), none finished.
	body = sglangDecodeText("20", "1.8", "10")
	if got := scrape(); got != 19 {
		t.Errorf("after a first token = %v, want 19 (held, not 18.2)", got)
	}
	// It finished after 5 s: e2e +5, so the raw value is 25-1.8 = 23.2.
	body = sglangDecodeText("25", "1.8", "11")
	if got := scrape(); got != 23.2 {
		t.Errorf("after it finished = %v, want 23.2", got)
	}
	// Engine restarted: num_requests_total went down, the drop is real.
	body = sglangDecodeText("3", "0.5", "1")
	if got := scrape(); got != 2.5 {
		t.Errorf("after a restart = %v, want 2.5", got)
	}
}
