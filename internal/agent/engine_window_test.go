// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"math"
	"testing"
	"time"

	"hivenet_router/internal/domain"
)

func hist(sum float64, count uint64, buckets ...domain.HistogramBucket) *domain.HistogramSnapshot {
	return &domain.HistogramSnapshot{Sum: sum, Count: count, Buckets: buckets}
}

func fp(v float64) *float64 { return &v }

func near(a *float64, want float64) bool { return a != nil && math.Abs(*a-want) < 1e-9 }

// The figures are the change over the window, not the totals since the
// engine started: an engine slow long ago and fast now reports fast.
func TestEngineWindow_FiguresOverTheWindowOnly(t *testing.T) {
	now := time.Unix(1000, 0)
	w := &engineWindow{now: func() time.Time { return now }}
	// Since start: 100 TTFTs averaging 2 s, 10,000 ITLs averaging 0.1 s.
	w.apply(&domain.BackendMetrics{TTFTHistogram: hist(200, 100), ITLHistogram: hist(1000, 10000),
		PrefillComputedTokensTotal: fp(1e6), PrefillSecondsTotal: fp(1000)})
	now = now.Add(30 * time.Second)
	// In the last 30 s: 10 TTFTs of 0.1 s, 1,000 ITLs of 0.01 s, 200k tokens in 10 s.
	bm := &domain.BackendMetrics{
		TTFTHistogram:              hist(201, 110, domain.HistogramBucket{Le: 0.1, Count: 10}, domain.HistogramBucket{Le: 5, Count: 110}),
		ITLHistogram:               hist(1010, 11000),
		PrefillComputedTokensTotal: fp(1.2e6), PrefillSecondsTotal: fp(1010),
	}
	w.apply(bm)
	if !near(bm.AvgTTFTSeconds, 0.1) || !near(bm.AvgITLSeconds, 0.01) {
		t.Errorf("avg TTFT %v ITL %v, want 0.1 / 0.01 (the window, not since start)", *bm.AvgTTFTSeconds, *bm.AvgITLSeconds)
	}
	if bm.P90TTFTSeconds == nil || *bm.P90TTFTSeconds > 0.1 {
		t.Errorf("p90 TTFT %v, want ≤ 0.1 (all 10 window TTFTs are in the 0.1 bucket)", bm.P90TTFTSeconds)
	}
	if !near(bm.DecodeStepSeconds, 0.01) {
		t.Errorf("decode step %v, want the window ITL 0.01 (no decode counters)", bm.DecodeStepSeconds)
	}
	if !near(bm.PrefillTokensPerSecond, 20000) || bm.PrefillSeconds != 10 || bm.DecodeSamples != 1000 {
		t.Errorf("prefill %v tok/s over %v s, decode samples %v; want 20000 / 10 / 1000", bm.PrefillTokensPerSecond, bm.PrefillSeconds, bm.DecodeSamples)
	}
}

// Nothing happened in the window: no figure, so a gate on it passes and
// the replica is not judged on the past.
func TestEngineWindow_IdleWindowHasNoFigures(t *testing.T) {
	now := time.Unix(1000, 0)
	w := &engineWindow{now: func() time.Time { return now }}
	for range 3 {
		bm := &domain.BackendMetrics{TTFTHistogram: hist(500, 100), ITLHistogram: hist(9, 10), PrefillComputedTokensTotal: fp(5), PrefillSecondsTotal: fp(1)}
		w.apply(bm)
		if bm.AvgTTFTSeconds != nil || bm.AvgITLSeconds != nil || bm.P90TTFTSeconds != nil || bm.PrefillTokensPerSecond != nil {
			t.Fatalf("idle: got figures %+v", bm)
		}
		now = now.Add(20 * time.Second)
	}
}

// Old scrapes leave the window: activity older than EngineWindow is gone.
func TestEngineWindow_OldActivityLeaves(t *testing.T) {
	now := time.Unix(1000, 0)
	w := &engineWindow{now: func() time.Time { return now }}
	w.apply(&domain.BackendMetrics{TTFTHistogram: hist(0, 0)})
	now = now.Add(5 * time.Second)
	w.apply(&domain.BackendMetrics{TTFTHistogram: hist(50, 10)}) // 10 slow TTFTs
	for range 8 {
		now = now.Add(10 * time.Second)
		bm := &domain.BackendMetrics{TTFTHistogram: hist(50, 10)}
		w.apply(bm)
		if now.Sub(time.Unix(1005, 0)) > EngineWindow && bm.AvgTTFTSeconds != nil {
			t.Fatalf("at +%s the slow TTFTs are still counted: %v", now.Sub(time.Unix(1000, 0)), *bm.AvgTTFTSeconds)
		}
	}
}

// A counter going down means the engine restarted: start over, no negative
// figure.
func TestEngineWindow_EngineRestart(t *testing.T) {
	now := time.Unix(1000, 0)
	w := &engineWindow{now: func() time.Time { return now }}
	w.apply(&domain.BackendMetrics{TTFTHistogram: hist(500, 100)})
	now = now.Add(10 * time.Second)
	bm := &domain.BackendMetrics{TTFTHistogram: hist(1, 2)}
	w.apply(bm)
	if bm.AvgTTFTSeconds != nil {
		t.Errorf("after a restart: %v, want nil until a second scrape", *bm.AvgTTFTSeconds)
	}
	now = now.Add(10 * time.Second)
	bm = &domain.BackendMetrics{TTFTHistogram: hist(3, 4)}
	w.apply(bm)
	if !near(bm.AvgTTFTSeconds, 1) {
		t.Errorf("after a restart and a new scrape: %v, want 1", bm.AvgTTFTSeconds)
	}
}

func TestEngineWindow_NilSafe(t *testing.T) {
	var w *engineWindow
	w.apply(&domain.BackendMetrics{})
	newEngineWindow().apply(nil)
}

// Decode counters (every request, streamed or not) win over the inter-token
// latency histogram (SGLang fills it for streamed requests only).
func TestEngineWindow_DecodeStepFromDecodeCounters(t *testing.T) {
	now := time.Unix(1000, 0)
	w := &engineWindow{now: func() time.Time { return now }}
	w.apply(&domain.BackendMetrics{DecodeTokensTotal: fp(100), DecodeSecondsTotal: fp(1)})
	now = now.Add(10 * time.Second)
	bm := &domain.BackendMetrics{DecodeTokensTotal: fp(1100), DecodeSecondsTotal: fp(21)}
	w.apply(bm)
	if !near(bm.DecodeStepSeconds, 0.02) || bm.DecodeSamples != 1000 || bm.AvgITLSeconds != nil {
		t.Errorf("decode step %v over %v tokens (ITL %v), want 20 ms over 1000 and no ITL", bm.DecodeStepSeconds, bm.DecodeSamples, bm.AvgITLSeconds)
	}
}

// Short prompts give no prefill speed: a fixed per-request cost would make
// tokens over seconds read far below the engine's speed.
func TestEngineWindow_ShortPromptsGiveNoPrefillSpeed(t *testing.T) {
	now := time.Unix(1000, 0)
	w := &engineWindow{now: func() time.Time { return now }}
	w.apply(&domain.BackendMetrics{PrefillComputedTokensTotal: fp(0), PrefillSecondsTotal: fp(0), PrefillRequestsTotal: fp(0)})
	now = now.Add(10 * time.Second)
	short := &domain.BackendMetrics{PrefillComputedTokensTotal: fp(100), PrefillSecondsTotal: fp(0.1), PrefillRequestsTotal: fp(10)}
	w.apply(short)
	if short.PrefillTokensPerSecond != nil {
		t.Errorf("10-token prompts gave a prefill speed %v", *short.PrefillTokensPerSecond)
	}
	now = now.Add(10 * time.Second)
	long := &domain.BackendMetrics{PrefillComputedTokensTotal: fp(30100), PrefillSecondsTotal: fp(1.1), PrefillRequestsTotal: fp(30)}
	w.apply(long)
	if long.PrefillTokensPerSecond == nil {
		t.Error("~1,500-token prompts gave no prefill speed")
	}
}
