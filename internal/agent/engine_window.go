// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"math"
	"sync"
	"time"

	"hivenet_router/internal/domain"
)

// EngineWindow is how far back the agent's latency figures look: averages,
// percentiles and the prefill speed it reports are computed over the engine
// counters' change in this window, never since the engine started. A
// replica that was slow once is judged on what it does now.
const EngineWindow = 60 * time.Second

// engineWindow keeps the engine's cumulative counters from the scrapes of the
// last EngineWindow and turns them into windowed figures.
type engineWindow struct {
	mu      sync.Mutex
	samples []windowSample
	now     func() time.Time
}

type windowSample struct {
	at               time.Time
	ttft, itl        *domain.HistogramSnapshot
	prefTok, prefSec *float64
	prefReq          *float64
	decTok, decSec   *float64
}

// minPrefillTokensPerRequest: a window whose requests computed fewer prompt
// tokens than this on average reports no prefill speed. On short prompts a
// fixed per-request cost dominates the prefill time, so tokens over seconds
// would read far below the engine's real speed; and with short prompts the
// prefill speed hardly matters to the cost.
const minPrefillTokensPerRequest = 256

func newEngineWindow() *engineWindow { return &engineWindow{now: time.Now} }

// apply records a scrape and replaces bm's latency figures (average and p90
// TTFT and ITL) with their values over the window, and sets the prefill
// speed. A figure with no observation in the window is nil.
func (w *engineWindow) apply(bm *domain.BackendMetrics) {
	if w == nil || bm == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	cur := windowSample{at: now, ttft: bm.TTFTHistogram, itl: bm.ITLHistogram,
		prefTok: bm.PrefillComputedTokensTotal, prefSec: bm.PrefillSecondsTotal, prefReq: bm.PrefillRequestsTotal,
		decTok: bm.DecodeTokensTotal, decSec: bm.DecodeSecondsTotal}
	if n := len(w.samples); n > 0 && restarted(w.samples[n-1], cur) {
		w.samples = w.samples[:0] // the engine restarted: its counters began again
	}
	w.samples = append(w.samples, cur)
	// The base is the oldest scrape still inside the window; older ones go.
	cut := now.Add(-EngineWindow)
	i := 0
	for i < len(w.samples)-1 && w.samples[i+1].at.Before(cut) {
		i++
	}
	if w.samples[i].at.Before(cut) && i < len(w.samples)-1 {
		i++
	}
	w.samples = w.samples[i:]
	base := w.samples[0]

	bm.AvgTTFTSeconds, bm.P90TTFTSeconds, _ = windowedHistogram(base.ttft, cur.ttft)
	var itlN float64
	bm.AvgITLSeconds, bm.P90ITLSeconds, itlN = windowedHistogram(base.itl, cur.itl)
	// The decode step: decode seconds over decode tokens covers every
	// request (SGLang records inter-token latencies for streamed ones only);
	// the average inter-token latency when the engine has no such counters.
	bm.DecodeStepSeconds, bm.DecodeSamples = nil, 0
	if dTok, ok := delta(base.decTok, cur.decTok); ok && dTok > 0 {
		if dSec, ok := delta(base.decSec, cur.decSec); ok && dSec > 0 {
			step := dSec / dTok
			bm.DecodeStepSeconds, bm.DecodeSamples = &step, dTok
		}
	}
	if bm.DecodeStepSeconds == nil && bm.AvgITLSeconds != nil {
		step := *bm.AvgITLSeconds
		bm.DecodeStepSeconds, bm.DecodeSamples = &step, itlN
	}
	bm.PrefillTokensPerSecond, bm.PrefillSeconds = nil, 0
	if dTok, ok := delta(base.prefTok, cur.prefTok); ok {
		dReq, haveReq := delta(base.prefReq, cur.prefReq)
		longEnough := !haveReq || (dReq > 0 && dTok/dReq >= minPrefillTokensPerRequest)
		if dSec, ok := delta(base.prefSec, cur.prefSec); ok && dSec > 0 && dTok > 0 && longEnough {
			rate := dTok / dSec
			bm.PrefillTokensPerSecond = &rate
			bm.PrefillSeconds = dSec
		}
	}
}

// windowedHistogram returns the average, the p90 and the number of
// observations between two cumulative snapshots; nil figures when none.
func windowedHistogram(base, cur *domain.HistogramSnapshot) (avg, p90 *float64, n float64) {
	if base == nil || cur == nil || cur.Count <= base.Count {
		return nil, nil, 0
	}
	dCount := float64(cur.Count - base.Count)
	a := (cur.Sum - base.Sum) / dCount
	if a < 0 || math.IsNaN(a) {
		return nil, nil, 0
	}
	baseAt := make(map[float64]uint64, len(base.Buckets))
	for _, b := range base.Buckets {
		baseAt[b.Le] = b.Count
	}
	pts := make([]bucketPoint, 0, len(cur.Buckets)+1)
	for _, b := range cur.Buckets {
		c := float64(b.Count)
		if prev, ok := baseAt[b.Le]; ok {
			c -= float64(prev)
		}
		pts = append(pts, bucketPoint{le: b.Le, count: math.Max(c, 0)})
	}
	pts = append(pts, bucketPoint{le: math.Inf(1), count: dCount})
	return &a, histogramQuantile(pts, 0.9), dCount
}

func delta(base, cur *float64) (float64, bool) {
	if base == nil || cur == nil || *cur < *base {
		return 0, false
	}
	return *cur - *base, true
}

// restarted reports whether a counter went down between two scrapes: the
// engine restarted and its cumulative counters began again.
func restarted(prev, cur windowSample) bool {
	down := func(a, b *domain.HistogramSnapshot) bool { return a != nil && b != nil && b.Count < a.Count }
	fdown := func(a, b *float64) bool { return a != nil && b != nil && *b < *a }
	return down(prev.ttft, cur.ttft) || down(prev.itl, cur.itl) || fdown(prev.prefTok, cur.prefTok) || fdown(prev.prefSec, cur.prefSec)
}
