// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics

import (
	"math"
	"sync"

	"hivenet_router/internal/domain"

	"github.com/prometheus/client_golang/prometheus"
)

// engineHistogramLabels is the label set on every histogram metric emitted by
// engineHistogramCollector. Kept as a package-level var so the Desc values in
// the collector and any future consumers stay in sync.
var engineHistogramLabels = []string{"peer_id", "model", "engine", "organization", "machine"}

// engineHistogramCollector re-exports per-peer engine histogram snapshots
// (TTFT, ITL, prompt-token length, generation-token length) as proper
// Prometheus histograms via prometheus.NewConstHistogram.
//
// Why a custom collector rather than HistogramVec.Observe: the agent scrapes
// vLLM's /metrics endpoint and ships the full bucket counts in BackendMetrics.
// We don't have individual observations to Observe() — we have cumulative
// bucket counts. ConstHistogram is designed for exactly this case.
//
// Why histograms rather than scalar percentile gauges: correct fleet-wide
// aggregation. histogram_quantile(q, sum by(le) (rate(bucket))) gives the
// true fleet q-quantile; avg(per-peer-p95-gauge) does not. It also unlocks
// heatmap panels, which require the raw bucket distribution.
type engineHistogramCollector struct {
	mu sync.RWMutex

	ttftDesc   *prometheus.Desc
	itlDesc    *prometheus.Desc
	promptDesc *prometheus.Desc
	genDesc    *prometheus.Desc

	// snapshots keyed by peer label combination — one entry per active peer.
	snapshots map[string]*engineHistSnapshot
}

// engineHistSnapshot holds the latest histogram snapshots for a single peer.
// Label values are stored in the order of engineHistogramLabels so they can be
// passed positionally to NewConstHistogram.
type engineHistSnapshot struct {
	labelValues []string
	ttft        *domain.HistogramSnapshot
	itl         *domain.HistogramSnapshot
	prompt      *domain.HistogramSnapshot
	gen         *domain.HistogramSnapshot
}

func newEngineHistogramCollector() *engineHistogramCollector {
	return &engineHistogramCollector{
		ttftDesc: prometheus.NewDesc(
			"hivenet_router_agent_engine_ttft_seconds",
			"Time-to-first-token latency histogram (seconds). Re-exported from the engine's TTFT histogram with the router's label set. Use histogram_quantile() for correct fleet-wide percentiles.",
			engineHistogramLabels, nil,
		),
		itlDesc: prometheus.NewDesc(
			"hivenet_router_agent_engine_itl_seconds",
			"Inter-token latency histogram (seconds). Re-exported from the engine's ITL histogram with the router's label set.",
			engineHistogramLabels, nil,
		),
		promptDesc: prometheus.NewDesc(
			"hivenet_router_agent_engine_request_prompt_tokens",
			"Per-request prompt length histogram (tokens). Drives the prompt-length workload-shape heatmap on the Inference Engine dashboard.",
			engineHistogramLabels, nil,
		),
		genDesc: prometheus.NewDesc(
			"hivenet_router_agent_engine_request_generation_tokens",
			"Per-request generation length histogram (tokens). Drives the generation-length workload-shape heatmap on the Inference Engine dashboard.",
			engineHistogramLabels, nil,
		),
		snapshots: make(map[string]*engineHistSnapshot),
	}
}

// Describe implements prometheus.Collector.
func (c *engineHistogramCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ttftDesc
	ch <- c.itlDesc
	ch <- c.promptDesc
	ch <- c.genDesc
}

// Collect implements prometheus.Collector. It emits one ConstHistogram per
// (peer, metric) pair, skipping any metric whose snapshot is still nil (the
// engine has not yet reported that histogram).
func (c *engineHistogramCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, snap := range c.snapshots {
		c.emit(ch, c.ttftDesc, snap.ttft, snap.labelValues)
		c.emit(ch, c.itlDesc, snap.itl, snap.labelValues)
		c.emit(ch, c.promptDesc, snap.prompt, snap.labelValues)
		c.emit(ch, c.genDesc, snap.gen, snap.labelValues)
	}
}

// emit builds and sends a single ConstHistogram from a HistogramSnapshot.
// Errors from NewConstHistogram (typically bucket-count mismatch) are logged
// but do not stop other metrics — one misbehaving snapshot must not break the
// entire /metrics response.
func (c *engineHistogramCollector) emit(
	ch chan<- prometheus.Metric,
	desc *prometheus.Desc,
	hs *domain.HistogramSnapshot,
	labels []string,
) {
	if hs == nil {
		return
	}
	// ConstHistogram expects the +Inf bucket to be implicit via the count
	// parameter. Filter +Inf out of the bucket map to avoid double-counting.
	bucketsMap := make(map[float64]uint64, len(hs.Buckets))
	for _, b := range hs.Buckets {
		if math.IsInf(b.Le, 1) {
			continue
		}
		bucketsMap[b.Le] = b.Count
	}
	m, err := prometheus.NewConstHistogram(desc, hs.Count, hs.Sum, bucketsMap, labels...)
	if err != nil {
		log.Warnf("engineHistogramCollector: NewConstHistogram(%s) failed: %v", desc, err)
		return
	}
	ch <- m
}

// update stores the latest snapshot for a peer. Nil histogram fields on bm are
// silently skipped — the existing snapshot (if any) is retained, matching the
// scalar-gauge convention elsewhere in the router.
func (c *engineHistogramCollector) update(peerID, model, engine, organization, machine string, bm *domain.BackendMetrics) {
	key := engineSnapshotKey(peerID, model, engine, organization, machine)

	c.mu.Lock()
	defer c.mu.Unlock()

	snap, exists := c.snapshots[key]
	if !exists {
		snap = &engineHistSnapshot{
			labelValues: []string{peerID, model, engine, organization, machine},
		}
		c.snapshots[key] = snap
	}
	if bm.TTFTHistogram != nil {
		snap.ttft = bm.TTFTHistogram
	}
	if bm.ITLHistogram != nil {
		snap.itl = bm.ITLHistogram
	}
	if bm.PromptTokensHistogram != nil {
		snap.prompt = bm.PromptTokensHistogram
	}
	if bm.GenerationTokensHistogram != nil {
		snap.gen = bm.GenerationTokensHistogram
	}
}

// remove clears the snapshot for a peer. Called on agent disconnect so the
// /metrics response no longer emits histograms for a departed peer.
func (c *engineHistogramCollector) remove(peerID, model, engine, organization, machine string) {
	key := engineSnapshotKey(peerID, model, engine, organization, machine)

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.snapshots, key)
}

// engineSnapshotKey deterministically combines the engine label set into a
// single map key. Pipe is a safe separator — none of the label values contain
// it (peer IDs are libp2p-style hex, org/machine are free-form but pipe-free
// in practice).
func engineSnapshotKey(peerID, model, engine, organization, machine string) string {
	return peerID + "|" + model + "|" + engine + "|" + organization + "|" + machine
}

// engineFinishReasonTracker converts the cumulative-count-per-reason snapshots
// reported by the engine into counter deltas suitable for Add() on a Prometheus
// CounterVec.
//
// Pod restarts manifest as a decrease in cumulative count (new < prev). In
// that case the new value is treated as the full delta — we assume the
// pre-restart observations were already accounted for on the previous scrape,
// and the post-restart count is fresh. This matches how PromQL's rate()
// handles counter resets.
type engineFinishReasonTracker struct {
	mu sync.Mutex
	// state[peerKey][reason] = last cumulative count observed on that peer.
	state map[string]map[string]uint64
}

func newEngineFinishReasonTracker() *engineFinishReasonTracker {
	return &engineFinishReasonTracker{state: make(map[string]map[string]uint64)}
}

// deltas returns the per-reason increment since the last snapshot and updates
// the internal state with the current cumulative counts.
//
// First observation for a peer records a baseline and emits no deltas. This
// prevents a day-zero spike on hivenet_router_agent_engine_request_success_total —
// otherwise the vLLM pod's cumulative-since-its-own-startup count would be
// Add()ed as a single delta on the first scrape, inflating rate() for a full
// window. After the baseline is recorded, subsequent scrapes compute real
// deltas and the counter reflects "events observed since this router started
// tracking this peer", matching the semantics of every other router counter.
func (t *engineFinishReasonTracker) deltas(peerKey string, current map[string]uint64) map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	prev, exists := t.state[peerKey]
	if !exists {
		prev = make(map[string]uint64, len(current))
		for reason, cur := range current {
			prev[reason] = cur
		}
		t.state[peerKey] = prev
		return nil
	}

	deltas := make(map[string]uint64, len(current))
	for reason, cur := range current {
		p := prev[reason]
		var d uint64
		if cur < p {
			// Counter reset on the engine side (pod restart). Treat the
			// whole current count as the delta — these are observations
			// that happened after the reset and were not previously seen.
			d = cur
		} else {
			d = cur - p
		}
		if d > 0 {
			deltas[reason] = d
		}
		prev[reason] = cur
	}
	return deltas
}

// clear drops all state for a peer. Called on agent disconnect so the next
// reconnection starts with a fresh baseline.
func (t *engineFinishReasonTracker) clear(peerKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.state, peerKey)
}
