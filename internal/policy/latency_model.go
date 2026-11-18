// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"sync"

	"hivenet_router/internal/domain"
)

// The prefix-aware cost turns tokens into seconds with two numbers: the
// prefill speed (tokens per second) and the time one decode step takes. They
// depend on the model, the GPU, the quantization and the engine, so they are
// learned, never configured: every agent reports its engine's figures over
// its last minute (prefill tokens computed / prefill seconds, decode seconds
// / decode tokens), and the router pools them across the replicas of a
// group — same model, engine and GPU, i.e. the same variant, which the
// replicas behind one router normally all are.
//
// Pooling is what keeps a replica from being judged on its own past: the
// group's figure comes from every replica that served traffic recently, not
// from the one the router picked, so a replica the router avoided still gets
// the same figure as its peers. A group no replica reported for keeps its
// last figure (it describes the hardware, not a moment); a group never
// reported for uses the defaults.

// Learned figures are kept within these bounds, so one odd report cannot
// make the cost meaningless.
const (
	minLearnedPrefillRate = 100.0
	maxLearnedPrefillRate = 10_000_000.0
	minLearnedDecodeStep  = 0.0005 // seconds
	maxLearnedDecodeStep  = 2.0    // seconds
)

// latencyGroup identifies replicas expected to run at the same speed. The
// agent fills the GPU from NVML when --gpu-model is unset and its GPUs are
// all one model. A replica whose GPU is still unknown is pooled with the
// others of its model and engine (warned once per group): most sites run one
// GPU model per model, and pooling is what keeps a replica from being judged
// on its own past.
type latencyGroup struct{ model, engine, gpu string }

func groupOf(a *domain.Agent) latencyGroup {
	return latencyGroup{model: a.Metadata.Model, engine: a.Metadata.Engine, gpu: a.Metadata.GPUModel}
}

// latencyFigures are a group's prefill speed and decode step; zero = unknown.
type latencyFigures struct {
	prefillRate float64 // tokens per second
	decodeStep  float64 // seconds
}

// LatencyMetrics observes the learned figures. Implemented by
// *metrics.RouterMetrics; may be nil.
type LatencyMetrics interface {
	LatencyModel(model, engine, gpu string, prefillRate, decodeStep float64)
}

// latencyModel holds each group's last pooled figures.
type latencyModel struct {
	mu      sync.Mutex
	last    map[latencyGroup]latencyFigures
	metrics LatencyMetrics
	warned  map[latencyGroup]bool // groups with no GPU, warned about once
}

// learn pools the candidates' reports per group, weighted by the prefill
// time (prefill) or the tokens (decode) each report covers, and returns
// every candidate's group figures. A group with no report now keeps its last
// figures.
func (m *latencyModel) learn(candidates []ScoredCandidate) map[latencyGroup]latencyFigures {
	type acc struct{ pw, p, dw, d float64 }
	sums := map[latencyGroup]*acc{}
	for _, c := range candidates {
		g := groupOf(c.Agent)
		a := sums[g]
		if a == nil {
			a = &acc{}
			sums[g] = a
		}
		s := c.Snapshot
		if s.PrefillTokensPerSecond != nil && *s.PrefillTokensPerSecond > 0 && s.PrefillSeconds > 0 {
			// Weighted by prefill time: the pooled speed is total tokens over
			// total seconds across the group.
			a.pw += s.PrefillSeconds
			a.p += s.PrefillSeconds * *s.PrefillTokensPerSecond
		}
		if s.DecodeStepSeconds != nil && *s.DecodeStepSeconds > 0 && s.DecodeSamples > 0 {
			a.dw += s.DecodeSamples
			a.d += s.DecodeSamples * *s.DecodeStepSeconds
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last == nil {
		m.last = map[latencyGroup]latencyFigures{}
	}
	out := make(map[latencyGroup]latencyFigures, len(sums))
	for g, a := range sums {
		f := m.last[g]
		changed := false
		if a.pw > 0 {
			f.prefillRate = clamp(a.p/a.pw, minLearnedPrefillRate, maxLearnedPrefillRate)
			changed = true
		}
		if a.dw > 0 {
			f.decodeStep = clamp(a.d/a.dw, minLearnedDecodeStep, maxLearnedDecodeStep)
			changed = true
		}
		if changed && g.gpu == "" && !m.warned[g] {
			if m.warned == nil {
				m.warned = map[latencyGroup]bool{}
			}
			m.warned[g] = true
			log.Warnf("prefix-aware: replicas of %s on %s report no GPU model; their learned speeds are pooled. If they run on GPUs of different models, set --gpu-model on their agents", g.model, g.engine)
		}
		if changed {
			m.last[g] = f
			if m.metrics != nil {
				m.metrics.LatencyModel(g.model, g.engine, g.gpu, f.prefillRate, f.decodeStep)
			}
		}
		out[g] = f
	}
	return out
}

func clamp(v, lo, hi float64) float64 { return min(max(v, lo), hi) }
