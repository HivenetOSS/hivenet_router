// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package tokenizer estimates prompt token counts for admission control. It
// replaces the fixed len/4 heuristic with a per-model learned ratio: the router
// admits on an estimate, then folds the exact prompt_tokens the backend reports
// back into the ratio via an EWMA, so future estimates self-calibrate to each
// model's real tokenizer without embedding one. This is the industry pattern
// ("estimate on admission, true up on actuals") and needs no per-request tokenize
// hop on the hot path.
package tokenizer

import "sync"

const (
	// coldStartCharsPerToken is the assumed density before any sample. Code
	// tokenizes denser than prose (~3.2 chars/token), so starting at 3.2 rather
	// than 4 keeps the cold estimate from under-counting coding inputs by ~20%.
	coldStartCharsPerToken = 3.2

	// defaultAlpha is the EWMA weight of each new sample. 0.2 converges within a
	// couple dozen requests while staying stable against per-request noise.
	defaultAlpha = 0.2

	// perMessageOverhead floors the estimate so a textless (image/tool-only)
	// request is still counted rather than measured as zero.
	perMessageOverhead = 4
)

// Estimator holds a per-model tokens-per-byte ratio, learned from backend usage.
// It is safe for concurrent use.
type Estimator struct {
	alpha float64
	cold  float64 // cold-start tokens per byte

	mu    sync.RWMutex
	ratio map[string]float64 // model → learned tokens-per-byte
}

// NewEstimator returns an Estimator seeded with the code-leaning cold-start
// density; each model's ratio then converges to its own measured value.
func NewEstimator() *Estimator {
	return &Estimator{
		alpha: defaultAlpha,
		cold:  1.0 / coldStartCharsPerToken,
		ratio: make(map[string]float64),
	}
}

// ratioFor returns the model's learned ratio, or the cold-start ratio if it has
// no sample yet.
func (e *Estimator) ratioFor(model string) float64 {
	e.mu.RLock()
	r, ok := e.ratio[model]
	e.mu.RUnlock()
	if ok {
		return r
	}
	return e.cold
}

// Estimate returns the estimated prompt tokens for textBytes of message content
// (plus any system prompt) across messageCount messages, using the model's
// learned tokens-per-byte ratio. The result is floored at a per-message overhead
// so a request with no estimable text is still counted.
func (e *Estimator) Estimate(model string, textBytes, messageCount int) int {
	est := int(float64(textBytes) * e.ratioFor(model))
	if floor := messageCount * perMessageOverhead; est < floor {
		est = floor
	}
	return est
}

// Observe folds an exact prompt_tokens (from a backend usage report) for a
// request of textBytes into the model's ratio via EWMA. The first sample blends
// with the cold-start ratio, so the estimate moves off cold-start gradually
// rather than snapping to one noisy request. Non-positive samples are ignored.
func (e *Estimator) Observe(model string, textBytes, promptTokens int) {
	if textBytes <= 0 || promptTokens <= 0 {
		return
	}
	obs := float64(promptTokens) / float64(textBytes)
	e.mu.Lock()
	cur, ok := e.ratio[model]
	if !ok {
		cur = e.cold
	}
	e.ratio[model] = e.alpha*obs + (1-e.alpha)*cur
	e.mu.Unlock()
}

// RatioFor exposes the current tokens-per-byte ratio for a model, for tests and
// diagnostics.
func (e *Estimator) RatioFor(model string) float64 { return e.ratioFor(model) }
