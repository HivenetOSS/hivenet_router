// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package tokenizer_test exercises the learned per-model token estimator: the
// cold-start density, EWMA convergence, and the coding-corpus accuracy gain over
// the legacy len/4 heuristic that motivated the 0.90 admit fraction.
package tokenizer_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"hivenet_router/internal/tokenizer"
)

const model = "gemma-4-31b"

// codeDensityCharsPerToken is the density code tokenizes at (~3.2 chars/token).
// The corpus fixtures are code, so their "true" token count is bytes/3.2 — the
// value a real tokenizer would report and the target the estimator recovers.
const codeDensityCharsPerToken = 3.2

// TestColdStartDensity verifies the cold-start estimate uses ~3.2 chars/token,
// not 4 — so a code prompt is not under-counted before any sample is seen.
func TestColdStartDensity(t *testing.T) {
	e := tokenizer.NewEstimator()
	// 3200 bytes → ~1000 tokens at 3.2 chars/token, vs 800 at the old len/4.
	got := e.Estimate(model, 3200, 1)
	if got < 960 || got > 1040 {
		t.Errorf("cold-start estimate = %d, want ~1000 (3.2 chars/token), not 800 (len/4)", got)
	}
	if got <= 800 {
		t.Errorf("cold-start estimate %d must exceed the len/4 value 800 (denser code assumption)", got)
	}
}

// TestLearnedRatioConvergesPerModel verifies the EWMA moves the ratio toward the
// observed value over repeated samples, and that models are independent.
func TestLearnedRatioConvergesPerModel(t *testing.T) {
	e := tokenizer.NewEstimator()
	// Feed a true density of 2.5 chars/token (ratio 0.40) for modelA only.
	const bytes, trueTokens = 1000, 400 // 0.40 tokens/byte
	for i := 0; i < 50; i++ {
		e.Observe("modelA", bytes, trueTokens)
	}
	if r := e.RatioFor("modelA"); math.Abs(r-0.40) > 0.01 {
		t.Errorf("modelA ratio = %.4f, want ~0.40 after convergence", r)
	}
	// modelB, never observed, still sits at the cold-start ratio.
	if r := e.RatioFor("modelB"); math.Abs(r-1.0/codeDensityCharsPerToken) > 1e-9 {
		t.Errorf("unobserved modelB ratio = %.4f, want cold-start %.4f", r, 1.0/codeDensityCharsPerToken)
	}
}

// TestObserveIgnoresDegenerateSamples verifies zero/negative samples do not move
// the ratio (a missing usage report must not corrupt the estimate).
func TestObserveIgnoresDegenerateSamples(t *testing.T) {
	e := tokenizer.NewEstimator()
	before := e.RatioFor(model)
	e.Observe(model, 0, 100)
	e.Observe(model, 100, 0)
	if after := e.RatioFor(model); after != before {
		t.Errorf("degenerate samples changed the ratio: %.4f → %.4f", before, after)
	}
}

// TestCodingCorpusErrorUnderFewPercent verifies the estimator cuts the coding
// prompt-token error from the legacy len/4 heuristic's ~20% to low single digits.
func TestCodingCorpusErrorUnderFewPercent(t *testing.T) {
	corpus := loadCorpus(t)
	e := tokenizer.NewEstimator()

	var legacyErr, learnedErr float64
	for _, text := range corpus {
		bytes := len(text)
		trueTokens := int(math.Round(float64(bytes) / codeDensityCharsPerToken))

		legacy := bytes / 4 // the heuristic RL-7 replaces
		legacyErr += relErr(legacy, trueTokens)

		// The estimator learns from the actual token count, then is re-estimated.
		e.Observe(model, bytes, trueTokens)
		learnedErr += relErr(e.Estimate(model, bytes, 1), trueTokens)
	}
	legacyErr /= float64(len(corpus))
	learnedErr /= float64(len(corpus))

	if legacyErr < 0.15 {
		t.Fatalf("expected the legacy len/4 error on code to be ~20%%, got %.1f%% — check the fixture", legacyErr*100)
	}
	if learnedErr > 0.05 {
		t.Errorf("learned-estimator error on the coding corpus = %.1f%%, want < 5%%", learnedErr*100)
	}
	t.Logf("coding-corpus mean error: len/4 = %.1f%%, learned = %.1f%%", legacyErr*100, learnedErr*100)
}

func relErr(estimate, truth int) float64 {
	if truth == 0 {
		return 0
	}
	return math.Abs(float64(estimate-truth)) / float64(truth)
}

func loadCorpus(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join("testdata", "corpus")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read corpus dir: %v", err)
	}
	var corpus []string
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		corpus = append(corpus, string(b))
	}
	if len(corpus) == 0 {
		t.Fatal("coding corpus fixture is empty")
	}
	return corpus
}
