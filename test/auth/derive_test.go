// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package auth_test exercises the serverless per-key default derivation through
// its exported surface: each of the four cap rules and every branch that can
// bind (share floor, ITPM floor vs ceiling vs share), plus the published
// gemma-4-31b reference row.
package auth_test

import (
	"math"
	"testing"

	"hivenet_router/internal/auth"
)

// --- occupancy share ---------------------------------------------------------

func TestDerive_ShareBaselineFloor(t *testing.T) {
	// max_model_len / admit_budget = 0.25 < 0.40 → the 0.40 baseline binds.
	d := auth.DeriveKeyDefaults(auth.ModelLimits{AdmitBudgetTokens: 1_000_000, MaxModelLen: 250_000})
	if d.OccupancyShare != 0.40 {
		t.Errorf("share = %v, want 0.40 (baseline floor)", d.OccupancyShare)
	}
}

func TestDerive_ShareContextFloorBinds(t *testing.T) {
	// max_model_len / admit_budget = 0.60 > 0.40 → the context ratio binds.
	d := auth.DeriveKeyDefaults(auth.ModelLimits{AdmitBudgetTokens: 1_000_000, MaxModelLen: 600_000})
	if math.Abs(d.OccupancyShare-0.60) > 1e-9 {
		t.Errorf("share = %v, want 0.60 (context-ratio floor)", d.OccupancyShare)
	}
}

// --- ITPM: the three binding branches ---------------------------------------

func TestDerive_ITPMFloorBinds(t *testing.T) {
	// share × ceiling = 0.40 × 1,000,000 = 400,000, below the floor 2×250,000.
	d := auth.DeriveKeyDefaults(auth.ModelLimits{
		AdmitBudgetTokens: 1_000_000, MaxModelLen: 100_000, // share = 0.40
		MaxInputTokens: 250_000, ITPMCeiling: 1_000_000,
	})
	if d.InputTokensPerMinute != 500_000 { // 2 × max_input_tokens
		t.Errorf("ITPM = %d, want 500000 (floor binds)", d.InputTokensPerMinute)
	}
}

func TestDerive_ITPMShareBinds(t *testing.T) {
	// floor 2×100,000=200,000 < share×ceiling 0.40×800,000=320,000 < ceiling.
	d := auth.DeriveKeyDefaults(auth.ModelLimits{
		AdmitBudgetTokens: 1_000_000, MaxModelLen: 100_000, // share = 0.40
		MaxInputTokens: 100_000, ITPMCeiling: 800_000,
	})
	if d.InputTokensPerMinute != 320_000 { // 0.40 × 800,000
		t.Errorf("ITPM = %d, want 320000 (share binds)", d.InputTokensPerMinute)
	}
}

func TestDerive_ITPMCeilingBinds(t *testing.T) {
	// floor 2×260,000=520,000 exceeds the physical ceiling 500,000 → ceiling wins.
	d := auth.DeriveKeyDefaults(auth.ModelLimits{
		AdmitBudgetTokens: 1_000_000, MaxModelLen: 100_000, // share = 0.40
		MaxInputTokens: 260_000, ITPMCeiling: 500_000,
	})
	if d.InputTokensPerMinute != 500_000 { // ceiling caps below the floor
		t.Errorf("ITPM = %d, want 500000 (ceiling binds)", d.InputTokensPerMinute)
	}
}

// --- OTPM and RPM ------------------------------------------------------------

func TestDerive_OTPMIsShareTimesCeiling(t *testing.T) {
	d := auth.DeriveKeyDefaults(auth.ModelLimits{
		AdmitBudgetTokens: 1_000_000, MaxModelLen: 100_000, // share = 0.40
		OTPMCeiling: 120_000,
	})
	if d.OutputTokensPerMinute != 48_000 { // 0.40 × 120,000, no floor
		t.Errorf("OTPM = %d, want 48000", d.OutputTokensPerMinute)
	}
}

func TestDerive_RPMIsTwiceSellable(t *testing.T) {
	d := auth.DeriveKeyDefaults(auth.ModelLimits{SellableRPM: 48})
	if d.RequestsPerMinute != 96 {
		t.Errorf("RPM = %d, want 96 (2 × sellable)", d.RequestsPerMinute)
	}
}

// --- published reference row -------------------------------------------------

// TestDerive_Gemma431bReferenceRow reproduces the fleet-table row for
// gemma-4-31b. ITPM (ceiling binds) and RPM are integer-exact; the occupancy
// share and OTPM depend on the exact certified ceilings (which live in the
// benchmark's router_limits.yaml, not this repo), so they are checked within a
// small tolerance of the published values.
func TestDerive_Gemma431bReferenceRow(t *testing.T) {
	d := auth.DeriveKeyDefaults(auth.ModelLimits{
		AdmitBudgetTokens: 408_987,
		MaxModelLen:       262_144,
		MaxInputTokens:    262_144,
		ITPMCeiling:       519_540,
		OTPMCeiling:       74_000,
		SellableRPM:       48,
	})
	if d.InputTokensPerMinute != 519_540 {
		t.Errorf("ITPM = %d, want 519540 (physics ceiling)", d.InputTokensPerMinute)
	}
	if d.RequestsPerMinute != 96 {
		t.Errorf("RPM = %d, want 96", d.RequestsPerMinute)
	}
	if math.Abs(d.OccupancyShare-0.64) > 0.01 {
		t.Errorf("share = %v, want ~0.64", d.OccupancyShare)
	}
	if math.Abs(float64(d.OutputTokensPerMinute-47_424)) > 500 {
		t.Errorf("OTPM = %d, want ~47424", d.OutputTokensPerMinute)
	}
}
