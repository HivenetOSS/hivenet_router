// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

// ModelLimits holds the per-model numbers the serverless per-key defaults are
// derived from — the values a benchmark campaign certifies for a model
// (router_limits.yaml). Deriving from these, rather than copying a frozen block
// of constants, keeps each model's caps tied to its own measured envelope.
type ModelLimits struct {
	AdmitBudgetTokens int // KV-occupancy admit budget B (tokens)
	MaxModelLen       int // engine max context window (tokens)
	MaxInputTokens    int // per-request input cap (tokens)
	ITPMCeiling       int // physical input-tokens/min ceiling for the SKU
	OTPMCeiling       int // physical output-tokens/min ceiling for the SKU
	SellableRPM       int // max(per_sku_reference.*.sellable_rpm): never-throttle floor
}

// KeyDefaults are the serverless per-key B4 caps derived for one model.
type KeyDefaults struct {
	OccupancyShare        float64
	InputTokensPerMinute  int
	OutputTokensPerMinute int
	RequestsPerMinute     int
}

// minOccupancyShare is the baseline per-key share: even a small model gives each
// key at least this fraction of the admit budget so per-key caps never become a
// hidden context throttle.
const minOccupancyShare = 0.40

// DeriveKeyDefaults renders the serverless per-key caps for a model from its
// certified limits, per the four rules below. The caps are intentionally
// oversubscribed (they do not sum to the box) — box safety is the occupancy
// budget and the shed gate, not the sum of per-key shares.
//
//   - occupancy share = max(max_model_len / admit_budget, 0.40). Below
//     max_model_len/admit_budget the share would silently become a context cap,
//     so that ratio is the floor; 0.40 is the baseline for larger models.
//   - ITPM = clamp(share × itpm_ceiling, 2 × max_input_tokens, itpm_ceiling).
//     The floor (2 × max_input_tokens) lets one full-context request through
//     roughly every 30 s; the ceiling caps it at the physical rate. When the
//     ceiling is below the floor, the ceiling wins (physics binds).
//   - OTPM = share × otpm_ceiling. No floor: output is metered as it is
//     generated, never reserved, so the bucket is just one minute's rate.
//   - RPM ≈ 2 × max(sellable_rpm): twice the never-throttle-legit floor, a flood
//     guard for tiny requests (the true tiny-request ceiling is unmeasured).
func DeriveKeyDefaults(m ModelLimits) KeyDefaults {
	share := minOccupancyShare
	if m.AdmitBudgetTokens > 0 {
		if floor := float64(m.MaxModelLen) / float64(m.AdmitBudgetTokens); floor > share {
			share = floor
		}
	}
	// A share is a fraction of the admit budget, so it can never exceed 1.0 (the
	// loader validates (0,1]). It reaches the ceiling only when the KV pool
	// barely holds one max-context request — a provisioning limit the derivation
	// cannot lift, but the returned value must stay valid.
	if share > 1.0 {
		share = 1.0
	}

	itpm := int(share * float64(m.ITPMCeiling))
	if floor := 2 * m.MaxInputTokens; itpm < floor {
		itpm = floor
	}
	if m.ITPMCeiling > 0 && itpm > m.ITPMCeiling {
		itpm = m.ITPMCeiling
	}

	return KeyDefaults{
		OccupancyShare:        share,
		InputTokensPerMinute:  itpm,
		OutputTokensPerMinute: int(share * float64(m.OTPMCeiling)),
		RequestsPerMinute:     2 * m.SellableRPM,
	}
}
