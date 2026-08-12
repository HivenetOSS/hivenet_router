// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"testing"

	"hivenet_router/internal/auth"
)

// TestMinute_ITPMAllowsWithinRateDeniesOver verifies the input-tokens-per-minute
// bucket admits input within the per-minute allowance and denies input that
// would exceed it.
func TestMinute_ITPMAllowsWithinRateDeniesOver(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	if !l.AllowInputTokens("k1", "m", 1000, 600) {
		t.Fatal("first 600-token request must fit a 1000/min bucket")
	}
	if l.AllowInputTokens("k1", "m", 1000, 600) {
		t.Error("second 600-token request must be denied (1200 > 1000/min)")
	}
	// A different key has its own independent bucket.
	if !l.AllowInputTokens("k2", "m", 1000, 600) {
		t.Error("a different key's bucket must be independent")
	}
}

// TestMinute_ITPMDisabledWhenZero verifies a zero limit disables the cap.
func TestMinute_ITPMDisabledWhenZero(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	for i := 0; i < 100; i++ {
		if !l.AllowInputTokens("k1", "m", 0, 1_000_000) {
			t.Fatalf("limit 0 must disable the cap, denied at i=%d", i)
		}
	}
}

// TestMinute_FullContextInputFits verifies a single full-context request fits the
// ITPM burst — the loader guarantees ITPM capacity >= max_input_tokens, so a
// legitimate max-size prompt is never throttled by its own rate bucket.
func TestMinute_FullContextInputFits(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	if !l.AllowInputTokens("k1", "m", 519_540, 262_144) {
		t.Error("a full-context request (input = max_input_tokens) must fit the ITPM burst")
	}
}

// TestMinute_TinyFloodPassesITPM verifies ITPM does NOT stop a tiny-request
// flood: thousands of 100-token requests fit a large ITPM bucket, which is why
// RPM (not ITPM) is the flood guard.
func TestMinute_TinyFloodPassesITPM(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	admitted := 0
	for i := 0; i < 1000; i++ {
		if l.AllowInputTokens("k1", "m", 520_000, 100) {
			admitted++
		}
	}
	if admitted != 1000 {
		t.Errorf("ITPM must admit a tiny-request flood (520000/100 = 5200 capacity); admitted %d/1000", admitted)
	}
}

// TestMinute_OTPMChargeDrainsAndExhausts verifies output charging drains the OTPM
// bucket and that OutputExhausted then blocks new requests, with the charge
// capped at one minute's capacity.
func TestMinute_OTPMChargeDrainsAndExhausts(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	if l.OutputExhausted("k1", "m", 1000) {
		t.Fatal("a fresh OTPM bucket must not be exhausted")
	}
	// A single huge response drains at most a full minute's capacity.
	l.ChargeOutputTokens("k1", "m", 1000, 50_000)
	if !l.OutputExhausted("k1", "m", 1000) {
		t.Error("after output over the per-minute rate, the OTPM bucket must be exhausted")
	}
	// A disabled OTPM cap is never exhausted.
	if l.OutputExhausted("k1", "m", 0) {
		t.Error("limit 0 must disable OTPM (never exhausted)")
	}
}
