// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Tests that the dynamic key registry enforces the same per-key quota bounds
// static auth.yaml keys get at load: max_occupancy_share in (0,1] and
// non-negative per-minute buckets. Enforced at the registry (not only the
// admin HTTP layer), so every mutation path is covered.
package auth_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/auth"
)

const testHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func dynEntry(share float64, itpm int) auth.DynamicKeyEntry {
	return auth.DynamicKeyEntry{
		ID:         "k1",
		KeyHash:    testHash,
		KeyPreview: "sk-...k1",
		Owner:      "acme",
		Name:       "test-key",
		Enabled:    true,
		Quota: auth.QuotaLimits{
			MaxOccupancyShare:    share,
			InputTokensPerMinute: itpm,
		},
	}
}

// TestDynamicUpsert_RejectsShareOutOfRange verifies a share above 1 (one key
// out-reserving the whole pool) or negative is rejected at Upsert, exactly as
// the static loader rejects it.
func TestDynamicUpsert_RejectsShareOutOfRange(t *testing.T) {
	for _, share := range []float64{1.5, -0.1} {
		reg := auth.NewDynamicKeyProvider()
		err := reg.Upsert("v1", dynEntry(share, 0))
		if err == nil || !strings.Contains(err.Error(), "max_occupancy_share") {
			t.Errorf("share %v: expected max_occupancy_share error, got %v", share, err)
		}
	}
}

// TestDynamicUpsert_RejectsNegativeBuckets verifies negative ITPM/OTPM values
// are rejected like the static QuotaConfig validation does.
func TestDynamicUpsert_RejectsNegativeBuckets(t *testing.T) {
	reg := auth.NewDynamicKeyProvider()
	if err := reg.Upsert("v1", dynEntry(0, -5)); err == nil {
		t.Error("expected an error for a negative input_tokens_per_minute")
	}
}

// TestDynamicUpsert_ValidShareAccepted verifies an in-range share is stored and
// surfaces on the key entry (the value the B4 occupancy gate reads at request
// time).
func TestDynamicUpsert_ValidShareAccepted(t *testing.T) {
	reg := auth.NewDynamicKeyProvider()
	if err := reg.Upsert("v1", dynEntry(0.4, 600000)); err != nil {
		t.Fatalf("valid entry rejected: %v", err)
	}
	got, ok := reg.GetKey("k1")
	if !ok {
		t.Fatal("key not found after upsert")
	}
	if got.Quota.MaxOccupancyShare != 0.4 || got.Quota.InputTokensPerMinute != 600000 {
		t.Errorf("quota not carried: %+v", got.Quota)
	}
}

// TestDynamicReplaceAll_ValidatesEveryEntry verifies ReplaceAll runs the same
// validation per entry and applies nothing on failure.
func TestDynamicReplaceAll_ValidatesEveryEntry(t *testing.T) {
	reg := auth.NewDynamicKeyProvider()
	bad := dynEntry(2.0, 0)
	bad.ID = "k2"
	bad.KeyHash = strings.Repeat("ab", 32)
	if err := reg.ReplaceAll("v1", []auth.DynamicKeyEntry{dynEntry(0.4, 0), bad}); err == nil {
		t.Fatal("expected ReplaceAll to reject the out-of-range share")
	}
	if _, n := reg.Version(); n != 0 {
		t.Errorf("a failed ReplaceAll must apply nothing, registry has %d keys", n)
	}
}
