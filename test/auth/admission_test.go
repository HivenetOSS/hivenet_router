// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for the per-key admission fields on the auth config: the flat
// input/output token-per-minute buckets, the key-level max_occupancy_share, and
// the input-bucket-covers-context helper. Plumbing is verified through the
// exported provider surface (NewStaticKeyProvider + Tenants).
package auth_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/auth"
)

func TestQuotaConfig_FlatWithITPMOTPM(t *testing.T) {
	q := auth.QuotaConfig{
		RequestsPerMinute:     96,
		TokensPerDay:          5_000_000,
		InputTokensPerMinute:  519_540,
		OutputTokensPerMinute: 47_424,
	}
	limits, err := q.Validate("test")
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if limits.InputTokensPerMinute != 519_540 || limits.OutputTokensPerMinute != 47_424 {
		t.Errorf("input/output buckets not carried into QuotaLimits: %+v", limits)
	}
	if limits.RequestsPerMinute != 96 || limits.TokensPerDay != 5_000_000 {
		t.Errorf("existing flat fields regressed: %+v", limits)
	}
}

func TestQuotaConfig_PerModelConflictsWithITPM(t *testing.T) {
	rpm, tpd := 60, 1000
	q := auth.QuotaConfig{
		InputTokensPerMinute: 519_540, // flat field...
		PerModel: map[string]*auth.PerModelQuotaConfig{ // ...set together with per_model
			"gemma-4-31b": {RequestsPerMinutePerReplica: &rpm, TokensPerDay: &tpd},
		},
	}
	_, err := q.Validate("test")
	if err == nil {
		t.Fatal("expected error mixing per_model with a flat input bucket, got nil")
	}
	if !strings.Contains(err.Error(), "per_model") {
		t.Errorf("error = %q, want it to mention the per_model conflict", err)
	}
}

func TestQuotaConfig_NegativeITPM(t *testing.T) {
	q := auth.QuotaConfig{InputTokensPerMinute: -1}
	if _, err := q.Validate("test"); err == nil {
		t.Fatal("expected error for a negative input bucket, got nil")
	}
}

// TestQuota_RejectITPMBelowMaxInput: a serverless key's input token bucket must
// hold at least one maximum-size prompt.
func TestQuota_RejectITPMBelowMaxInput(t *testing.T) {
	const maxInput = 262_144
	cases := []struct {
		name    string
		itpm    int
		wantErr bool
	}{
		{"covers", 519_540, false},
		{"exactly-covers", maxInput, false},
		{"one-below", maxInput - 1, true},
		{"far-below", 100, true},
		{"unset-bucket-skipped", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := auth.ValidateITPMCoversMaxInput("key X", c.itpm, maxInput)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "262144") {
				// The error must name both values so the operator can fix it.
				t.Errorf("error must name max_input_tokens: %q", err)
			}
		})
	}
}

// TestQuota_ZeroMaxInputSkipped: when the policy declares no input cap yet,
// there is nothing to cover regardless of the bucket value.
func TestQuota_ZeroMaxInputSkipped(t *testing.T) {
	if err := auth.ValidateITPMCoversMaxInput("key X", 10, 0); err != nil {
		t.Errorf("expected nil when maxInput=0, got %v", err)
	}
}

// TestStaticKeyProvider_ServerlessKeyFullQuota: a serverless key with the full
// set of caps (requests/min, input+output tokens/min, tokens/day, occupancy
// share) loads and every value reaches the resolved QuotaLimits.
func TestStaticKeyProvider_ServerlessKeyFullQuota(t *testing.T) {
	entries := []auth.APIKeyEntry{{
		KeyHash:           "hashserverless",
		Metadata:          auth.KeyMetadata{Name: "some-customer", Owner: "serverless-tenant"},
		Models:            []string{"gemma-4-31b"},
		MaxOccupancyShare: 0.64,
		Quota: auth.QuotaConfig{
			RequestsPerMinute:     96,
			InputTokensPerMinute:  519_540,
			OutputTokensPerMinute: 47_424,
			TokensPerDay:          5_000_000,
		},
	}}
	p, err := auth.NewStaticKeyProvider(entries)
	if err != nil {
		t.Fatalf("NewStaticKeyProvider: %v", err)
	}
	got := p.Tenants()["serverless-tenant"]
	if got.RequestsPerMinute != 96 || got.TokensPerDay != 5_000_000 ||
		got.InputTokensPerMinute != 519_540 || got.OutputTokensPerMinute != 47_424 ||
		got.MaxOccupancyShare != 0.64 {
		t.Errorf("QuotaLimits = %+v, want RPM=96 TPD=5M ITPM=519540 OTPM=47424 share=0.64", got)
	}
}

// TestStaticKeyProvider_ReservedKeyNoQuota: a reserved-replica key carries auth
// + metadata only (no quota, no occupancy share) and must load with zero
// (unlimited) limits — the reserved product has no per-key caps.
func TestStaticKeyProvider_ReservedKeyNoQuota(t *testing.T) {
	entries := []auth.APIKeyEntry{{
		KeyHash:  "hashreserved",
		Metadata: auth.KeyMetadata{Name: "acme-reserved-r0", Owner: "reserved-tenant"},
		Models:   []string{"gemma-4-31b"},
	}}
	p, err := auth.NewStaticKeyProvider(entries)
	if err != nil {
		t.Fatalf("NewStaticKeyProvider: %v", err)
	}
	got := p.Tenants()["reserved-tenant"]
	if got.RequestsPerMinute != 0 || got.TokensPerDay != 0 ||
		got.InputTokensPerMinute != 0 || got.OutputTokensPerMinute != 0 ||
		got.MaxOccupancyShare != 0 || got.PerModel != nil {
		t.Errorf("reserved key must have zero (unlimited) limits, got %+v", got)
	}
}

// TestStaticKeyProvider_OccupancyShareRange drives the (0,1] bound through the
// exported provider: 0 (unset) and values up to 1 load; a share above 1 or
// below 0 is rejected (it would let one key reserve more than the whole box).
func TestStaticKeyProvider_OccupancyShareRange(t *testing.T) {
	cases := []struct {
		share   float64
		wantErr bool
	}{
		{0, false},
		{0.40, false},
		{0.64, false},
		{1, false},
		{1.5, true},
		{-0.1, true},
	}
	for _, c := range cases {
		entries := []auth.APIKeyEntry{{
			KeyHash:           "hashshare",
			Metadata:          auth.KeyMetadata{Name: "k", Owner: "acme"},
			MaxOccupancyShare: c.share,
		}}
		_, err := auth.NewStaticKeyProvider(entries)
		if (err != nil) != c.wantErr {
			t.Errorf("max_occupancy_share=%v: err=%v, wantErr=%v", c.share, err, c.wantErr)
		}
	}
}
