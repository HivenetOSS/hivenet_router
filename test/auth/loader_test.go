// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/auth"
)

// intPtr / mkEntry exist so each test can declare the YAML loader's pointer
// fields inline without a helper var.
func intPtr(v int) *int { return &v }

func mkEntry(rpm, tpd *int) *auth.PerModelQuotaConfig {
	return &auth.PerModelQuotaConfig{RequestsPerMinutePerReplica: rpm, TokensPerDay: tpd}
}

// TestQuotaConfig_Validate_LegacyFlatPassthrough verifies the back-compat
// path: a YAML block with only the flat fields produces a QuotaLimits whose
// PerModel is nil — the middleware then takes the legacy code path.
func TestQuotaConfig_Validate_LegacyFlatPassthrough(t *testing.T) {
	q, err := auth.QuotaConfig{RequestsPerMinute: 100, TokensPerDay: 5_000_000}.Validate("test")
	if err != nil {
		t.Fatalf("flat shape must validate: %v", err)
	}
	if q.PerModel != nil {
		t.Fatalf("flat shape must leave PerModel nil so callers branch into legacy path")
	}
	if q.RequestsPerMinute != 100 || q.TokensPerDay != 5_000_000 {
		t.Fatalf("flat passthrough lost values: %+v", q)
	}
}

// TestQuotaConfig_Validate_PerModelHappyPath verifies a valid per-model block
// is converted into the runtime shape with both knobs preserved per entry.
func TestQuotaConfig_Validate_PerModelHappyPath(t *testing.T) {
	q, err := auth.QuotaConfig{
		PerModel: map[string]*auth.PerModelQuotaConfig{
			"Qwen/Qwen3.6-27B": mkEntry(intPtr(10), intPtr(3_000_000)),
			"Qwen/Qwen3.6-35B": mkEntry(intPtr(5), intPtr(1_000_000)),
		},
	}.Validate("test")
	if err != nil {
		t.Fatalf("valid per_model block must validate: %v", err)
	}
	if q.PerModel == nil {
		t.Fatalf("per_model shape must populate PerModel")
	}
	if got := q.PerModel["Qwen/Qwen3.6-27B"]; got.RequestsPerMinutePerReplica != 10 || got.TokensPerDay != 3_000_000 {
		t.Fatalf("per_model entry not preserved: %+v", got)
	}
}

// TestQuotaConfig_Validate_RejectsMixedShape verifies the loader refuses a key
// that declares BOTH legacy flat values and per_model — there is no defined
// precedence and the operator must pick one.
func TestQuotaConfig_Validate_RejectsMixedShape(t *testing.T) {
	_, err := auth.QuotaConfig{
		RequestsPerMinute: 100,
		PerModel: map[string]*auth.PerModelQuotaConfig{
			"Qwen/Qwen3.6-27B": mkEntry(intPtr(10), intPtr(3_000_000)),
		},
	}.Validate("test")
	if err == nil || !strings.Contains(err.Error(), "use one shape") {
		t.Fatalf("mixed flat+per_model must be rejected; got err=%v", err)
	}
}

// TestQuotaConfig_Validate_RejectsEmptyPerModel verifies an empty per_model
// block is rejected — the operator either declares entries or removes the
// per_model key entirely.
func TestQuotaConfig_Validate_RejectsEmptyPerModel(t *testing.T) {
	_, err := auth.QuotaConfig{
		PerModel: map[string]*auth.PerModelQuotaConfig{},
	}.Validate("test")
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty per_model must be rejected; got err=%v", err)
	}
}

// TestQuotaConfig_Validate_RejectsPartialEntry verifies the "forgot a field"
// guard — a per_model entry missing requests_per_minute_per_replica OR
// tokens_per_day must fail at load. This is the central reason both YAML
// fields are pointer types in the loader.
func TestQuotaConfig_Validate_RejectsPartialEntry(t *testing.T) {
	// Missing requests_per_minute_per_replica.
	_, err := auth.QuotaConfig{
		PerModel: map[string]*auth.PerModelQuotaConfig{
			"Qwen/Qwen3.6-27B": mkEntry(nil, intPtr(3_000_000)),
		},
	}.Validate("test")
	if err == nil || !strings.Contains(err.Error(), "requests_per_minute_per_replica") {
		t.Fatalf("missing rpm field must be rejected with a field-pointing error; got err=%v", err)
	}

	// Missing tokens_per_day.
	_, err = auth.QuotaConfig{
		PerModel: map[string]*auth.PerModelQuotaConfig{
			"Qwen/Qwen3.6-27B": mkEntry(intPtr(10), nil),
		},
	}.Validate("test")
	if err == nil || !strings.Contains(err.Error(), "tokens_per_day") {
		t.Fatalf("missing tpd field must be rejected with a field-pointing error; got err=%v", err)
	}
}

// TestQuotaConfig_Validate_RejectsNegative verifies negative values are
// rejected — they have no meaning in either knob.
func TestQuotaConfig_Validate_RejectsNegative(t *testing.T) {
	_, err := auth.QuotaConfig{
		PerModel: map[string]*auth.PerModelQuotaConfig{
			"Qwen/Qwen3.6-27B": mkEntry(intPtr(-1), intPtr(0)),
		},
	}.Validate("test")
	if err == nil || !strings.Contains(err.Error(), ">= 0") {
		t.Fatalf("negative rpm must be rejected; got err=%v", err)
	}
}
