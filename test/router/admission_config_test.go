// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for the cross-config invariant that a serverless key's input
// token bucket must cover one maximum-size prompt. Exercises the exported
// router.ValidateAdmissionInvariants over the public policy/auth types.
package router_test

import (
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/router"
)

func serverlessPolicy(models []string, maxInput int) *policy.Policy {
	return &policy.Policy{Models: models, Mode: policy.ModeServerless, MaxInputTokens: maxInput}
}

func keyWithITPM(models []string, itpm int) auth.APIKeyEntry {
	return auth.APIKeyEntry{
		KeyPreview: "sk-...X",
		Models:     models,
		Quota:      auth.QuotaConfig{InputTokensPerMinute: itpm},
	}
}

func TestValidateAdmissionInvariants_RejectsITPMBelowMaxInput(t *testing.T) {
	policies := []*policy.Policy{serverlessPolicy([]string{"gemma-4-31b"}, 262_144)}
	keys := []auth.APIKeyEntry{keyWithITPM([]string{"gemma-4-31b"}, 100)}
	if err := router.ValidateAdmissionInvariants(policies, keys); err == nil {
		t.Fatal("expected error for an input bucket below max_input_tokens, got nil")
	}
}

func TestValidateAdmissionInvariants_Pass(t *testing.T) {
	policies := []*policy.Policy{serverlessPolicy([]string{"gemma-4-31b"}, 262_144)}
	keys := []auth.APIKeyEntry{keyWithITPM([]string{"gemma-4-31b"}, 519_540)}
	if err := router.ValidateAdmissionInvariants(policies, keys); err != nil {
		t.Errorf("expected nil for a bucket covering max_input, got %v", err)
	}
}

func TestValidateAdmissionInvariants_ReservedModeSkipped(t *testing.T) {
	// Same undersized bucket, but a reserved policy has no per-key caps.
	reserved := &policy.Policy{Models: []string{"gemma-4-31b"}, Mode: policy.ModeReserved, MaxInputTokens: 262_144}
	keys := []auth.APIKeyEntry{keyWithITPM([]string{"gemma-4-31b"}, 100)}
	if err := router.ValidateAdmissionInvariants([]*policy.Policy{reserved}, keys); err != nil {
		t.Errorf("reserved policy must skip the input-bucket check, got %v", err)
	}
}

func TestValidateAdmissionInvariants_UnsetBucketSkipped(t *testing.T) {
	// A zero bucket means unset; nothing to check yet.
	policies := []*policy.Policy{serverlessPolicy([]string{"gemma-4-31b"}, 262_144)}
	keys := []auth.APIKeyEntry{keyWithITPM([]string{"gemma-4-31b"}, 0)}
	if err := router.ValidateAdmissionInvariants(policies, keys); err != nil {
		t.Errorf("an unset bucket must be skipped, got %v", err)
	}
}

// TestValidateAdmissionInvariants_Reachability drives the key→policy reach logic
// through the public entrypoint: an undersized bucket errors iff the key can
// actually reach the serverless policy's models.
func TestValidateAdmissionInvariants_Reachability(t *testing.T) {
	cases := []struct {
		name      string
		keyModels []string
		polModels []string
		wantErr   bool
	}{
		{"both-unrestricted", nil, nil, true},
		{"key-unrestricted", nil, []string{"a"}, true},
		{"policy-global", []string{"a"}, nil, true},
		{"intersect", []string{"a", "b"}, []string{"b", "c"}, true},
		{"disjoint", []string{"a"}, []string{"b"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			policies := []*policy.Policy{serverlessPolicy(c.polModels, 262_144)}
			keys := []auth.APIKeyEntry{keyWithITPM(c.keyModels, 100)}
			err := router.ValidateAdmissionInvariants(policies, keys)
			if (err != nil) != c.wantErr {
				t.Errorf("err=%v, wantErr=%v", err, c.wantErr)
			}
		})
	}
}
