// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"fmt"
	"slices"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/policy"
)

// checkAdmissionConfig loads the API keys from auth.yaml (when configured for
// api-key mode) and runs ValidateAdmissionInvariants against the effective
// policies, keeping ValidateAdmissionInvariants pure and unit-testable. Other
// auth modes have no static key list, so there is nothing to cross-check.
func checkAdmissionConfig(cfg *config.Config, global *policy.Policy, named map[string]*policy.Policy) error {
	keys, err := admissionKeysFromConfig(cfg)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return ValidateAdmissionInvariants(gatherPolicies(global, named), keys)
}

// admissionKeysFromConfig returns the API keys from auth.yaml for cross-config
// validation, or nil when no static key list applies (no auth file, or a
// non-api-key mode). Used both at startup and on reload.
func admissionKeysFromConfig(cfg *config.Config) ([]auth.APIKeyEntry, error) {
	if cfg.AuthConfigFile == "" {
		return nil, nil
	}
	authCfg, err := auth.LoadAuthConfig(cfg.AuthConfigFile)
	if err != nil {
		return nil, err
	}
	if authCfg.API.Mode != auth.AuthModeAPIKey {
		return nil, nil
	}
	return authCfg.API.Keys, nil
}

// gatherPolicies flattens the global policy plus the named per-model policies
// into a single slice for ValidateAdmissionInvariants.
func gatherPolicies(global *policy.Policy, named map[string]*policy.Policy) []*policy.Policy {
	policies := make([]*policy.Policy, 0, 1+len(named))
	if global != nil {
		policies = append(policies, global)
	}
	for _, p := range named {
		policies = append(policies, p)
	}
	return policies
}

// ValidateAdmissionInvariants enforces the admission-control invariants that
// span the policy and auth configs — the ones neither loader can check alone
// because each holds only half of the pair. Today that is the input-bucket rule:
// on a serverless replica a key's input token bucket must be able to hold one
// maximum-size prompt, or it silently caps the usable context. A violation is
// returned so startup fails loudly rather than silently throttling full-context
// traffic once serving begins.
//
// keys is the raw API-key list from auth.yaml; policies is every policy whose
// replicas a key could reach (the effective global policy plus every named one).
func ValidateAdmissionInvariants(policies []*policy.Policy, keys []auth.APIKeyEntry) error {
	for _, p := range policies {
		if p == nil || !p.IsServerless() || p.MaxInputTokens <= 0 {
			// Nothing to check: reserved replicas have no per-key caps, and a
			// policy with no input cap declares no context size to cover yet.
			continue
		}
		for i, k := range keys {
			if !keyReachesPolicy(k, p) {
				continue
			}
			label := fmt.Sprintf("key entry %d (%q) on serverless models %v", i, k.KeyPreview, p.Models)
			if err := auth.ValidateITPMCoversMaxInput(label, k.Quota.InputTokensPerMinute, p.MaxInputTokens); err != nil {
				return err
			}
		}
	}
	return nil
}

// keyReachesPolicy reports whether a key could send a request to a model
// governed by policy p. A key with no Models list may call any model; a policy
// with no Models list governs every model (the global/default policy). Otherwise
// the two model sets must intersect.
func keyReachesPolicy(k auth.APIKeyEntry, p *policy.Policy) bool {
	if len(k.Models) == 0 || len(p.Models) == 0 {
		return true
	}
	for _, km := range k.Models {
		if slices.Contains(p.Models, km) {
			return true
		}
	}
	return false
}
