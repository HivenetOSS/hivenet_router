// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"fmt"

	"hivenet_router/internal/domain"
)

// ExternalFallback is a key's own provider account (bring your own key);
// see domain.ExternalFallback.
type ExternalFallback = domain.ExternalFallback

// Secret is a credential that never prints; see domain.Secret.
type Secret = domain.Secret

// NewSecret wraps a credential.
func NewSecret(v string) Secret { return domain.NewSecret(v) }

// externalProviders are the providers a key's external fallback may name.
var externalProviders = map[string]struct{}{"openai": {}, "anthropic": {}}

// validateExternalFallback checks a key's external fallback: a supported
// provider, a model and a credential, on a key whose requests may leave the
// country (scope_max "all"): the provider runs outside the operator's sites,
// so a key bound to a narrower scope cannot have one.
func validateExternalFallback(label string, fb *ExternalFallback, scopeMax string) error {
	if fb == nil {
		return nil
	}
	if _, ok := externalProviders[fb.Provider]; !ok {
		return fmt.Errorf("auth: %s: external_fallback.provider %q: want openai or anthropic", label, fb.Provider)
	}
	if fb.Model == "" {
		return fmt.Errorf("auth: %s: external_fallback.model is required", label)
	}
	if fb.APIKey.Empty() {
		return fmt.Errorf("auth: %s: external_fallback.api_key is required (the client's own provider key)", label)
	}
	if scopeMax != "" && domain.Scope(scopeMax) != domain.ScopeAll {
		return fmt.Errorf("auth: %s: external_fallback sends requests outside the operator's sites; not allowed with scope_max %q", label, scopeMax)
	}
	return nil
}
