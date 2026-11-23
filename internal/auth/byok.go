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
	if looksLikePreview(fb.APIKey.Reveal()) {
		return fmt.Errorf("auth: %s: external_fallback.api_key is a preview as reads return it (\"abc...xyz\"), not a key: send the full key, or omit api_key to keep the stored one", label)
	}
	if scopeMax != "" && domain.Scope(scopeMax) != domain.ScopeAll {
		return fmt.Errorf("auth: %s: external_fallback sends requests outside the operator's sites; not allowed with scope_max %q", label, scopeMax)
	}
	return nil
}

// looksLikePreview reports whether v has the shape of Secret.Preview ("abc...xyz"
// or "***"): no provider key has it, and a client that read a key and wrote
// it back unchanged would otherwise store the preview as its credential.
func looksLikePreview(v string) bool {
	return v == "***" || (len(v) == 9 && v[3:6] == "...")
}

// keepStoredCredential gives e the credential stored for its key when e's
// external fallback names the same provider and leaves api_key out, or sends
// back the preview a read returned: a read-modify-write of a key keeps its
// provider credential, which reads never show.
func keepStoredCredential(e *DynamicKeyEntry, stored *DynamicKeyEntry) {
	fb := e.ExternalFallback
	if fb == nil || stored == nil || stored.ExternalFallback == nil {
		return
	}
	old := stored.ExternalFallback
	if old.Provider != fb.Provider || old.APIKey.Empty() {
		return
	}
	if v := fb.APIKey.Reveal(); v == "" || v == old.APIKey.Preview() {
		kept := *fb
		kept.APIKey = old.APIKey
		e.ExternalFallback = &kept
	}
}
