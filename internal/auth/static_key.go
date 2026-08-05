// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
)

// keyEntry is the internal map value used by StaticKeyProvider.
// It holds the pre-built AuthResult alongside an optional expiry time
// so the expiry check does not require re-parsing on every request.
type keyEntry struct {
	result    AuthResult
	expiresAt *time.Time // nil = never expires
}

// StaticKeyProvider authenticates requests by SHA-256-hashing the incoming bearer
// token and looking it up in a pre-built map keyed by SHA-256 hex strings loaded
// from auth.yaml. Plaintext keys are never retained after startup.
type StaticKeyProvider struct {
	keys map[string]keyEntry // SHA-256 hex → entry
}

// NewStaticKeyProvider creates a StaticKeyProvider from a list of APIKeyEntry.
// The key_hash field in each entry must already be the SHA-256 hex of the real key
// (as produced by "hivenet-router keygen"). Incoming bearer tokens are hashed at
// request time and compared to these stored hashes.
//
// Validation at startup:
//   - key_hash must be non-empty
//   - metadata.owner must be non-empty (it becomes TenantID)
//   - metadata.name must be non-empty (required for audit/log identification)
//   - key_hash values must be unique across all entries
//   - expires_at, if set, must be a valid "DD-MM-YYYY" date
func NewStaticKeyProvider(entries []APIKeyEntry) (*StaticKeyProvider, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("auth: api-key mode requires at least one key entry")
	}
	keys := make(map[string]keyEntry, len(entries))
	for i, e := range entries {
		if e.KeyHash == "" {
			return nil, fmt.Errorf("auth: key entry %d: key_hash must not be empty", i)
		}
		if e.Metadata.Owner == "" {
			return nil, fmt.Errorf("auth: key entry %d (%q): metadata.owner must not be empty", i, e.KeyPreview)
		}
		if e.Metadata.Name == "" {
			return nil, fmt.Errorf("auth: key entry %d (%q): metadata.name must not be empty", i, e.KeyPreview)
		}
		if _, dup := keys[e.KeyHash]; dup {
			return nil, fmt.Errorf("auth: duplicate key_hash in entry %d (%q)", i, e.KeyPreview)
		}
		expiresAt, err := ParseExpiresAt(e.Metadata.ExpiresAt)
		if err != nil {
			return nil, fmt.Errorf("auth: key entry %d (%q): %w", i, e.KeyPreview, err)
		}
		quota, err := e.Quota.Validate(fmt.Sprintf("key entry %d (%q)", i, e.KeyPreview))
		if err != nil {
			return nil, err
		}
		keys[e.KeyHash] = keyEntry{
			result: AuthResult{
				TenantID:      e.Metadata.Owner,
				KeyPreview:    e.KeyPreview,
				AllowedModels: e.Models,
				QuotaLimits:   quota,
			},
			expiresAt: expiresAt,
		}
	}
	return &StaticKeyProvider{keys: keys}, nil
}

// Tenants returns a map from tenant ID to QuotaLimits for every key entry,
// deduplicated by tenant ID. When multiple keys share an owner the last
// entry's limits win (same owner → same tier in practice).
// Used at startup and after SIGHUP to pre-populate Prometheus quota gauges
// so the Grafana $tenant_id variable populates within the first scrape.
func (s *StaticKeyProvider) Tenants() map[string]QuotaLimits {
	out := make(map[string]QuotaLimits, len(s.keys))
	for _, e := range s.keys {
		out[e.result.TenantID] = e.result.QuotaLimits
	}
	return out
}

// Authenticate extracts the bearer token, SHA-256-hashes it, and looks it up
// in the pre-built map (O(1), ~microseconds).
// Returns ErrMissingCredentials if no token is present.
// Returns ErrInvalidCredentials if the hash is not found or the key has expired.
// Expiry returns the same 401 as an invalid key — no information leak.
func (s *StaticKeyProvider) Authenticate(r *http.Request) (*AuthResult, error) {
	token := extractBearerToken(r)
	if token == "" {
		return nil, ErrMissingCredentials
	}
	h := sha256.Sum256([]byte(token))
	hash := hex.EncodeToString(h[:])
	entry, ok := s.keys[hash]
	if !ok {
		return nil, ErrInvalidCredentials
	}
	if entry.expiresAt != nil && !time.Now().UTC().Before(*entry.expiresAt) {
		return nil, ErrInvalidCredentials
	}
	result := entry.result
	return &result, nil
}

// StaticAdminKeyProvider authenticates /admin/* requests against a fixed set of
// raw keys supplied via the HIVENET_ROUTER_ADMIN_API_KEYS environment variable.
// Keys are SHA-256-hashed at startup; plaintext is not retained.
type StaticAdminKeyProvider struct {
	keys map[string]struct{} // SHA-256 hex → present
}

// NewStaticAdminKeyProvider creates a StaticAdminKeyProvider from raw key strings.
// Each key is SHA-256-hashed immediately; the plaintext slice is not stored.
// Returns an error if any key is empty or if there are duplicates.
func NewStaticAdminKeyProvider(rawKeys []string) (*StaticAdminKeyProvider, error) {
	if len(rawKeys) == 0 {
		return nil, fmt.Errorf("auth: api-key mode for admin requires at least one key")
	}
	keys := make(map[string]struct{}, len(rawKeys))
	for i, k := range rawKeys {
		if k == "" {
			return nil, fmt.Errorf("auth: admin key entry %d: key must not be empty", i)
		}
		h := sha256.Sum256([]byte(k))
		hash := hex.EncodeToString(h[:])
		if _, dup := keys[hash]; dup {
			return nil, fmt.Errorf("auth: duplicate admin key at entry %d", i)
		}
		keys[hash] = struct{}{}
	}
	return &StaticAdminKeyProvider{keys: keys}, nil
}

// Authenticate hashes the bearer token and checks membership in the admin key set.
// Returns ErrMissingCredentials if no token is present, ErrInvalidCredentials
// if the hash is not in the set. On success, TenantID is always "admin".
func (s *StaticAdminKeyProvider) Authenticate(r *http.Request) (*AuthResult, error) {
	token := extractBearerToken(r)
	if token == "" {
		return nil, ErrMissingCredentials
	}
	h := sha256.Sum256([]byte(token))
	hash := hex.EncodeToString(h[:])
	if _, ok := s.keys[hash]; !ok {
		return nil, ErrInvalidCredentials
	}
	return &AuthResult{TenantID: "admin"}, nil
}
