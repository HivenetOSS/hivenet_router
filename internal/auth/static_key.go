// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
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
	keys    map[string]keyEntry // SHA-256 hex → entry
	tenants []string            // every owner, once
}

// NewStaticKeyProvider creates a StaticKeyProvider from a list of APIKeyEntry.
// The key_hash field in each entry must already be the SHA-256 hex of the real key
// (as produced by "hivenet-router keygen"). Incoming bearer tokens are hashed at
// request time and compared to these stored hashes.
//
// Validation at startup:
//   - key_hash must be a SHA-256 hex digest (64 hex characters, either case;
//     stored lowercase, as presented keys are hashed)
//   - metadata.owner must be non-empty (it becomes TenantID), a safe
//     identifier, and not a tenant the router assigns itself
//   - metadata.name must be non-empty (required for audit/log identification)
//   - key_hash values must be unique across all entries
//   - expires_at, if set, must be a valid "DD-MM-YYYY" date
func NewStaticKeyProvider(entries []APIKeyEntry) (*StaticKeyProvider, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("auth: api-key mode requires at least one key entry")
	}
	keys := make(map[string]keyEntry, len(entries))
	ids := make(map[string]int, len(entries)) // derived key ID → entry index
	for i, e := range entries {
		label := fmt.Sprintf("key entry %d (%q)", i, e.KeyPreview)
		hash, err := normalizeKeyHash(label, e.KeyHash)
		if err != nil {
			return nil, err
		}
		if e.Metadata.Owner == "" {
			return nil, fmt.Errorf("auth: key entry %d (%q): metadata.owner must not be empty", i, e.KeyPreview)
		}
		if err := validateOwner(label, "metadata.owner", e.Metadata.Owner); err != nil {
			return nil, err
		}
		if e.Metadata.Name == "" {
			return nil, fmt.Errorf("auth: key entry %d (%q): metadata.name must not be empty", i, e.KeyPreview)
		}
		if _, dup := keys[hash]; dup {
			return nil, fmt.Errorf("auth: duplicate key_hash in entry %d (%q)", i, e.KeyPreview)
		}
		keyID := staticKeyID(hash)
		if other, dup := ids[keyID]; dup {
			return nil, fmt.Errorf("auth: key entries %d and %d derive the same key ID %q", other, i, keyID)
		}
		ids[keyID] = i
		expiresAt, err := ParseExpiresAt(e.Metadata.ExpiresAt)
		if err != nil {
			return nil, fmt.Errorf("auth: key entry %d (%q): %w", i, e.KeyPreview, err)
		}
		quota, err := e.Quota.Validate(label)
		if err != nil {
			return nil, err
		}
		if err := validateOccupancyShare(label, e.MaxOccupancyShare); err != nil {
			return nil, err
		}
		quota.MaxOccupancyShare = e.MaxOccupancyShare
		if err := validateKeyScope(label, e.ScopeMax, e.HomeCountry); err != nil {
			return nil, err
		}
		keys[hash] = keyEntry{
			result: AuthResult{
				TenantID:          e.Metadata.Owner,
				KeyID:             keyID,
				KeyPreview:        e.KeyPreview,
				AllowedModels:     e.Models,
				RoutingStrategies: e.RoutingStrategies,
				ScopeMax:          e.ScopeMax,
				HomeCountry:       e.HomeCountry,
				QuotaLimits:       quota,
			},
			expiresAt: expiresAt,
		}
	}
	// Each key's result carries its tenant's daily budget: the lowest value
	// among the tenant's keys.
	byTenant := make(map[string][]QuotaLimits)
	for _, e := range keys {
		byTenant[e.result.TenantID] = append(byTenant[e.result.TenantID], e.result.QuotaLimits)
	}
	budgets := tenantBudgets(byTenant)
	for hash, e := range keys {
		e.result.QuotaLimits.TenantTokensPerDay = budgets[e.result.TenantID]
		keys[hash] = e
	}
	tenants := make([]string, 0, len(budgets))
	for t := range budgets {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)
	return &StaticKeyProvider{keys: keys, tenants: tenants}, nil
}

// KeysWithoutModelAllowlist returns how many keys have neither a model list
// (models) nor per-model quotas: keys that may use every model, or none when
// an allowlist is required.
func (s *StaticKeyProvider) KeysWithoutModelAllowlist() int {
	n := 0
	for _, e := range s.keys {
		if len(e.result.AllowedModels) == 0 && e.result.QuotaLimits.PerModel == nil {
			n++
		}
	}
	return n
}

// TenantIDs returns every tenant with a key. Cheaper than Tenants: no
// ceilings are computed.
func (s *StaticKeyProvider) TenantIDs() []string {
	return append([]string(nil), s.tenants...)
}

// keyIDDomain separates derived key IDs from the key hash: the ID is a hash
// of the key hash under this label, not a prefix of it, so an ID seen in a
// metric label or log line cannot be matched against a key_hash stored
// elsewhere.
const keyIDDomain = "hivenet-router/key-id/v1\x00"

// derivedKeyID returns prefix + 16 hex characters (64 bits) of
// SHA-256(keyIDDomain + prefix + NUL + lowercase key hash). It is stable: the
// same key always gets the same ID, across restarts and across routers. The
// prefix is hashed too, so one token listed both as an admin key and in
// auth.yaml gets two unrelated IDs that do not link the two in logs.
//
// Like anything derived from the key alone, the ID lets someone who sees it
// test guesses of a weak, hand-picked key offline. Generated keys
// ("hivenet-router keygen") have far too much entropy for that.
func derivedKeyID(prefix, keyHash string) string {
	h := sha256.Sum256([]byte(keyIDDomain + prefix + "\x00" + strings.ToLower(keyHash)))
	return prefix + hex.EncodeToString(h[:8])
}

// staticKeyID derives the key ID of a static auth.yaml key ("static-" + 16
// hex). Static keys have no upstream ID, and without one every static key
// would share one identity in per-key counters, metric labels and audit lines.
func staticKeyID(keyHash string) string {
	return derivedKeyID(staticKeyIDPrefix, keyHash)
}

// Tenants returns every tenant's total ceiling: the limits of its keys
// summed (see tenantCeilings). Used at startup and after SIGHUP to
// pre-populate the per-tenant Prometheus quota gauges so the Grafana
// $tenant_id variable populates within the first scrape. Enforcement is per
// key and never reads this.
func (s *StaticKeyProvider) Tenants() map[string]QuotaLimits {
	byTenant := make(map[string][]QuotaLimits)
	for _, e := range s.keys {
		byTenant[e.result.TenantID] = append(byTenant[e.result.TenantID], e.result.QuotaLimits)
	}
	out := make(map[string]QuotaLimits, len(byTenant))
	for tenant, keys := range byTenant {
		out[tenant] = tenantCeilings(keys)
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
// if the hash is not in the set. On success, TenantID is always AdminTenantID and
// KeyID identifies which admin key was used.
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
	// KeyID tells admin keys apart in the audit log ("admin-" + 16 hex,
	// derived like a static API key's ID).
	return &AuthResult{TenantID: AdminTenantID, KeyID: derivedKeyID(adminKeyIDPrefix, hash)}, nil
}
