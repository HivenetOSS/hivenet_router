// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrStaleVersion is returned by Upsert/Delete when the incoming version string
// is not newer than the registry's current version. Callers should fetch the
// current version separately via Version() if they need to report it.
//
// Version comparison is lexicographic. The machines service must generate
// monotonically increasing strings — e.g. zero-padded sequences ("rev_00000001")
// or ISO timestamps ("2026-05-06T14:30:00Z"). The initial empty version "" is
// less than any non-empty string, so the first operation always succeeds.
var ErrStaleVersion = errors.New("auth: stale version")

// DynamicKeyEntry is one API key managed by the machines service.
// KeyHash is the SHA-256 hex of the raw bearer token — the raw key is never stored.
type DynamicKeyEntry struct {
	ID            string
	KeyHash       string // SHA-256 hex of raw bearer token (case-insensitive; stored lowercase)
	KeyPreview    string // e.g. "sk-...KJ4", for logs only
	Owner         string // → AuthResult.TenantID
	Name          string // human label, for audit logs
	Enabled       bool
	ExpiresAt     *time.Time // nil = never
	AllowedModels []string   // empty = unrestricted
	Quota         QuotaLimits
}

// KeyStore is a persistence backend for the dynamic key registry.
// Implementations must be safe for concurrent use. The registry is
// persisted on every mutation (Upsert, Delete, ReplaceAll) and restored on
// startup; a missing snapshot (fresh store) is not an error.
type KeyStore interface {
	// Load returns the persisted registry snapshot (version + entries).
	// A fresh store (entry never written) returns ("", nil, nil).
	Load() (version string, entries []DynamicKeyEntry, err error)
	// Save atomically replaces the persisted registry snapshot.
	Save(version string, entries []DynamicKeyEntry) error
	// Reset removes the persisted snapshot entirely (fresh-start escape hatch).
	Reset() error
}

// DynamicKeyProvider is a mutable in-memory API key registry.
// Keys are pushed by the machines service at runtime via admin endpoints.
// All state is lost on restart and rebuilt from the machines service —
// unless a KeyStore is wired (SetKeyStore), in which case every mutation is
// persisted and the registry is restored on startup instead.
type DynamicKeyProvider struct {
	mu       sync.RWMutex
	byHash   map[string]*DynamicKeyEntry // SHA-256 hex → entry (for Authenticate)
	byID     map[string]*DynamicKeyEntry // key ID → entry (for Upsert/Delete)
	version  string                      // opaque version string, from machines service
	onChange func()                      // optional, fires after a successful registry mutation

	// store is the persistence backend (nil = memory-only, the default).
	// Wired once before the registry serves traffic; see SetKeyStore.
	store KeyStore

	// onPersistError reports persistence failures (nil = ignored). A persist
	// failure never fails the in-memory mutation — see persist.
	onPersistError func(error)

	// persistMu serializes Snapshot+Save in persist(). Without it two
	// concurrent mutations can interleave (A snapshots S1, B mutates and
	// saves S2, A saves S1) and the disk ends up holding the OLDER snapshot
	// — a revoked key would resurrect and a minted key would be lost after a
	// restart. Holding it across both steps guarantees the last-completed
	// save reflects the latest state at its snapshot time.
	persistMu sync.Mutex
}

// NewDynamicKeyProvider creates an empty key registry.
func NewDynamicKeyProvider() *DynamicKeyProvider {
	return &DynamicKeyProvider{
		byHash: make(map[string]*DynamicKeyEntry),
		byID:   make(map[string]*DynamicKeyEntry),
	}
}

// SetKeyStore wires a persistence backend into the registry. Must be called
// once before the registry serves traffic (i.e. before the admin endpoints
// are exposed); wiring a second store later is a programming error. A nil
// store (the default) keeps the registry memory-only.
func (r *DynamicKeyProvider) SetKeyStore(s KeyStore) { r.store = s }

// SetOnPersistError registers the sink for persistence failures. Persistence
// failures never fail an in-memory mutation (the registry remains the
// serving source of truth); they are only reported through this hook, e.g.
// to a log line.
func (r *DynamicKeyProvider) SetOnPersistError(fn func(error)) { r.onPersistError = fn }

// LoadKeyStore restores the persisted registry into this provider. Returns
// nil when no store is wired or nothing is persisted. Errors (backend
// failure, corrupt snapshot) are returned so the caller can fail startup
// closed — a persistent registry that cannot be read is worse than no
// router; on error the provider is left in its previous (empty) state.
func (r *DynamicKeyProvider) LoadKeyStore() error {
	if r.store == nil {
		return nil
	}
	ver, entries, err := r.store.Load()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		// Restore the version even when the persisted registry is empty (all
		// keys deleted): without this a stale push with any version below the
		// persisted one would be accepted after a restart, weakening the
		// monotonic-version guarantee across restarts.
		if ver != "" {
			r.mu.Lock()
			if ver > r.version {
				r.version = ver
			}
			r.mu.Unlock()
		}
		return nil
	}
	// Re-validate through the normal mutation path (field rules, duplicate
	// checks); the snapshot was produced by Snapshot, so this is a formality.
	return r.ReplaceAll(ver, entries)
}

// Snapshot returns a copy of the current entries (deterministic ID order)
// plus the registry version.
func (r *DynamicKeyProvider) Snapshot() (string, []DynamicKeyEntry) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]DynamicKeyEntry, 0, len(r.byID))
	for _, e := range r.byID {
		cp := *e
		cp.AllowedModels = append([]string(nil), e.AllowedModels...)
		entries = append(entries, cp)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return r.version, entries
}

// persist writes the current registry state to the wired key store. No-op in
// memory mode. A failure is reported via onPersistError (if set) and does not
// affect the in-memory state: the last good snapshot stays on disk, and the
// next mutation retries.
func (r *DynamicKeyProvider) persist() {
	if r.store == nil {
		return
	}
	r.persistMu.Lock()
	defer r.persistMu.Unlock()
	ver, entries := r.Snapshot()
	if err := r.store.Save(ver, entries); err != nil && r.onPersistError != nil {
		r.onPersistError(err)
	}
}

// SetOnChange registers a callback fired after every successful registry
// mutation (Upsert, Delete, ReplaceAll). Used by the router to refresh
// per-tenant Prometheus quota gauges. Safe to call once during wiring before
// the provider is exposed; not safe to swap concurrently with mutations.
// Callback runs synchronously after the lock is released.
func (r *DynamicKeyProvider) SetOnChange(fn func()) {
	r.onChange = fn
}

// Authenticate extracts the bearer token, SHA-256-hashes it, and looks it up
// in the registry (O(1)). Checks Enabled and expiry.
// Returns ErrMissingCredentials if no token is present.
// Returns ErrInvalidCredentials if the hash is not found, disabled, or expired.
func (r *DynamicKeyProvider) Authenticate(req *http.Request) (*AuthResult, error) {
	token := extractBearerToken(req)
	if token == "" {
		return nil, ErrMissingCredentials
	}
	h := sha256.Sum256([]byte(token))
	hash := hex.EncodeToString(h[:])

	r.mu.RLock()
	entry, ok := r.byHash[hash]
	r.mu.RUnlock()

	if !ok {
		return nil, ErrInvalidCredentials
	}
	if !entry.Enabled {
		return nil, ErrInvalidCredentials
	}
	if entry.ExpiresAt != nil && !time.Now().UTC().Before(*entry.ExpiresAt) {
		return nil, ErrInvalidCredentials
	}

	return &AuthResult{
		TenantID:      entry.Owner,
		KeyID:         entry.ID,
		KeyPreview:    entry.KeyPreview,
		AllowedModels: entry.AllowedModels,
		QuotaLimits:   entry.Quota,
	}, nil
}

// normalizeAndValidateKeyEntry lowercases KeyHash, then checks required fields
// and hex format. Mutates the entry in place. Returns a descriptive error if any
// field is invalid. SHA-256 hashes are case-insensitive — accepting uppercase
// avoids spurious 400s when callers use stdlib formatters that emit uppercase.
func normalizeAndValidateKeyEntry(e *DynamicKeyEntry) error {
	if e.ID == "" {
		return fmt.Errorf("auth: key entry: id must not be empty")
	}
	if e.KeyHash == "" {
		return fmt.Errorf("auth: key entry %q: key_hash must not be empty", e.ID)
	}
	e.KeyHash = strings.ToLower(e.KeyHash)
	if len(e.KeyHash) != 64 {
		return fmt.Errorf("auth: key entry %q: key_hash must be 64 hex characters (SHA-256), got %d", e.ID, len(e.KeyHash))
	}
	for _, c := range e.KeyHash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("auth: key entry %q: key_hash contains non-hex character %q", e.ID, c)
		}
	}
	if e.Owner == "" {
		return fmt.Errorf("auth: key entry %q: owner must not be empty", e.ID)
	}
	if e.Name == "" {
		return fmt.Errorf("auth: key entry %q: name must not be empty", e.ID)
	}
	// The same quota bounds static keys get at load (static_key.go):
	// max_occupancy_share must be in (0, 1] (0 means unset) — above 1 one key
	// could out-reserve the whole pool — and negative per-minute buckets are
	// nonsense. Enforced here — not only in the HTTP DTO layer — so every
	// registry mutation path is covered.
	if err := validateOccupancyShare(fmt.Sprintf("key entry %q", e.ID), e.Quota.MaxOccupancyShare); err != nil {
		return err
	}
	if e.Quota.InputTokensPerMinute < 0 || e.Quota.OutputTokensPerMinute < 0 {
		return fmt.Errorf("auth: key entry %q: input_tokens_per_minute and output_tokens_per_minute must be >= 0", e.ID)
	}
	// An empty non-nil per-model block is invalid (matching the auth.yaml
	// loader, which rejects it at load): accepting it would let a key flip
	// from per-model mode to legacy flat mode after a restart, when the
	// persisted (omitted) map comes back as nil.
	if e.Quota.PerModel != nil && len(e.Quota.PerModel) == 0 {
		return fmt.Errorf("auth: key entry %q: quota per_model must not be an empty object", e.ID)
	}
	return nil
}

// Upsert adds or updates a key entry. Rejects if version < current version.
// Equal version is accepted as a no-op replay (idempotent at-least-once delivery).
// Validates required fields and KeyHash format (same rules as StaticKeyProvider).
func (r *DynamicKeyProvider) Upsert(version string, e DynamicKeyEntry) error {
	if err := normalizeAndValidateKeyEntry(&e); err != nil {
		return err
	}
	r.mu.Lock()
	if version < r.version {
		r.mu.Unlock()
		return ErrStaleVersion
	}

	// Reject key_hash collisions across different IDs. Allowing two IDs to
	// share a hash desynchronizes byHash/byID: the second Upsert overwrites
	// byHash silently, and a later Delete on either ID removes the lookup
	// entry the other key relies on. Force the operator to delete the old ID
	// first.
	if existing, ok := r.byHash[e.KeyHash]; ok && existing.ID != e.ID {
		r.mu.Unlock()
		return fmt.Errorf("auth: key_hash already registered to id %q — delete that key before assigning the same hash to id %q", existing.ID, e.ID)
	}

	entry := &DynamicKeyEntry{
		ID:            e.ID,
		KeyHash:       e.KeyHash,
		KeyPreview:    e.KeyPreview,
		Owner:         e.Owner,
		Name:          e.Name,
		Enabled:       e.Enabled,
		ExpiresAt:     e.ExpiresAt,
		AllowedModels: e.AllowedModels,
		Quota:         e.Quota,
	}

	// Rotation: same ID, new hash — drop the previous hash mapping so it
	// stops authenticating. Safe because the collision check above guarantees
	// byHash[old.KeyHash] still points to this ID.
	if old, ok := r.byID[e.ID]; ok && old.KeyHash != e.KeyHash {
		delete(r.byHash, old.KeyHash)
	}

	r.byHash[e.KeyHash] = entry
	r.byID[e.ID] = entry
	r.version = version
	r.mu.Unlock()

	if r.onChange != nil {
		r.onChange()
	}
	r.persist()
	return nil
}

// Delete removes a key by ID. Rejects if version < current version.
// Equal version is accepted as a no-op replay (idempotent at-least-once delivery).
// Idempotent — returns nil even if the key does not exist.
func (r *DynamicKeyProvider) Delete(version string, keyID string) error {
	if keyID == "" {
		return fmt.Errorf("auth: delete: keyID must not be empty")
	}
	r.mu.Lock()
	if version < r.version {
		r.mu.Unlock()
		return ErrStaleVersion
	}

	if entry, ok := r.byID[keyID]; ok {
		// Only remove the byHash entry when it still points to THIS key by
		// pointer identity. Defends against any future regression that lets
		// two IDs share a hash; today Upsert prevents that.
		if cur, exists := r.byHash[entry.KeyHash]; exists && cur == entry {
			delete(r.byHash, entry.KeyHash)
		}
		delete(r.byID, keyID)
	}

	r.version = version
	r.mu.Unlock()

	if r.onChange != nil {
		r.onChange()
	}
	r.persist()
	return nil
}

// checkDuplicates returns an error if any two entries share the same ID or hash.
func checkDuplicates(entries []DynamicKeyEntry) error {
	seenIDs := make(map[string]int, len(entries))
	seenHashes := make(map[string]int, len(entries))
	for i, e := range entries {
		if j, ok := seenIDs[e.ID]; ok {
			return fmt.Errorf("auth: key[%d]: duplicate ID %q (first seen at index %d)", i, e.ID, j)
		}
		seenIDs[e.ID] = i
		if j, ok := seenHashes[e.KeyHash]; ok {
			return fmt.Errorf("auth: key[%d]: duplicate key_hash (first seen at index %d)", i, j)
		}
		seenHashes[e.KeyHash] = i
	}
	return nil
}

// ReplaceAll atomically swaps the entire registry. Rejects if version < current
// version; equal version is accepted as a no-op replay (idempotent at-least-once
// delivery). Works for bootstrap (r.version == "") because any non-empty version
// satisfies the strict-less check.
// Validates all entries and rejects duplicates before replacing; partial
// updates are not applied. Each entry is copied into a fresh struct so the
// registry is independent of the caller's slice.
func (r *DynamicKeyProvider) ReplaceAll(version string, entries []DynamicKeyEntry) error {
	for i := range entries {
		if err := normalizeAndValidateKeyEntry(&entries[i]); err != nil {
			return fmt.Errorf("auth: key[%d]: %w", i, err)
		}
	}
	if err := checkDuplicates(entries); err != nil {
		return err
	}
	byHash := make(map[string]*DynamicKeyEntry, len(entries))
	byID := make(map[string]*DynamicKeyEntry, len(entries))
	for i := range entries {
		entry := &DynamicKeyEntry{
			ID:            entries[i].ID,
			KeyHash:       entries[i].KeyHash,
			KeyPreview:    entries[i].KeyPreview,
			Owner:         entries[i].Owner,
			Name:          entries[i].Name,
			Enabled:       entries[i].Enabled,
			ExpiresAt:     entries[i].ExpiresAt,
			AllowedModels: entries[i].AllowedModels,
			Quota:         entries[i].Quota,
		}
		byHash[entry.KeyHash] = entry
		byID[entry.ID] = entry
	}

	r.mu.Lock()
	if version < r.version {
		r.mu.Unlock()
		return ErrStaleVersion
	}
	r.byHash = byHash
	r.byID = byID
	r.version = version
	r.mu.Unlock()

	if r.onChange != nil {
		r.onChange()
	}
	r.persist()
	return nil
}

// Version returns the current registry version string and key count.
func (r *DynamicKeyProvider) Version() (string, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version, len(r.byID)
}

// ListKeys returns all active key entries without key hashes (for operator
// visibility and drift detection). The returned slice is a copy — safe to
// iterate without holding the lock.
func (r *DynamicKeyProvider) ListKeys() []DynamicKeyEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]DynamicKeyEntry, 0, len(r.byID))
	for _, e := range r.byID {
		entry := *e
		entry.KeyHash = "" // never expose hashes
		entries = append(entries, entry)
	}
	return entries
}

// GetKey returns a single key entry by ID without the key hash.
// Returns zero value and false if not found.
func (r *DynamicKeyProvider) GetKey(keyID string) (DynamicKeyEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byID[keyID]
	if !ok {
		return DynamicKeyEntry{}, false
	}
	entry := *e
	entry.KeyHash = "" // never expose hash
	return entry, true
}

// Tenants returns the deduped per-tenant quota limits, used by the router to
// pre-populate Prometheus quota gauges so the Grafana $tenant_id variable is
// populated within the first scrape. Mirrors StaticKeyProvider.Tenants().
//
// A tenant may have multiple keys with different QuotaLimits — the actual
// per-request limit is whichever quota is carried by the key the caller
// authenticates with, applied by QuotaMiddleware on every request. This
// function only seeds an initial value so the Grafana variable populates
// before any traffic arrives; the per-request gauge writes will overwrite
// it. Iterates byID in sorted order so the seeded value is deterministic
// across restarts (the alphabetically last key ID wins per tenant).
func (r *DynamicKeyProvider) Tenants() map[string]QuotaLimits {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make(map[string]QuotaLimits, len(ids))
	for _, id := range ids {
		e := r.byID[id]
		out[e.Owner] = e.Quota
	}
	return out
}
