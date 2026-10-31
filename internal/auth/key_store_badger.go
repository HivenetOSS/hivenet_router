// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"encoding/json"
	"fmt"
	"time"

	"hivenet_router/internal/storage"
)

// keyStoreEntryKey is the single diskDB entry holding the persisted dynamic
// key registry. One entry (rather than one per key) makes a snapshot write
// a single atomic badger transaction — a partial write is impossible, and a
// crash mid-write leaves either the old or the new snapshot, never a mix.
const keyStoreEntryKey = storage.PrefixKeyStore + "registry"

// keyStoreSchema is the on-disk schema version of the persisted registry
// snapshot. Load refuses (fail-closed) any other version rather than
// guessing. Bump it on any keyStoreSnapshot shape change AND add a migration
// case in Load (read the old schema and convert it) — a bare refusal strands
// every existing deployment behind --reset-key-store.
const keyStoreSchema = 1

// keyStoreSnapshot is the on-disk shape of the persisted registry.
type keyStoreSnapshot struct {
	Schema  int             `json:"schema"`
	Version string          `json:"version"`
	Entries []keyStoreEntry `json:"entries"`
}

// keyStoreEntry is the explicit on-disk shape of a registry entry. The wire
// format is pinned by these tags — NOT by Go field names — so renaming a
// field in DynamicKeyEntry cannot silently zero persisted data on Unmarshal
// (the dynamic entry type has no JSON tags of its own; the admin HTTP API
// uses its own DTO).
type keyStoreEntry struct {
	ID            string        `json:"id"`
	KeyHash       string        `json:"key_hash"`
	KeyPreview    string        `json:"key_preview"`
	Owner         string        `json:"owner"`
	Name          string        `json:"name"`
	Enabled       bool          `json:"enabled"`
	ExpiresAt     *time.Time    `json:"expires_at"` // RFC3339; null = never
	AllowedModels []string      `json:"allowed_models"`
	Quota         keyStoreQuota `json:"quota"`
}

// keyStoreQuota / keyStorePerModel mirror auth.QuotaLimits /
// auth.PerModelQuotaLimits with an explicit wire shape.
type keyStoreQuota struct {
	RequestsPerMinute     int                         `json:"requests_per_minute"`
	TokensPerDay          int                         `json:"tokens_per_day"`
	InputTokensPerMinute  int                         `json:"input_tokens_per_minute"`
	OutputTokensPerMinute int                         `json:"output_tokens_per_minute"`
	MaxOccupancyShare     float64                     `json:"max_occupancy_share"`
	PerModel              map[string]keyStorePerModel `json:"per_model,omitempty"`
}

type keyStorePerModel struct {
	RequestsPerMinutePerReplica int `json:"requests_per_minute_per_replica"`
	TokensPerDay                int `json:"tokens_per_day"`
}

// BadgerKeyStore is the KeyStore backend that persists the dynamic key
// registry into the router's diskDB (BadgerDB at --disk-db-path). It is
// wired when config KeyStoreBackend = "badger" (HIVENET_ROUTER_KEY_STORE),
// letting deployments without a machines-service keysync (PoCs, local dev)
// keep minted keys across router restarts.
//
// Only key hashes (SHA-256 of the raw bearer) plus metadata are written to
// disk — raw bearer tokens are never persisted, matching the in-memory
// registry's storage model.
type BadgerKeyStore struct {
	store *storage.BadgerStorage
}

// NewBadgerKeyStore wraps the router's BadgerStorage instance.
func NewBadgerKeyStore(s *storage.BadgerStorage) *BadgerKeyStore {
	return &BadgerKeyStore{store: s}
}

// Load implements KeyStore. A fresh store (entry never written) is not an
// error — it returns an empty snapshot. Unknown schema versions and corrupt
// payloads are errors (the caller fails startup closed).
func (b *BadgerKeyStore) Load() (string, []DynamicKeyEntry, error) {
	raw, err := b.store.GetDiskEntry([]byte(keyStoreEntryKey))
	if err != nil {
		return "", nil, fmt.Errorf("key store: read: %w", err)
	}
	if raw == nil {
		return "", nil, nil
	}
	var snap keyStoreSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return "", nil, fmt.Errorf("key store: corrupt snapshot: %w", err)
	}
	if snap.Schema != keyStoreSchema {
		if snap.Schema < keyStoreSchema {
			return "", nil, fmt.Errorf("key store: unsupported snapshot schema %d (this router supports %d): the snapshot is from an OLDER router — no migration is available, wipe with --reset-key-store", snap.Schema, keyStoreSchema)
		}
		return "", nil, fmt.Errorf("key store: unsupported snapshot schema %d (this router supports %d): the snapshot is from a NEWER router — upgrade the router (or wipe with --reset-key-store)", snap.Schema, keyStoreSchema)
	}
	entries := make([]DynamicKeyEntry, 0, len(snap.Entries))
	for _, w := range snap.Entries {
		entries = append(entries, wireToEntry(w))
	}
	return snap.Version, entries, nil
}

// Save implements KeyStore — one atomic write of the whole registry.
// Written WITHOUT the configured diskDB TTL (SetDiskEntryNoTTL): a TTL'd
// snapshot would silently expire (default DiskDBTTLDays=30) and resurrect an
// empty registry on the next restart — exactly the failure the persistence
// backend exists to prevent.
func (b *BadgerKeyStore) Save(version string, entries []DynamicKeyEntry) error {
	wire := make([]keyStoreEntry, 0, len(entries))
	for _, e := range entries {
		wire = append(wire, entryToWire(e))
	}
	snap := keyStoreSnapshot{Schema: keyStoreSchema, Version: version, Entries: wire}
	raw, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("key store: encode: %w", err)
	}
	if err := b.store.SetDiskEntryNoTTL([]byte(keyStoreEntryKey), raw); err != nil {
		return fmt.Errorf("key store: write: %w", err)
	}
	return nil
}

// Reset implements KeyStore — drops the persisted snapshot (fresh start).
// Idempotent: resetting a store that was never written is a no-op.
func (b *BadgerKeyStore) Reset() error {
	if err := b.store.DeleteDiskEntry([]byte(keyStoreEntryKey)); err != nil {
		return fmt.Errorf("key store: reset: %w", err)
	}
	return nil
}

// entryToWire maps a registry entry to its explicit on-disk shape.
func entryToWire(e DynamicKeyEntry) keyStoreEntry {
	q := e.Quota
	wq := keyStoreQuota{
		RequestsPerMinute:     q.RequestsPerMinute,
		TokensPerDay:          q.TokensPerDay,
		InputTokensPerMinute:  q.InputTokensPerMinute,
		OutputTokensPerMinute: q.OutputTokensPerMinute,
		MaxOccupancyShare:     q.MaxOccupancyShare,
	}
	if len(q.PerModel) > 0 {
		wq.PerModel = make(map[string]keyStorePerModel, len(q.PerModel))
		for model, pm := range q.PerModel {
			wq.PerModel[model] = keyStorePerModel{
				RequestsPerMinutePerReplica: pm.RequestsPerMinutePerReplica,
				TokensPerDay:                pm.TokensPerDay,
			}
		}
	}
	return keyStoreEntry{
		ID:            e.ID,
		KeyHash:       e.KeyHash,
		KeyPreview:    e.KeyPreview,
		Owner:         e.Owner,
		Name:          e.Name,
		Enabled:       e.Enabled,
		ExpiresAt:     e.ExpiresAt,
		AllowedModels: e.AllowedModels,
		Quota:         wq,
	}
}

// wireToEntry maps the on-disk shape back to a registry entry.
func wireToEntry(w keyStoreEntry) DynamicKeyEntry {
	q := QuotaLimits{
		RequestsPerMinute:     w.Quota.RequestsPerMinute,
		TokensPerDay:          w.Quota.TokensPerDay,
		InputTokensPerMinute:  w.Quota.InputTokensPerMinute,
		OutputTokensPerMinute: w.Quota.OutputTokensPerMinute,
		MaxOccupancyShare:     w.Quota.MaxOccupancyShare,
	}
	if len(w.Quota.PerModel) > 0 {
		q.PerModel = make(map[string]PerModelQuotaLimits, len(w.Quota.PerModel))
		for model, pm := range w.Quota.PerModel {
			q.PerModel[model] = PerModelQuotaLimits{
				RequestsPerMinutePerReplica: pm.RequestsPerMinutePerReplica,
				TokensPerDay:                pm.TokensPerDay,
			}
		}
	}
	return DynamicKeyEntry{
		ID:            w.ID,
		KeyHash:       w.KeyHash,
		KeyPreview:    w.KeyPreview,
		Owner:         w.Owner,
		Name:          w.Name,
		Enabled:       w.Enabled,
		ExpiresAt:     w.ExpiresAt,
		AllowedModels: w.AllowedModels,
		Quota:         q,
	}
}
