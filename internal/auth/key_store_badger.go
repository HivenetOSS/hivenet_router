// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"golang.org/x/crypto/hkdf"
)

// On-disk layout of the dedicated key-store BadgerDB (its own directory,
// --key-store-path — never the history diskDB, so --reset-disk-db and
// --reset-key-store each wipe only their own data):
//
//	keyStore:v1:meta                     → MAC ‖ JSON keyStoreMeta
//	keyStore:v1:g<gen>:key:<id>          → MAC ‖ JSON keyStoreEntry
//
// One record per key keeps Upsert/Delete O(1) in registry size: a single
// small transaction writes that key's record plus the meta record. ReplaceAll
// writes a whole new generation with a WriteBatch (not bounded by badger's
// per-transaction size limit), flips meta.gen to it in one transaction, then
// deletes the previous generation. A crash before the flip leaves meta on the
// previous generation; orphaned generations are removed on the next Load.
//
// Every value is prefixed with an HMAC-SHA256 over (record key ‖ 0x00 ‖
// payload), keyed by a secret derived from the router's JWT secret. Binding
// the record key into the MAC stops a valid record from being moved to
// another ID or generation.
const (
	keyStoreKeyPrefix = "keyStore:v1:"
	keyStoreMetaKey   = keyStoreKeyPrefix + "meta"
	keyStoreGenPrefix = keyStoreKeyPrefix + "g"
)

// keyStoreSchema is the on-disk schema version (meta.schema). Load refuses
// any other version rather than guessing (fail closed). Bump it on any shape
// change AND add a migration case in Load. Note the persisted registry is a
// warm-start cache of the upstream key-sync service, so the cost of an
// unmigrated bump is one cold start after --reset-key-store, not data loss.
const keyStoreSchema = 2

// keyStoreHMACSalt / Info domain-separate the record MAC key from every other
// key derived from the JWT secret (JWT signing, gRPC TLS — see tlscreds.go).
const (
	keyStoreHMACSalt = "hivenet-router-keystore-hmac-v1"
	keyStoreHMACInfo = "hmac-sha256"
)

// keyStoreMeta is the on-disk shape of the meta record.
type keyStoreMeta struct {
	Schema  int       `json:"schema"`
	Gen     uint64    `json:"gen"`      // generation holding the live key records
	Version string    `json:"version"`  // registry version (from upstream key-sync)
	SavedAt time.Time `json:"saved_at"` // time of the last successful write
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

// errRecordIntegrity marks a record whose MAC does not verify.
var errRecordIntegrity = errors.New("integrity check failed")

// BadgerKeyStore is the KeyStore backend that persists the dynamic key
// registry into a dedicated BadgerDB. It is wired when config
// KeyStoreBackend = "badger" (--key-store / HIVENET_ROUTER_KEY_STORE).
//
// Only key hashes (SHA-256 of the raw bearer) plus metadata are written to
// disk — raw bearer tokens are never persisted. The DB is opened with
// SyncWrites, so every acknowledged write (revocations included) survives a
// node crash or power loss.
type BadgerKeyStore struct {
	db     *badger.DB
	macKey []byte

	mu  sync.Mutex // guards gen; callers are already serialized, this is belt-and-braces
	gen uint64     // live generation (0 = nothing written yet)
}

// DeriveKeyStoreMACKey derives the record-MAC key from the router's JWT
// secret via HKDF-SHA256 (same pattern as DeriveGRPCCredentials).
func DeriveKeyStoreMACKey(secret []byte) ([]byte, error) {
	if len(secret) == 0 {
		return nil, fmt.Errorf("key store: empty MAC secret")
	}
	key := make([]byte, sha256.Size)
	if _, err := io.ReadFull(hkdf.New(sha256.New, secret, []byte(keyStoreHMACSalt), []byte(keyStoreHMACInfo)), key); err != nil {
		return nil, fmt.Errorf("key store: derive MAC key: %w", err)
	}
	return key, nil
}

// OpenBadgerKeyStore opens (creating if needed) the key-store DB at path.
// The directory is created with mode 0700 and tightened to 0700 if it
// already exists. secret is the router's JWT secret (the MAC key is derived
// from it; see DeriveKeyStoreMACKey).
func OpenBadgerKeyStore(path string, secret []byte) (*BadgerKeyStore, error) {
	macKey, err := DeriveKeyStoreMACKey(secret)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("key store: create %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return nil, fmt.Errorf("key store: chmod %s: %w", path, err)
	}
	db, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil).WithSyncWrites(true))
	if err != nil {
		return nil, fmt.Errorf("key store: open %s: %w", path, err)
	}
	return &BadgerKeyStore{db: db, macKey: macKey}, nil
}

// Close implements KeyStore.
func (b *BadgerKeyStore) Close() error { return b.db.Close() }

func genPrefix(gen uint64) string { return fmt.Sprintf("%s%016x:key:", keyStoreGenPrefix, gen) }

func recordKey(gen uint64, id string) []byte { return []byte(genPrefix(gen) + id) }

// seal returns MAC ‖ payload for the given record key.
func (b *BadgerKeyStore) seal(key, payload []byte) []byte {
	m := hmac.New(sha256.New, b.macKey)
	m.Write(key)
	m.Write([]byte{0})
	m.Write(payload)
	return append(m.Sum(nil), payload...)
}

// open verifies MAC ‖ payload for the given record key and returns payload.
func (b *BadgerKeyStore) open(key, val []byte) ([]byte, error) {
	if len(val) < sha256.Size {
		return nil, fmt.Errorf("%w: record too short", errRecordIntegrity)
	}
	mac, payload := val[:sha256.Size], val[sha256.Size:]
	m := hmac.New(sha256.New, b.macKey)
	m.Write(key)
	m.Write([]byte{0})
	m.Write(payload)
	if !hmac.Equal(mac, m.Sum(nil)) {
		return nil, errRecordIntegrity
	}
	return payload, nil
}

func (b *BadgerKeyStore) sealMeta(meta keyStoreMeta) ([]byte, error) {
	raw, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("key store: encode meta: %w", err)
	}
	return b.seal([]byte(keyStoreMetaKey), raw), nil
}

func (b *BadgerKeyStore) sealEntry(gen uint64, e DynamicKeyEntry) ([]byte, []byte, error) {
	raw, err := json.Marshal(entryToWire(e))
	if err != nil {
		return nil, nil, fmt.Errorf("key store: encode key %q: %w", e.ID, err)
	}
	k := recordKey(gen, e.ID)
	return k, b.seal(k, raw), nil
}

// Load implements KeyStore. A fresh store returns (nil, nil). The meta
// record is all-or-nothing: unreadable meta (I/O error, corrupt JSON,
// unknown schema) is an error so the caller fails startup closed; a meta
// record whose MAC does not verify is reported as Untrusted (the caller
// discards the snapshot). Individual key records are decoded independently:
// one that fails its MAC or JSON decode is returned with Err set rather than
// failing the whole load. Generations other than the live one (left behind
// by an interrupted ReplaceAll) are deleted.
func (b *BadgerKeyStore) Load() (*PersistedRegistry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var (
		out      *PersistedRegistry
		orphans  [][]byte
		metaRead bool
	)
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyStoreMetaKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("key store: read meta: %w", err)
		}
		metaRead = true
		val, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("key store: read meta: %w", err)
		}
		payload, macErr := b.open([]byte(keyStoreMetaKey), val)
		if macErr != nil {
			// Nothing in an unverified meta record is trusted, its generation
			// included. Restart the counter: the next write clears the
			// generation it targets, and records in any other generation can
			// never become live without a validly sealed meta pointing at
			// them (the next trusted Load deletes them as orphans).
			b.gen = 0
			out = &PersistedRegistry{Untrusted: true}
			return nil
		}
		var meta keyStoreMeta
		if err := json.Unmarshal(payload, &meta); err != nil {
			return fmt.Errorf("key store: corrupt meta: %w", err)
		}
		if meta.Schema != keyStoreSchema {
			if meta.Schema < keyStoreSchema {
				return fmt.Errorf("key store: unsupported schema %d (this router supports %d): the snapshot is from an OLDER router — no migration is available, wipe with --reset-key-store", meta.Schema, keyStoreSchema)
			}
			return fmt.Errorf("key store: unsupported schema %d (this router supports %d): the snapshot is from a NEWER router — upgrade the router (or wipe with --reset-key-store)", meta.Schema, keyStoreSchema)
		}
		b.gen = meta.Gen
		out = &PersistedRegistry{Version: meta.Version, SavedAt: meta.SavedAt}

		live := []byte(genPrefix(meta.Gen))
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: true, PrefetchSize: 256, Prefix: []byte(keyStoreGenPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if !bytes.HasPrefix(k, live) {
				orphans = append(orphans, k)
				continue
			}
			id := string(k[len(live):])
			pk := PersistedKey{ID: id}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("key store: read key %q: %w", id, err)
			}
			payload, err := b.open(k, v)
			if err != nil {
				pk.Err = err
				out.Keys = append(out.Keys, pk)
				continue
			}
			var w keyStoreEntry
			if err := json.Unmarshal(payload, &w); err != nil {
				pk.Err = fmt.Errorf("corrupt record: %w", err)
				out.Keys = append(out.Keys, pk)
				continue
			}
			if w.ID != id {
				pk.Err = fmt.Errorf("record id %q does not match its key", w.ID)
				out.Keys = append(out.Keys, pk)
				continue
			}
			pk.Entry = wireToEntry(w)
			out.Keys = append(out.Keys, pk)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !metaRead {
		return nil, nil
	}
	if out.Untrusted {
		return out, nil
	}
	if len(orphans) > 0 {
		if err := b.deleteKeys(orphans); err != nil {
			return nil, fmt.Errorf("key store: remove orphaned generation records: %w", err)
		}
	}
	return out, nil
}

// Upsert implements KeyStore: one transaction writing the key's record in the
// live generation plus the meta record.
func (b *BadgerKeyStore) Upsert(version string, e DynamicKeyEntry, savedAt time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen == 0 {
		// Nothing written yet: start generation 1 the same way ReplaceAll does
		// so the meta record exists from the first write on.
		return b.writeGenerationLocked(version, []DynamicKeyEntry{e}, savedAt)
	}
	k, v, err := b.sealEntry(b.gen, e)
	if err != nil {
		return err
	}
	mv, err := b.sealMeta(keyStoreMeta{Schema: keyStoreSchema, Gen: b.gen, Version: version, SavedAt: savedAt.UTC()})
	if err != nil {
		return err
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(k, v); err != nil {
			return err
		}
		return txn.Set([]byte(keyStoreMetaKey), mv)
	}); err != nil {
		return fmt.Errorf("key store: write key %q: %w", e.ID, err)
	}
	return nil
}

// Delete implements KeyStore: one transaction deleting the key's record and
// updating the meta record. Deleting an absent key still advances meta.
func (b *BadgerKeyStore) Delete(version, id string, savedAt time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen == 0 {
		return b.writeGenerationLocked(version, nil, savedAt)
	}
	mv, err := b.sealMeta(keyStoreMeta{Schema: keyStoreSchema, Gen: b.gen, Version: version, SavedAt: savedAt.UTC()})
	if err != nil {
		return err
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(recordKey(b.gen, id)); err != nil {
			return err
		}
		return txn.Set([]byte(keyStoreMetaKey), mv)
	}); err != nil {
		return fmt.Errorf("key store: delete key %q: %w", id, err)
	}
	return nil
}

// ReplaceAll implements KeyStore: write a new generation, flip meta to it,
// then delete the previous generation.
func (b *BadgerKeyStore) ReplaceAll(version string, entries []DynamicKeyEntry, savedAt time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writeGenerationLocked(version, entries, savedAt)
}

func (b *BadgerKeyStore) writeGenerationLocked(version string, entries []DynamicKeyEntry, savedAt time.Time) error {
	oldGen, newGen := b.gen, b.gen+1

	// 0. The target generation must start empty. It normally is, but after an
	//    untrusted meta record (whose generation could not be read) b.gen may
	//    point below a generation that still holds stale records.
	if err := b.deletePrefix([]byte(genPrefix(newGen))); err != nil {
		return fmt.Errorf("key store: clear generation %d: %w", newGen, err)
	}

	// 1. Write the new generation. WriteBatch splits into as many
	//    transactions as needed, so a large registry cannot hit ErrTxnTooBig.
	//    Until step 2 commits these records are invisible to Load (orphans).
	wb := b.db.NewWriteBatch()
	for _, e := range entries {
		k, v, err := b.sealEntry(newGen, e)
		if err != nil {
			wb.Cancel()
			return err
		}
		if err := wb.Set(k, v); err != nil {
			wb.Cancel()
			return fmt.Errorf("key store: write generation %d: %w", newGen, err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("key store: write generation %d: %w", newGen, err)
	}

	// 2. Flip meta to the new generation — the single atomic commit point.
	mv, err := b.sealMeta(keyStoreMeta{Schema: keyStoreSchema, Gen: newGen, Version: version, SavedAt: savedAt.UTC()})
	if err != nil {
		return err
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyStoreMetaKey), mv)
	}); err != nil {
		return fmt.Errorf("key store: commit generation %d: %w", newGen, err)
	}
	b.gen = newGen

	// 3. Drop the previous generation. A failure here is not a durability
	//    problem (meta already points at newGen); Load removes leftovers.
	if oldGen != 0 {
		if err := b.deletePrefix([]byte(genPrefix(oldGen))); err != nil {
			return fmt.Errorf("key store: generation %d committed, but removing generation %d failed: %w", newGen, oldGen, err)
		}
	}
	return nil
}

// DeleteRecords implements KeyStore: remove the given IDs' records from the
// live generation (used to drop entries skipped at load). Meta is untouched.
func (b *BadgerKeyStore) DeleteRecords(ids []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen == 0 || len(ids) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, recordKey(b.gen, id))
	}
	return b.deleteKeys(keys)
}

func (b *BadgerKeyStore) deletePrefix(prefix []byte) error {
	var keys [][]byte
	if err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return err
	}
	return b.deleteKeys(keys)
}

func (b *BadgerKeyStore) deleteKeys(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	wb := b.db.NewWriteBatch()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			wb.Cancel()
			return err
		}
	}
	return wb.Flush()
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
			wq.PerModel[model] = keyStorePerModel(pm)
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
			q.PerModel[model] = PerModelQuotaLimits(pm)
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
