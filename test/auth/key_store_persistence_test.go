// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Tests the opt-in persistence backend for the dynamic key registry
// (KeyStoreBackend "badger", env HIVENET_ROUTER_KEY_STORE):
//   - provider wiring: every mutation persists, memory mode persists nothing,
//     persist failures are reported without failing the mutation;
//   - BadgerKeyStore: Save/Load round-trip, fresh store, Reset;
//   - restart simulation: a second provider instance restores the registry
//     from the same diskDB and the key still authenticates.
package auth_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/storage"
)

// fakeKeyStore is an in-memory KeyStore for wiring tests (no badger).
type fakeKeyStore struct {
	mu sync.Mutex // -race safety; production saves are serialized by the provider's persistMu

	saves       int
	lastVer     string
	lastEntries []auth.DynamicKeyEntry
	failSave    error
	loaded      string
	loadEntries []auth.DynamicKeyEntry
	loadErr     error
	resetCalls  int

	// saveGate, when non-nil, blocks the FIRST Save until the channel is
	// closed by the test (used to deterministically order concurrent saves).
	saveGate chan struct{}
}

func (f *fakeKeyStore) Load() (string, []auth.DynamicKeyEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loaded, f.loadEntries, f.loadErr
}
func (f *fakeKeyStore) Save(version string, entries []auth.DynamicKeyEntry) error {
	f.mu.Lock()
	f.saves++
	n := f.saves
	f.mu.Unlock()
	if f.saveGate != nil && n == 1 {
		<-f.saveGate // first Save blocks until the test releases it
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastVer = version
	f.lastEntries = entries
	return f.failSave
}
func (f *fakeKeyStore) Reset() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resetCalls++
	return nil
}

func (f *fakeKeyStore) saveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saves
}

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func persistEntry(id, owner string) auth.DynamicKeyEntry {
	// Raw key derived from the ID so each entry gets a distinct hash — the
	// registry rejects two IDs sharing a key_hash (byHash desync guard).
	raw := "sk-" + id
	h := sha256.Sum256([]byte(raw))
	return auth.DynamicKeyEntry{
		ID:            id,
		KeyHash:       hex.EncodeToString(h[:]),
		Owner:         owner,
		Name:          id,
		Enabled:       true,
		AllowedModels: []string{"org/model"},
	}
}

func TestKeyStore_MutationsPersist(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)

	if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if fs.saves != 1 || fs.lastVer != "1" || len(fs.lastEntries) != 1 || fs.lastEntries[0].ID != "k1" {
		t.Fatalf("after Upsert: saves=%d ver=%q entries=%d (want 1, \"1\", 1 x k1)", fs.saves, fs.lastVer, len(fs.lastEntries))
	}

	if err := p.Delete("2", "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if fs.saves != 2 || fs.lastVer != "2" || len(fs.lastEntries) != 0 {
		t.Fatalf("after Delete: saves=%d ver=%q entries=%d (want 2, \"2\", 0)", fs.saves, fs.lastVer, len(fs.lastEntries))
	}

	entries := []auth.DynamicKeyEntry{persistEntry("k1", "org-x"), persistEntry("k2", "org-x")}
	if err := p.ReplaceAll("3", entries); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	if fs.saves != 3 || fs.lastVer != "3" || len(fs.lastEntries) != 2 {
		t.Fatalf("after ReplaceAll: saves=%d ver=%q entries=%d (want 3, \"3\", 2)", fs.saves, fs.lastVer, len(fs.lastEntries))
	}
	// Deterministic ID order in the persisted snapshot.
	if fs.lastEntries[0].ID != "k1" || fs.lastEntries[1].ID != "k2" {
		t.Fatalf("snapshot order = [%s %s], want [k1 k2]", fs.lastEntries[0].ID, fs.lastEntries[1].ID)
	}
}

func TestKeyStore_MemoryModePersistsNothing(t *testing.T) {
	p := auth.NewDynamicKeyProvider() // no store wired — default
	if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if ver, entries := p.Snapshot(); ver != "1" || len(entries) != 1 {
		t.Fatalf("Snapshot = (%q, %d entries), want (\"1\", 1)", ver, len(entries))
	}
	if err := p.LoadKeyStore(); err != nil { // must be a no-op without a store
		t.Fatalf("LoadKeyStore (unwired): %v", err)
	}
}

func TestKeyStore_PersistFailureDoesNotFailMutation(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	fs := &fakeKeyStore{failSave: errors.New("disk full")}
	var reported error
	p.SetKeyStore(fs)
	p.SetOnPersistError(func(err error) { reported = err })

	if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
		t.Fatalf("Upsert must not fail when persist fails: %v", err)
	}
	if e, ok := p.GetKey("k1"); !ok || e.ID != "k1" {
		t.Fatalf("in-memory mutation lost: ok=%v", ok)
	}
	if reported == nil {
		t.Fatal("persist failure not reported via OnPersistError")
	}
}

func TestKeyStore_LoadRestoresIntoEmptyProvider(t *testing.T) {
	snap := persistEntry("k1", "org-x")
	// Build a real Badger-backed store in a temp dir.
	bs, err := storage.NewBadgerStorage(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	bks := auth.NewBadgerKeyStore(bs)

	src := auth.NewDynamicKeyProvider()
	src.SetKeyStore(bks) // the source provider persists through the same diskDB
	if err := src.Upsert("1", snap); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Simulate a restart: a fresh provider loads the same diskDB.
	restored := auth.NewDynamicKeyProvider()
	restored.SetKeyStore(bks)
	if err := restored.LoadKeyStore(); err != nil {
		t.Fatalf("LoadKeyStore: %v", err)
	}
	ver, entries := restored.Snapshot()
	if ver != "1" || len(entries) != 1 || entries[0].ID != snap.ID || entries[0].KeyHash != snap.KeyHash {
		t.Fatalf("restore mismatch: ver=%q entries=%+v", ver, entries)
	}
}

func TestBadgerKeyStore_RoundTrip(t *testing.T) {
	bs, err := storage.NewBadgerStorage(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	ks := auth.NewBadgerKeyStore(bs)

	// Fresh store: Load is a non-error empty snapshot.
	ver, entries, err := ks.Load()
	if err != nil || ver != "" || entries != nil {
		t.Fatalf("fresh Load = (%q, %v, %v), want (\"\", nil, nil)", ver, entries, err)
	}

	snap := []auth.DynamicKeyEntry{persistEntry("k1", "org-x"), persistEntry("k2", "org-y")}
	if err := ks.Save("7", snap); err != nil {
		t.Fatalf("Save: %v", err)
	}
	ver, entries, err = ks.Load()
	if err != nil || ver != "7" || len(entries) != 2 || entries[0].Owner != "org-x" || entries[1].Owner != "org-y" {
		t.Fatalf("round-trip mismatch: ver=%q entries=%+v err=%v", ver, entries, err)
	}

	// Reset drops the snapshot; a fresh load is empty again.
	if err := ks.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, entries, err := ks.Load(); err != nil || entries != nil {
		t.Fatalf("post-Reset Load = (%v, %v), want empty", entries, err)
	}
}

func TestBadgerKeyStore_CorruptSnapshotFailsLoad(t *testing.T) {
	bs, err := storage.NewBadgerStorage(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	ks := auth.NewBadgerKeyStore(bs)

	// Poison the snapshot entry directly (simulates corruption).
	if err := bs.SetDiskEntry([]byte("keyStore:registry"), []byte("{not-json")); err != nil {
		t.Fatalf("SetDiskEntry: %v", err)
	}
	p := auth.NewDynamicKeyProvider()
	p.SetKeyStore(ks)
	if err := p.LoadKeyStore(); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("corrupt snapshot must fail LoadKeyStore (fail closed), got: %v", err)
	}
}

// TestBadgerKeyStore_FullFieldRoundTrip: every field of a registry entry
// must survive Save→Load unchanged. If a field is dropped from the mapping
// — or a new field is added to DynamicKeyEntry/QuotaLimits without a DTO
// update — a restored key would come back with weaker limits; this test
// fails instead.
func TestBadgerKeyStore_FullFieldRoundTrip(t *testing.T) {
	bs, err := storage.NewBadgerStorage(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	ks := auth.NewBadgerKeyStore(bs)

	// Drift guards: if the source types gain fields, the DTO mapping must be
	// updated in the same change — fail loudly instead of silently zeroing
	// the new field on load.
	if n := reflect.TypeOf(auth.DynamicKeyEntry{}).NumField(); n != 9 {
		t.Fatalf("DynamicKeyEntry has %d fields (was 9) — update entryToWire/wireToEntry/keyStoreEntry", n)
	}
	if n := reflect.TypeOf(auth.QuotaLimits{}).NumField(); n != 6 {
		t.Fatalf("QuotaLimits has %d fields (was 6) — update entryToWire/wireToEntry/keyStoreQuota", n)
	}

	expiry := time.Date(2026, 12, 1, 9, 30, 0, 0, time.UTC)
	want := auth.DynamicKeyEntry{
		ID:            "k1",
		KeyHash:       strings.Repeat("ab", 32),
		KeyPreview:    "sk-...XYZ",
		Owner:         "org-x",
		Name:          "label",
		Enabled:       true,
		ExpiresAt:     &expiry,
		AllowedModels: []string{"m1", "m2"},
		Quota: auth.QuotaLimits{
			RequestsPerMinute:     11,
			TokensPerDay:          22,
			InputTokensPerMinute:  33,
			OutputTokensPerMinute: 44,
			MaxOccupancyShare:     0.25,
			PerModel: map[string]auth.PerModelQuotaLimits{
				"m1": {RequestsPerMinutePerReplica: 5, TokensPerDay: 6},
				"m2": {RequestsPerMinutePerReplica: 7, TokensPerDay: 8},
			},
		},
	}

	if err := ks.Save("v1", []auth.DynamicKeyEntry{want}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_, got, err := ks.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("round-trip mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

// TestBadgerKeyStore_UnknownSchemaRejected: a snapshot written by a newer
// (or other) router version must be refused, not half-parsed.
func TestBadgerKeyStore_UnknownSchemaRejected(t *testing.T) {
	bs, err := storage.NewBadgerStorage(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	if err := bs.SetDiskEntryNoTTL([]byte("keyStore:registry"), []byte(`{"schema":999,"version":"1","entries":[]}`)); err != nil {
		t.Fatalf("SetDiskEntryNoTTL: %v", err)
	}
	ks := auth.NewBadgerKeyStore(bs)
	if _, _, err := ks.Load(); err == nil || !strings.Contains(err.Error(), "unsupported snapshot schema") {
		t.Fatalf("unknown schema must be refused (fail closed), got: %v", err)
	}
}

// TestKeyStore_EmptySnapshotRestoresVersion: a persisted {version, []} (all
// keys deleted) must restore the version, or a stale push below that version
// would be accepted after a restart (monotonic-version weakening).
func TestKeyStore_EmptySnapshotRestoresVersion(t *testing.T) {
	fs := &fakeKeyStore{loaded: "9"} // persisted empty registry at version 9
	p := auth.NewDynamicKeyProvider()
	p.SetKeyStore(fs)
	if err := p.LoadKeyStore(); err != nil {
		t.Fatalf("LoadKeyStore: %v", err)
	}
	ver, _ := p.Version()
	if ver != "9" {
		t.Fatalf("version after empty-snapshot load = %q, want \"9\"", ver)
	}
	// A push replayed from before the last sync must now be rejected.
	if err := p.Upsert("8", persistEntry("k1", "org-x")); err != auth.ErrStaleVersion {
		t.Fatalf("stale push must be rejected, got: %v", err)
	}
}

// TestKeyStore_ConcurrentMutationsSerializePersists: the persisted snapshot
// must never end up older than the in-memory registry (lost update).
//
// Deterministic with persistMu: k2's save can only complete after k1's
// (gated) save — Snapshot+Save is serialized — so the last persisted
// snapshot contains both keys. Without persistMu, k1's stale one-entry save
// lands after k2's two-entry save and overwrites it (caught with some
// probability — the regression is a race, so no test can catch it with
// certainty).
func TestKeyStore_ConcurrentMutationsSerializePersists(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	gate := make(chan struct{})
	fs := &fakeKeyStore{saveGate: gate}
	p.SetKeyStore(fs)

	var wg sync.WaitGroup

	// k1's Upsert: its Save (the first one) blocks on the gate.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
			t.Errorf("Upsert k1: %v", err)
		}
	}()
	waitUntil(t, "first Save in flight", func() bool { return fs.saveCount() >= 1 })

	// k2's Upsert runs concurrently. With persistMu its save waits for k1's
	// (gated) save to finish; without it, k2's save can land first.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.Upsert("1", persistEntry("k2", "org-x")); err != nil {
			t.Errorf("Upsert k2: %v", err)
		}
	}()
	waitUntil(t, "k2 in memory", func() bool {
		_, entries := p.Snapshot()
		return len(entries) == 2
	})

	// Release k1's (possibly stale) save — it must be the LAST persisted
	// snapshot, so with persistMu k2's two-entry save lands after it.
	close(gate)
	wg.Wait()

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.lastEntries) != 2 {
		t.Fatalf("last persisted snapshot has %d entries, want 2 — an older save overwrote a newer one (lost update)", len(fs.lastEntries))
	}
}

// TestBadgerKeyStore_SnapshotSurvivesDiskTTL guards the TTL footgun: the
// default DiskDBTTLDays=30 means every SetDiskEntry write (which applies the
// configured TTL) would silently expire the registry snapshot, resurrecting
// an EMPTY store on the next router restart — defeating the persistence the
// backend exists to provide. BadgerKeyStore.Save therefore uses
// SetDiskEntryNoTTL; this test proves the snapshot outlives the TTL that a
// regular diskDB entry does not.
func TestBadgerKeyStore_SnapshotSurvivesDiskTTL(t *testing.T) {
	bs, err := storage.NewBadgerStorage(t.TempDir(), time.Second) // 1s diskTTL
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	// Control: a TTL'd entry (the SetDiskEntry path) must expire.
	if err := bs.SetDiskEntry([]byte("control"), []byte("x")); err != nil {
		t.Fatalf("SetDiskEntry: %v", err)
	}
	// The key-store snapshot path must NOT carry the TTL.
	ks := auth.NewBadgerKeyStore(bs)
	if err := ks.Save("1", []auth.DynamicKeyEntry{persistEntry("k1", "org-x")}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	time.Sleep(1100 * time.Millisecond) // let the 1s TTL elapse

	if v, err := bs.GetDiskEntry([]byte("control")); err != nil || v != nil {
		t.Fatalf("control entry should have expired with diskTTL=1s (test premise): v=%q err=%v", v, err)
	}
	if v, err := bs.GetDiskEntry([]byte("keyStore:registry")); err != nil || v == nil {
		t.Fatalf("key store snapshot must survive the diskTTL (no-TTL write): v=%q err=%v", v, err)
	}
}
