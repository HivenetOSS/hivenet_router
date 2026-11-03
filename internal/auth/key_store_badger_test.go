// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const internalTestSecret = "internal-test-jwt-secret-0123456789abcdef"

func ksEntry(id string) DynamicKeyEntry {
	h := sha256.Sum256([]byte("sk-" + id))
	return DynamicKeyEntry{ID: id, KeyHash: hex.EncodeToString(h[:]), Owner: "o", Name: id, Enabled: true}
}

func openTestStore(t testing.TB, dir string) *BadgerKeyStore {
	t.Helper()
	ks, err := OpenBadgerKeyStore(dir, []byte(internalTestSecret))
	if err != nil {
		t.Fatalf("OpenBadgerKeyStore: %v", err)
	}
	t.Cleanup(func() { ks.Close() }) //nolint:errcheck
	return ks
}

func rawSet(t testing.TB, ks *BadgerKeyStore, key, val []byte) {
	t.Helper()
	if err := ks.db.Update(func(txn *badger.Txn) error { return txn.Set(key, val) }); err != nil {
		t.Fatalf("raw set: %v", err)
	}
}

func liveIDs(t testing.TB, snap *PersistedRegistry) (ok []string, bad []string) {
	t.Helper()
	for _, k := range snap.Keys {
		if k.Err != nil {
			bad = append(bad, k.ID)
		} else {
			ok = append(ok, k.ID)
		}
	}
	return ok, bad
}

func countPrefix(t testing.TB, ks *BadgerKeyStore, prefix string) int {
	t.Helper()
	n := 0
	_ = ks.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(prefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	return n
}

func TestBadgerKeyStore_SyncWritesEnabled(t *testing.T) {
	ks := openTestStore(t, t.TempDir())
	if !ks.db.Opts().SyncWrites {
		t.Fatal("the key store must fsync every write (SyncWrites)")
	}
}

// A ReplaceAll interrupted before the meta flip leaves meta on the previous
// generation: Load returns the previous registry and removes the orphans.
func TestBadgerKeyStore_InterruptedReplaceKeepsPreviousGeneration(t *testing.T) {
	ks := openTestStore(t, t.TempDir())
	now := time.Now()
	if err := ks.ReplaceAll("v1", []DynamicKeyEntry{ksEntry("a"), ksEntry("b")}, now); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	// Simulate a crash mid-replace: generation 2 partially written, meta not flipped.
	k, v, err := ks.sealEntry(2, ksEntry("c"))
	if err != nil {
		t.Fatal(err)
	}
	rawSet(t, ks, k, v)

	snap, err := ks.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ok, bad := liveIDs(t, snap)
	if snap.Version != "v1" || len(ok) != 2 || len(bad) != 0 {
		t.Fatalf("Load after interrupted replace: version=%q ok=%v bad=%v, want v1 [a b]", snap.Version, ok, bad)
	}
	if n := countPrefix(t, ks, genPrefix(2)); n != 0 {
		t.Fatalf("orphaned generation must be removed on Load, %d record(s) left", n)
	}
	// The next replace still works and does not pick up stale records.
	if err := ks.ReplaceAll("v2", []DynamicKeyEntry{ksEntry("d")}, now); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	snap, _ = ks.Load()
	if ok, _ := liveIDs(t, snap); len(ok) != 1 || ok[0] != "d" {
		t.Fatalf("after replace: %v, want [d]", ok)
	}
	if n := countPrefix(t, ks, genPrefix(1)); n != 0 {
		t.Fatalf("previous generation must be deleted after a replace, %d left", n)
	}
}

// A record that does not verify — injected, modified, or copied to another
// ID — is reported per record, never restored.
func TestBadgerKeyStore_IntegrityPerRecord(t *testing.T) {
	ks := openTestStore(t, t.TempDir())
	now := time.Now()
	if err := ks.ReplaceAll("v1", []DynamicKeyEntry{ksEntry("a"), ksEntry("b")}, now); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	gen := ks.gen

	// 1. Injected record with a forged MAC.
	payload, _ := json.Marshal(entryToWire(ksEntry("evil")))
	rawSet(t, ks, recordKey(gen, "evil"), append(make([]byte, sha256.Size), payload...))
	// 2. Valid record for "a" copied under id "copy".
	var aVal []byte
	_ = ks.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(recordKey(gen, "a"))
		if err != nil {
			return err
		}
		aVal, err = it.ValueCopy(nil)
		return err
	})
	rawSet(t, ks, recordKey(gen, "copy"), aVal)
	// 3. Record "b" with one payload byte flipped.
	_, bVal, _ := ks.sealEntry(gen, ksEntry("b"))
	bVal[len(bVal)-2] ^= 0x01
	rawSet(t, ks, recordKey(gen, "b"), bVal)

	snap, err := ks.Load()
	if err != nil {
		t.Fatalf("per-record integrity failures must not fail Load: %v", err)
	}
	ok, bad := liveIDs(t, snap)
	if len(ok) != 1 || ok[0] != "a" || len(bad) != 3 {
		t.Fatalf("ok=%v bad=%v, want ok=[a] and 3 rejected", ok, bad)
	}
}

func TestBadgerKeyStore_TamperedMetaIsUntrusted(t *testing.T) {
	ks := openTestStore(t, t.TempDir())
	if err := ks.ReplaceAll("v1", []DynamicKeyEntry{ksEntry("a")}, time.Now()); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	// Push saved_at forward to dodge the max-age check, without a valid MAC.
	meta, _ := json.Marshal(keyStoreMeta{Schema: keyStoreSchema, Gen: ks.gen, Version: "v9", SavedAt: time.Now().Add(time.Hour)})
	rawSet(t, ks, []byte(keyStoreMetaKey), append(make([]byte, sha256.Size), meta...))
	snap, err := ks.Load()
	if err != nil || snap == nil || !snap.Untrusted {
		t.Fatalf("tampered meta must load as Untrusted, got %+v err=%v", snap, err)
	}
}

func TestBadgerKeyStore_UnreadableMetaFailsLoad(t *testing.T) {
	cases := map[string][]byte{
		"corrupt json":   []byte("{not-json"),
		"unknown schema": []byte(`{"schema":999,"gen":1,"version":"1"}`),
		"older schema":   []byte(`{"schema":1,"gen":1,"version":"1"}`),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			ks := openTestStore(t, t.TempDir())
			rawSet(t, ks, []byte(keyStoreMetaKey), ks.seal([]byte(keyStoreMetaKey), payload))
			if _, err := ks.Load(); err == nil {
				t.Fatal("unreadable meta must fail Load (startup fails closed)")
			}
		})
	}
}

// A replace far larger than badger's per-transaction limit must succeed
// (WriteBatch), which a single-transaction snapshot could not do.
func TestBadgerKeyStore_LargeReplaceAll(t *testing.T) {
	if testing.Short() {
		t.Skip("large replace")
	}
	ks := openTestStore(t, t.TempDir())
	entries := make([]DynamicKeyEntry, 50_000)
	for i := range entries {
		entries[i] = ksEntry(fmt.Sprintf("key-%06d", i))
		entries[i].AllowedModels = []string{"org/model-a", "org/model-b"}
		entries[i].KeyPreview = "sk-...abc"
	}
	if err := ks.ReplaceAll("v1", entries, time.Now()); err != nil {
		t.Fatalf("50k ReplaceAll: %v", err)
	}
	if err := ks.ReplaceAll("v2", entries, time.Now()); err != nil {
		t.Fatalf("second 50k ReplaceAll: %v", err)
	}
	snap, err := ks.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(snap.Keys) != 50_000 || snap.Version != "v2" {
		t.Fatalf("loaded %d keys at %q, want 50000 at v2", len(snap.Keys), snap.Version)
	}
	if n := countPrefix(t, ks, keyStoreGenPrefix); n != 50_000 {
		t.Fatalf("%d records on disk, want exactly one generation (50000)", n)
	}
}

// BenchmarkBadgerKeyStore_Upsert shows Upsert cost is independent of the
// registry size (one small transaction), unlike a full snapshot per mutation.
func BenchmarkBadgerKeyStore_Upsert(b *testing.B) {
	for _, size := range []int{1_000, 50_000} {
		b.Run(fmt.Sprintf("registry=%d", size), func(b *testing.B) {
			ks := openTestStore(b, b.TempDir())
			entries := make([]DynamicKeyEntry, size)
			for i := range entries {
				entries[i] = ksEntry(fmt.Sprintf("key-%06d", i))
			}
			if err := ks.ReplaceAll("v0", entries, time.Now()); err != nil {
				b.Fatalf("seed: %v", err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e := entries[i%size]
				if err := ks.Upsert(fmt.Sprintf("v%09d", i+1), e, time.Now()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// FuzzBadgerKeyStoreLoad feeds arbitrary bytes into the meta record and a
// key record: Load must never panic, and a key record that is not sealed by
// the store must never come back as a valid entry.
func FuzzBadgerKeyStoreLoad(f *testing.F) {
	f.Add([]byte(`{"schema":2,"gen":1,"version":"v1","saved_at":"2026-01-01T00:00:00Z"}`), []byte(`{"id":"x"}`), true)
	f.Add([]byte("{not-json"), []byte{}, true)
	f.Add([]byte{}, []byte("garbage"), false)
	f.Add([]byte(`{"schema":2,"gen":18446744073709551615}`), []byte(`null`), true)

	ks := openTestStore(f, f.TempDir())
	f.Fuzz(func(t *testing.T, metaPayload, recordVal []byte, sealMeta bool) {
		_ = ks.db.DropAll()
		ks.gen = 0
		metaVal := metaPayload
		if sealMeta {
			metaVal = ks.seal([]byte(keyStoreMetaKey), metaPayload)
		}
		var m keyStoreMeta
		gen := uint64(1)
		if json.Unmarshal(metaPayload, &m) == nil {
			gen = m.Gen
		}
		rawSet(t, ks, []byte(keyStoreMetaKey), metaVal)
		rawSet(t, ks, recordKey(gen, "x"), recordVal) // unsealed: must never verify

		snap, err := ks.Load()
		if err != nil || snap == nil || snap.Untrusted {
			return
		}
		for _, k := range snap.Keys {
			if k.Err == nil {
				t.Fatalf("an unsealed record was accepted: %+v", k)
			}
		}
	})
}
