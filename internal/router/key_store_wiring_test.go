// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/storage"
)

func newTestBadgerStore(t *testing.T) (*storage.BadgerStorage, *config.Config, *auth.DynamicKeyProvider) {
	t.Helper()
	dir := t.TempDir()
	s, err := storage.NewBadgerStorage(dir, 0)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{KeyStoreBackend: "badger", DiskDBPath: dir}
	reg := auth.NewDynamicKeyProvider()
	return s, cfg, reg
}

func testKey(id string) auth.DynamicKeyEntry {
	// 64-char lowercase hex hash derived from the id — distinct per ID
	// (the registry rejects two IDs sharing a key_hash).
	h := sha256.Sum256([]byte("sk-" + id))
	return auth.DynamicKeyEntry{
		ID:            id,
		KeyHash:       hex.EncodeToString(h[:]),
		Owner:         "org-x",
		Name:          id,
		Enabled:       true,
		AllowedModels: []string{"org/model"},
	}
}

func TestWireKeyStore_InvalidBackendRejected(t *testing.T) {
	s, cfg, reg := newTestBadgerStore(t)
	cfg.KeyStoreBackend = "bogus"
	err := wireKeyStore(cfg, s, reg)
	if err == nil || !strings.Contains(err.Error(), "invalid KeyStoreBackend") {
		t.Fatalf("invalid backend must be rejected at startup, got: %v", err)
	}
}

func TestWireKeyStore_BadgerRequiresDynamicRegistry(t *testing.T) {
	s, cfg, _ := newTestBadgerStore(t)
	if err := wireKeyStore(cfg, s, nil); err == nil ||
		!strings.Contains(err.Error(), "requires dynamic auth mode") {
		t.Fatalf("badger without a dynamic registry must fail, got: %v", err)
	}
}

func TestWireKeyStore_PoisonedSnapshotFailsClosed(t *testing.T) {
	s, cfg, reg := newTestBadgerStore(t)
	// Pre-poison the snapshot the way a crash/corruption would.
	if err := s.SetDiskEntryNoTTL([]byte("keyStore:registry"), []byte("{not-json")); err != nil {
		t.Fatalf("SetDiskEntryNoTTL: %v", err)
	}
	if err := wireKeyStore(cfg, s, reg); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("corrupt snapshot must abort startup (fail closed), got: %v", err)
	}
}

func TestWireKeyStore_ResetWipesSnapshot(t *testing.T) {
	s, cfg, reg := newTestBadgerStore(t)
	// Seed a persisted snapshot through a first wiring pass.
	reg.SetKeyStore(auth.NewBadgerKeyStore(s))
	if err := reg.Upsert("1", testKey("k1")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	cfg.ResetKeyStore = true
	fresh := auth.NewDynamicKeyProvider()
	if err := wireKeyStore(cfg, s, fresh); err != nil {
		t.Fatalf("wireKeyStore with reset: %v", err)
	}
	if ver, entries := fresh.Snapshot(); ver != "" || len(entries) != 0 {
		t.Fatalf("reset must wipe the snapshot: ver=%q entries=%d, want empty", ver, len(entries))
	}
}

func TestWireKeyStore_RestoresSnapshot(t *testing.T) {
	s, cfg, reg := newTestBadgerStore(t)
	reg.SetKeyStore(auth.NewBadgerKeyStore(s))
	if err := reg.Upsert("1", testKey("k1")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Simulate a restart: a fresh registry wired to the same store.
	fresh := auth.NewDynamicKeyProvider()
	if err := wireKeyStore(cfg, s, fresh); err != nil {
		t.Fatalf("wireKeyStore: %v", err)
	}
	ver, entries := fresh.Snapshot()
	if ver != "1" || len(entries) != 1 || entries[0].ID != "k1" {
		t.Fatalf("restore mismatch: ver=%q entries=%+v", ver, entries)
	}
}

func TestWireKeyStore_MemoryModeNoOp(t *testing.T) {
	s, cfg, reg := newTestBadgerStore(t)
	cfg.KeyStoreBackend = "memory"
	if err := wireKeyStore(cfg, s, reg); err != nil {
		t.Fatalf("memory mode must be a no-op: %v", err)
	}
	// And the unwired registry must not persist anything.
	if err := reg.Upsert("1", testKey("k1")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if v, err := s.GetDiskEntry([]byte("keyStore:registry")); err != nil || v != nil {
		t.Fatalf("memory mode must not write a snapshot: v=%q err=%v", v, err)
	}
}
