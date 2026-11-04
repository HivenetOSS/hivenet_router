// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"

	"github.com/dgraph-io/badger/v4"
)

const (
	wiringSecret   = "router-wiring-test-jwt-secret-0123456789"
	keyStoreSecret = "router-only-key-store-secret-0123456789ab"
)

func newKeyStoreCfg(t *testing.T) *config.Config {
	t.Helper()
	root := t.TempDir()
	return &config.Config{
		KeyStoreBackend: "badger",
		KeyStorePath:    filepath.Join(root, "keystore"),
		KeyStoreMaxAge:  15 * time.Minute,
		DiskDBPath:      filepath.Join(root, "disk"),
		JWTSecret:       wiringSecret,
		KeyStoreSecret:  keyStoreSecret,
	}
}

func testKey(id string) auth.DynamicKeyEntry {
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

// wire wires a fresh registry and registers its store for cleanup.
func wire(t *testing.T, cfg *config.Config, admit func(auth.DynamicKeyEntry) error) (*auth.DynamicKeyProvider, error) {
	t.Helper()
	reg := auth.NewDynamicKeyProvider()
	err := wireKeyStore(cfg, reg, admit)
	t.Cleanup(func() { reg.CloseKeyStore() }) //nolint:errcheck
	return reg, err
}

func TestWireKeyStore_InvalidBackendRejected(t *testing.T) {
	cfg := newKeyStoreCfg(t)
	cfg.KeyStoreBackend = "bogus"
	if _, err := wire(t, cfg, nil); err == nil || !strings.Contains(err.Error(), "invalid KeyStoreBackend") {
		t.Fatalf("invalid backend must be rejected at startup, got: %v", err)
	}
}

func TestWireKeyStore_BadgerRequiresDynamicRegistry(t *testing.T) {
	if err := wireKeyStore(newKeyStoreCfg(t), nil, nil); err == nil ||
		!strings.Contains(err.Error(), "requires dynamic auth mode") {
		t.Fatalf("badger without a dynamic registry must fail, got: %v", err)
	}
}

func TestWireKeyStore_PathMustNotOverlapDiskDB(t *testing.T) {
	for name, mut := range map[string]func(*config.Config){
		"same":   func(c *config.Config) { c.KeyStorePath = c.DiskDBPath },
		"inside": func(c *config.Config) { c.KeyStorePath = filepath.Join(c.DiskDBPath, "keys") },
		"parent": func(c *config.Config) { c.DiskDBPath = filepath.Join(c.KeyStorePath, "disk") },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := newKeyStoreCfg(t)
			mut(cfg)
			if _, err := wire(t, cfg, nil); err == nil || !strings.Contains(err.Error(), "overlaps") {
				t.Fatalf("overlapping paths must be rejected, got: %v", err)
			}
		})
	}
	if pathsOverlap("/data/disk", "/data/disk-keys") {
		t.Fatal("sibling directories sharing a name prefix do not overlap")
	}
}

// sealMeta writes a meta record sealed with the configured key-store secret
// but carrying the given payload, the way another router version (or a
// corruption) would leave it.
func sealMeta(t *testing.T, cfg *config.Config, payload []byte) {
	t.Helper()
	macKey, err := auth.DeriveKeyStoreMACKey([]byte(cfg.KeyStoreSecret))
	if err != nil {
		t.Fatal(err)
	}
	db, err := badger.Open(badger.DefaultOptions(cfg.KeyStorePath).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	key := []byte("keyStore:v1:meta")
	m := hmac.New(sha256.New, macKey)
	m.Write(key)
	m.Write([]byte{0})
	m.Write(payload)
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(key, append(m.Sum(nil), payload...)) }); err != nil {
		t.Fatal(err)
	}
}

// A broken cache must never take the router down: a newer schema (release
// rollback), an older one, or a corrupt meta record is discarded and the
// store rewritten in the current schema; the router starts empty.
func TestWireKeyStore_UnusableSnapshotDiscardedNotFatal(t *testing.T) {
	for name, payload := range map[string]string{
		"rollback: newer schema": `{"schema":999,"gen":7,"version":"rev_9"}`,
		"older schema":           `{"schema":1,"gen":1,"version":"rev_1"}`,
		"corrupt meta":           `{not-json`,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := newKeyStoreCfg(t)
			sealMeta(t, cfg, []byte(payload))
			reg, err := wire(t, cfg, nil)
			if err != nil {
				t.Fatalf("an unusable snapshot must not stop the router: %v", err)
			}
			if ver, n := reg.Version(); ver != "" || n != 0 {
				t.Fatalf("must start empty, got ver=%q n=%d", ver, n)
			}
			if st := reg.KeyStoreStatus(); st.Degraded || !st.Persistent {
				t.Fatalf("the store is usable after the rewrite, status = %+v", st)
			}
			// The rewritten store loads cleanly on the next start.
			reg.CloseKeyStore() //nolint:errcheck
			ks, err := auth.OpenBadgerKeyStore(cfg.KeyStorePath, []byte(cfg.KeyStoreSecret))
			if err != nil {
				t.Fatal(err)
			}
			defer ks.Close() //nolint:errcheck
			if snap, err := ks.Load(); err != nil || snap == nil || snap.Unusable != "" {
				t.Fatalf("store must be rewritten in the current schema, got %+v err=%v", snap, err)
			}
		})
	}
}

// When the store cannot even be opened, the router keeps serving
// memory-only (degraded) instead of crash-looping.
func TestWireKeyStore_UnopenableStoreDegrades(t *testing.T) {
	cfg := newKeyStoreCfg(t)
	if err := os.WriteFile(cfg.KeyStorePath, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := wire(t, cfg, nil)
	if err != nil {
		t.Fatalf("an unopenable store must not stop the router: %v", err)
	}
	st := reg.KeyStoreStatus()
	if !st.Degraded || st.Persistent || st.DegradedReason == "" {
		t.Fatalf("status = %+v, want degraded memory-only", st)
	}
	if err := reg.ReplaceAll("rev_1", []auth.DynamicKeyEntry{testKey("k1")}); err != nil {
		t.Fatalf("degraded registry must accept key-sync pushes: %v", err)
	}
}

func TestWireKeyStore_ConfigErrorsStopStartup(t *testing.T) {
	for name, tc := range map[string]struct {
		mut  func(*config.Config)
		want string
	}{
		"missing secret":   {func(c *config.Config) { c.KeyStoreSecret = "" }, "router-only secret is required"},
		"short secret":     {func(c *config.Config) { c.KeyStoreSecret = "short" }, "too short"},
		"secret == JWT":    {func(c *config.Config) { c.KeyStoreSecret = c.JWTSecret }, "must differ from the JWT secret"},
		"negative max age": {func(c *config.Config) { c.KeyStoreMaxAge = -time.Minute }, "max-age must be >= 0"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := newKeyStoreCfg(t)
			tc.mut(cfg)
			if _, err := wire(t, cfg, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestWireKeyStore_RestoresAndAppliesAdmission(t *testing.T) {
	cfg := newKeyStoreCfg(t)
	reg, err := wire(t, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.ReplaceAll("1", []auth.DynamicKeyEntry{testKey("k1"), testKey("k2")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	reg.CloseKeyStore() //nolint:errcheck

	// Restart under a policy change that k2 no longer satisfies.
	fresh, err := wire(t, cfg, func(e auth.DynamicKeyEntry) error {
		if e.ID == "k2" {
			return errors.New("ITPM below max input")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("wireKeyStore: %v", err)
	}
	ver, entries := fresh.Snapshot()
	if ver != "1" || len(entries) != 1 || entries[0].ID != "k1" {
		t.Fatalf("restore mismatch: ver=%q entries=%+v (k2 must be skipped by admission)", ver, entries)
	}
	if st := fresh.KeyStoreStatus(); !st.Restored || st.Confirmed || st.SkippedEntries != 1 {
		t.Fatalf("status = %+v", st)
	}
}

func TestWireKeyStore_ResetWipesOnlyKeyStore(t *testing.T) {
	cfg := newKeyStoreCfg(t)
	reg, err := wire(t, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Upsert("1", testKey("k1")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	reg.CloseKeyStore() //nolint:errcheck
	if err := os.MkdirAll(cfg.DiskDBPath, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(cfg.DiskDBPath, "history-marker")
	if err := os.WriteFile(marker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg.ResetKeyStore = true
	fresh, err := wire(t, cfg, nil)
	if err != nil {
		t.Fatalf("wireKeyStore with reset: %v", err)
	}
	if ver, entries := fresh.Snapshot(); ver != "" || len(entries) != 0 {
		t.Fatalf("reset must wipe the key store: ver=%q entries=%d", ver, len(entries))
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("--reset-key-store must not touch the history diskDB: %v", err)
	}
}

func TestWireKeyStore_MemoryModeNoOp(t *testing.T) {
	cfg := newKeyStoreCfg(t)
	cfg.KeyStoreBackend = "memory"
	reg, err := wire(t, cfg, nil)
	if err != nil {
		t.Fatalf("memory mode must be a no-op: %v", err)
	}
	if err := reg.Upsert("1", testKey("k1")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := os.Stat(cfg.KeyStorePath); !os.IsNotExist(err) {
		t.Fatalf("memory mode must not create a key store, stat err = %v", err)
	}
}
