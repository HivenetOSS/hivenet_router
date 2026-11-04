// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Tests the opt-in persistence backend for the dynamic key registry
// (--key-store=badger). The persisted registry is a warm-start cache of the
// upstream key-sync service:
//   - provider wiring: mutations persist incrementally; a failed write is
//     reported as ErrNotDurable (→ 503) while the change stays in effect in
//     memory; the store is marked dirty and repaired by a full rewrite
//     (next mutation or background retry);
//   - restore: max-age bound, restored/confirmed state, per-entry skipping,
//     no rewrite on a clean load;
//   - BadgerKeyStore: restart round-trips, secret rotation, ordering under
//     concurrency, directory permissions.
package auth_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/auth"
)

const testSecret = "0123456789abcdef0123456789abcdef-test-jwt-secret"

// ── fake store ──────────────────────────────────────────────────────────────

type storeOp struct {
	kind    string // upsert | delete | replace | touch | reset
	version string
	id      string
	n       int // entries (replace)
}

type fakeKeyStore struct {
	mu        sync.Mutex
	ops       []storeOp
	failNext  int // number of upcoming writes to fail
	failErr   error
	loadSnap  *auth.PersistedRegistry
	loadErr   error
	deleted   []string
	lastFull  []auth.DynamicKeyEntry
	closeCall int
}

func (f *fakeKeyStore) Load() (*auth.PersistedRegistry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadSnap, f.loadErr
}

func (f *fakeKeyStore) write(op storeOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return f.failErr
	}
	f.ops = append(f.ops, op)
	return nil
}

func (f *fakeKeyStore) Upsert(version string, e auth.DynamicKeyEntry, _ time.Time) error {
	return f.write(storeOp{kind: "upsert", version: version, id: e.ID})
}
func (f *fakeKeyStore) Delete(version, id string, _ time.Time) error {
	return f.write(storeOp{kind: "delete", version: version, id: id})
}
func (f *fakeKeyStore) Touch(version string, _, _ time.Time) error {
	return f.write(storeOp{kind: "touch", version: version})
}
func (f *fakeKeyStore) ReplaceAll(version string, entries []auth.DynamicKeyEntry, _, _ time.Time) error {
	if err := f.write(storeOp{kind: "replace", version: version, n: len(entries)}); err != nil {
		return err
	}
	f.mu.Lock()
	f.lastFull = entries
	f.mu.Unlock()
	return nil
}
func (f *fakeKeyStore) Reset(_ time.Time) error {
	return f.write(storeOp{kind: "reset"})
}
func (f *fakeKeyStore) DeleteRecords(ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, ids...)
	return nil
}
func (f *fakeKeyStore) Close() error { f.closeCall++; return nil }

func (f *fakeKeyStore) opsCopy() []storeOp {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storeOp(nil), f.ops...)
}

// ── helpers ─────────────────────────────────────────────────────────────────

func rawKey(id string) string { return "sk-" + id }

func persistEntry(id, owner string) auth.DynamicKeyEntry {
	h := sha256.Sum256([]byte(rawKey(id)))
	return auth.DynamicKeyEntry{
		ID:            id,
		KeyHash:       hex.EncodeToString(h[:]),
		Owner:         owner,
		Name:          id,
		Enabled:       true,
		AllowedModels: []string{"org/model"},
	}
}

func authenticates(p *auth.DynamicKeyProvider, id string) bool {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+rawKey(id))
	_, err := p.Authenticate(req)
	return err == nil
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

// fakeClock is a settable clock for max-age tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func openStore(t *testing.T, dir, secret string) *auth.BadgerKeyStore {
	t.Helper()
	ks, err := auth.OpenBadgerKeyStore(dir, []byte(secret))
	if err != nil {
		t.Fatalf("OpenBadgerKeyStore: %v", err)
	}
	return ks
}

// newPersistent returns a provider wired to a badger store at dir.
func newPersistent(t *testing.T, dir string, clock *fakeClock) *auth.DynamicKeyProvider {
	t.Helper()
	p := auth.NewDynamicKeyProvider()
	if clock != nil {
		p.SetClock(clock.Now)
	}
	p.SetKeyStore(openStore(t, dir, testSecret))
	return p
}

// restart closes p's store and returns a fresh provider loaded from dir.
func restart(t *testing.T, p *auth.DynamicKeyProvider, dir string, clock *fakeClock, opts auth.KeyStoreLoadOptions) (*auth.DynamicKeyProvider, auth.KeyStoreLoadReport) {
	t.Helper()
	if err := p.CloseKeyStore(); err != nil {
		t.Fatalf("CloseKeyStore: %v", err)
	}
	fresh := newPersistent(t, dir, clock)
	t.Cleanup(func() { fresh.CloseKeyStore() }) //nolint:errcheck
	rep, err := fresh.LoadKeyStore(opts)
	if err != nil {
		t.Fatalf("LoadKeyStore: %v", err)
	}
	return fresh, rep
}

// ── provider wiring (fake store) ────────────────────────────────────────────

func TestKeyStore_MutationsPersistIncrementally(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)

	if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := p.Delete("2", "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := p.ReplaceAll("3", []auth.DynamicKeyEntry{persistEntry("a", "o"), persistEntry("b", "o")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	want := []storeOp{
		{kind: "upsert", version: "1", id: "k1"},
		{kind: "delete", version: "2", id: "k1"},
		{kind: "replace", version: "3", n: 2},
	}
	if got := fs.opsCopy(); !reflect.DeepEqual(got, want) {
		t.Fatalf("store ops = %+v, want %+v", got, want)
	}
}

func TestKeyStore_MemoryModePersistsNothing(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if rep, err := p.LoadKeyStore(auth.KeyStoreLoadOptions{}); err != nil || rep.Restored != 0 {
		t.Fatalf("LoadKeyStore without a store must be a no-op: %+v %v", rep, err)
	}
	if st := p.KeyStoreStatus(); st.Persistent || st.Dirty {
		t.Fatalf("memory mode status = %+v", st)
	}
	if err := p.CloseKeyStore(); err != nil {
		t.Fatalf("CloseKeyStore without a store: %v", err)
	}
}

func TestKeyStore_EmptyPerModelRejected(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	e := persistEntry("k1", "org-x")
	e.Quota.PerModel = map[string]auth.PerModelQuotaLimits{}
	if err := p.Upsert("1", e); err == nil {
		t.Fatal("an empty per_model block must be rejected")
	}
}

// A failed write must surface to the caller (→ 503) instead of being
// swallowed, while the change — here a revocation — is already in effect.
func TestKeyStore_PersistFailureReturnsNotDurable(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	p.SetPersistRetryBackoff(time.Hour, time.Hour) // keep the background retry out of the way
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)
	var reported []error
	p.SetOnPersistError(func(err error) { reported = append(reported, err) })

	if err := p.Upsert("1", persistEntry("k1", "org-x")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	fs.mu.Lock()
	fs.failNext, fs.failErr = 1, errors.New("disk full")
	fs.mu.Unlock()

	err := p.Delete("2", "k1")
	if !errors.Is(err, auth.ErrNotDurable) {
		t.Fatalf("Delete with a failing store: err = %v, want ErrNotDurable", err)
	}
	if authenticates(p, "k1") {
		t.Fatal("the revocation must take effect in memory even though it was not persisted")
	}
	st := p.KeyStoreStatus()
	if !st.Dirty || st.PersistErrors != 1 || len(reported) != 1 {
		t.Fatalf("after failure: status=%+v reported=%d, want dirty, 1 error", st, len(reported))
	}
}

// While dirty, the next mutation (e.g. key-sync's equal-version replay)
// rewrites the full registry instead of writing incrementally on top of a
// store that missed earlier changes.
func TestKeyStore_DirtyStoreRepairedByNextMutation(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	p.SetPersistRetryBackoff(time.Hour, time.Hour)
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)

	fs.failNext, fs.failErr = 1, errors.New("io error")
	if err := p.Upsert("1", persistEntry("k1", "o")); !errors.Is(err, auth.ErrNotDurable) {
		t.Fatalf("want ErrNotDurable, got %v", err)
	}
	// Replay of the same version (key-sync retry after the 503).
	if err := p.Upsert("1", persistEntry("k1", "o")); err != nil {
		t.Fatalf("replay: %v", err)
	}
	ops := fs.opsCopy()
	if len(ops) != 1 || ops[0].kind != "replace" || ops[0].n != 1 || ops[0].version != "1" {
		t.Fatalf("dirty store must be repaired by a full rewrite, ops = %+v", ops)
	}
	if p.KeyStoreStatus().Dirty {
		t.Fatal("a successful rewrite must clear dirty")
	}
	// Back to incremental writes.
	if err := p.Upsert("2", persistEntry("k2", "o")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if ops := fs.opsCopy(); ops[len(ops)-1].kind != "upsert" {
		t.Fatalf("clean store must write incrementally, last op = %+v", ops[len(ops)-1])
	}
}

func TestKeyStore_BackgroundRetryRepairsDirtyStore(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	p.SetPersistRetryBackoff(time.Millisecond, 5*time.Millisecond)
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)
	t.Cleanup(func() { p.CloseKeyStore() }) //nolint:errcheck

	fs.failNext, fs.failErr = 3, errors.New("transient") // the mutation + 2 retries fail
	if err := p.Upsert("1", persistEntry("k1", "o")); !errors.Is(err, auth.ErrNotDurable) {
		t.Fatalf("want ErrNotDurable, got %v", err)
	}
	waitUntil(t, "background retry to clear dirty", func() bool { return !p.KeyStoreStatus().Dirty })

	st := p.KeyStoreStatus()
	if st.PersistErrors != 3 || st.LastPersistSuccess.IsZero() {
		t.Fatalf("status after retry = %+v, want 3 errors and a success timestamp", st)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.lastFull) != 1 || fs.lastFull[0].ID != "k1" {
		t.Fatalf("retry must rewrite the current registry, got %+v", fs.lastFull)
	}
}

// ── restore policy (fake store) ─────────────────────────────────────────────

// One bad entry must not brick startup: integrity failures, invalid,
// expired, admission-failing and duplicate-hash records are skipped,
// counted, and deleted; the rest is restored without rewriting the store.
func TestKeyStore_LoadSkipsBadEntries(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	invalid := persistEntry("invalid", "o")
	invalid.Quota.PerModel = map[string]auth.PerModelQuotaLimits{} // rule tightened in a later release
	expired := persistEntry("expired", "o")
	expired.ExpiresAt = &past
	dup := persistEntry("dup", "o")
	dup.KeyHash = persistEntry("good", "o").KeyHash
	noAdmit := persistEntry("no-admit", "o")

	fs := &fakeKeyStore{loadSnap: &auth.PersistedRegistry{
		Version:     "rev_7",
		SavedAt:     time.Now(),
		ConfirmedAt: time.Now(),
		Keys: []auth.PersistedKey{
			{ID: "good", Entry: persistEntry("good", "o")},
			{ID: "tampered", Err: errors.New("integrity check failed")},
			{ID: "invalid", Entry: invalid},
			{ID: "expired", Entry: expired},
			{ID: "dup", Entry: dup},
			{ID: "no-admit", Entry: noAdmit},
		},
	}}
	p := auth.NewDynamicKeyProvider()
	p.SetKeyStore(fs)
	rep, err := p.LoadKeyStore(auth.KeyStoreLoadOptions{
		MaxAge: 15 * time.Minute,
		Admit: func(e auth.DynamicKeyEntry) error {
			if e.ID == "no-admit" {
				return errors.New("ITPM below max input")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("bad entries must not fail startup: %v", err)
	}
	if rep.Restored != 1 || rep.Skipped != 5 || len(rep.SkippedReasons) != 5 {
		t.Fatalf("report = %+v, want 1 restored / 5 skipped", rep)
	}
	if !authenticates(p, "good") {
		t.Fatal("the valid key must be restored")
	}
	if ver, _ := p.Version(); ver != "rev_7" {
		t.Fatalf("version = %q, want rev_7", ver)
	}
	if st := p.KeyStoreStatus(); st.SkippedEntries != 5 || !st.Restored || st.Confirmed {
		t.Fatalf("status = %+v", st)
	}
	if len(fs.deleted) != 5 {
		t.Fatalf("skipped records must be deleted from the store, deleted = %v", fs.deleted)
	}
	if ops := fs.opsCopy(); len(ops) != 0 {
		t.Fatalf("a clean load must not rewrite the store, ops = %+v", ops)
	}
}

func TestKeyStore_LoadErrorFailsClosed(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	p.SetKeyStore(&fakeKeyStore{loadErr: errors.New("key store: corrupt meta")})
	if _, err := p.LoadKeyStore(auth.KeyStoreLoadOptions{}); err == nil {
		t.Fatal("an unreadable snapshot must be returned so startup fails closed")
	}
	if _, n := p.Version(); n != 0 {
		t.Fatalf("registry must stay empty on load error, got %d keys", n)
	}
}

func TestKeyStore_EmptySnapshotRestoresVersion(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	p.SetKeyStore(&fakeKeyStore{loadSnap: &auth.PersistedRegistry{Version: "5", SavedAt: time.Now(), ConfirmedAt: time.Now()}})
	if _, err := p.LoadKeyStore(auth.KeyStoreLoadOptions{MaxAge: time.Minute}); err != nil {
		t.Fatalf("LoadKeyStore: %v", err)
	}
	if err := p.Upsert("4", persistEntry("k1", "o")); !errors.Is(err, auth.ErrStaleVersion) {
		t.Fatalf("a push below the persisted version must be stale after restart, got %v", err)
	}
}

// ── restart behaviour (real badger) ─────────────────────────────────────────

func TestBadgerKeyStore_RestartRestoresAndReplaceAllConfirms(t *testing.T) {
	dir := t.TempDir()
	p := newPersistent(t, dir, nil)
	// key-sync bootstraps with a full replace (this confirms the snapshot),
	// then pushes single-key changes.
	if err := p.ReplaceAll("rev_01", []auth.DynamicKeyEntry{persistEntry("k1", "org-x")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	for i, id := range []string{"k2", "k3"} {
		if err := p.Upsert(fmt.Sprintf("rev_%02d", i+2), persistEntry(id, "org-x")); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
	}
	if err := p.Delete("rev_04", "k2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	fresh, rep := restart(t, p, dir, nil, auth.KeyStoreLoadOptions{MaxAge: 15 * time.Minute})
	if rep.Restored != 2 || rep.Version != "rev_04" || rep.Discarded != "" {
		t.Fatalf("report = %+v", rep)
	}
	if !authenticates(fresh, "k1") || authenticates(fresh, "k2") || !authenticates(fresh, "k3") {
		t.Fatal("restart must restore k1,k3 and keep k2 revoked")
	}
	if st := fresh.KeyStoreStatus(); !st.Restored || st.Confirmed || st.RestoredKeys != 2 {
		t.Fatalf("restored state = %+v, want restored+unconfirmed", st)
	}

	// key-sync's full push confirms the restored state (equal version accepted).
	if err := fresh.ReplaceAll("rev_04", []auth.DynamicKeyEntry{persistEntry("k1", "org-x"), persistEntry("k3", "org-x")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	if !fresh.KeyStoreStatus().Confirmed {
		t.Fatal("ReplaceAll must confirm the restored registry")
	}
}

func TestBadgerKeyStore_OverAgeSnapshotIgnored(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{t: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
	p := newPersistent(t, dir, clock)
	if err := p.ReplaceAll("rev_1", []auth.DynamicKeyEntry{persistEntry("revoked-upstream", "o")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}

	clock.Add(20 * time.Minute) // router was down longer than the max age
	fresh, rep := restart(t, p, dir, clock, auth.KeyStoreLoadOptions{MaxAge: 15 * time.Minute})
	if rep.Discarded == "" || rep.Restored != 0 {
		t.Fatalf("over-age snapshot must be discarded, report = %+v", rep)
	}
	if authenticates(fresh, "revoked-upstream") {
		t.Fatal("a key from an over-age snapshot must not authenticate")
	}
	if ver, n := fresh.Version(); ver != "" || n != 0 {
		t.Fatalf("over-age discard must start empty like memory mode: ver=%q n=%d", ver, n)
	}

	// The discarded keys must not come back through a later write.
	if err := fresh.ReplaceAll("rev_2", []auth.DynamicKeyEntry{persistEntry("new", "o")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	again, _ := restart(t, fresh, dir, clock, auth.KeyStoreLoadOptions{MaxAge: 15 * time.Minute})
	if authenticates(again, "revoked-upstream") || !authenticates(again, "new") {
		t.Fatal("after a discard only post-discard keys may be restored")
	}
}

// saved_at moves forward on every incremental write, so single-key pushes
// must not keep a restored set "young": the max age also applies from the
// last full replace (confirmation).
func TestBadgerKeyStore_MaxAgeMeasuredFromLastConfirmation(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{t: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
	p := newPersistent(t, dir, clock)
	if err := p.ReplaceAll("rev_00", []auth.DynamicKeyEntry{persistEntry("stale", "o")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	for i := 1; i <= 3; i++ { // a single-key push every 10 minutes, no full replace
		clock.Add(10 * time.Minute)
		if err := p.Upsert(fmt.Sprintf("rev_%02d", i), persistEntry(fmt.Sprintf("k%d", i), "o")); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	clock.Add(time.Minute) // last write 1m ago, last confirmation 31m ago
	fresh, rep := restart(t, p, dir, clock, auth.KeyStoreLoadOptions{MaxAge: 15 * time.Minute})
	if rep.Discarded == "" || authenticates(fresh, "stale") {
		t.Fatalf("a snapshot last confirmed 31m ago must be discarded at max age 15m, report = %+v", rep)
	}
}

// A discard must clear the confirmation time: keys pushed one by one after an
// over-age discard were never confirmed by a full replace, so a later restart
// must report them as never confirmed, not inherit the discarded snapshot's
// confirmation.
func TestBadgerKeyStore_DiscardClearsConfirmation(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{t: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
	opts := auth.KeyStoreLoadOptions{MaxAge: 15 * time.Minute}
	p := newPersistent(t, dir, clock)
	if err := p.ReplaceAll("rev_01", []auth.DynamicKeyEntry{persistEntry("old", "o")}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	clock.Add(20 * time.Minute)
	p2, rep := restart(t, p, dir, clock, opts)
	if rep.Discarded == "" {
		t.Fatalf("a snapshot confirmed 20m ago must be discarded at max age 15m, report = %+v", rep)
	}
	if err := p2.Upsert("rev_02", persistEntry("new", "o")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	clock.Add(time.Minute)
	fresh, rep := restart(t, p2, dir, clock, opts)
	if !strings.Contains(rep.Discarded, "never confirmed") || authenticates(fresh, "new") {
		t.Fatalf("keys written after a discard must count as never confirmed, report = %+v", rep)
	}
}

func TestBadgerKeyStore_NeverConfirmedSnapshotDiscarded(t *testing.T) {
	dir := t.TempDir()
	p := newPersistent(t, dir, nil)
	if err := p.Upsert("1", persistEntry("k1", "o")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	fresh, rep := restart(t, p, dir, nil, auth.KeyStoreLoadOptions{MaxAge: time.Hour})
	if rep.Discarded == "" || authenticates(fresh, "k1") {
		t.Fatalf("a never-confirmed snapshot must be discarded when a max age is set, report = %+v", rep)
	}
}

// key-sync re-sends the full set every sweep: an unchanged replace on a clean
// store rewrites only meta; a changed one (or a dirty store) rewrites all.
func TestKeyStore_UnchangedReplaceTouchesMetaOnly(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	p.SetPersistRetryBackoff(time.Hour, time.Hour)
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)
	set := []auth.DynamicKeyEntry{persistEntry("a", "o"), persistEntry("b", "o")}
	for _, v := range []string{"v1", "v2"} {
		if err := p.ReplaceAll(v, append([]auth.DynamicKeyEntry(nil), set...)); err != nil {
			t.Fatalf("ReplaceAll %s: %v", v, err)
		}
	}
	if err := p.ReplaceAll("v3", []auth.DynamicKeyEntry{persistEntry("a", "o")}); err != nil {
		t.Fatalf("ReplaceAll v3: %v", err)
	}
	fs.mu.Lock()
	fs.failNext, fs.failErr = 1, errors.New("io")
	fs.mu.Unlock()
	if err := p.Upsert("v4", persistEntry("a", "o")); !errors.Is(err, auth.ErrNotDurable) {
		t.Fatalf("want ErrNotDurable, got %v", err)
	}
	if err := p.ReplaceAll("v5", []auth.DynamicKeyEntry{persistEntry("a", "o")}); err != nil {
		t.Fatalf("ReplaceAll v5: %v", err)
	}
	var kinds []string
	for _, op := range fs.opsCopy() {
		kinds = append(kinds, op.kind+":"+op.version)
	}
	want := []string{"replace:v1", "touch:v2", "replace:v3", "replace:v5"}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("ops = %v, want %v (unchanged → touch; changed or dirty → full replace)", kinds, want)
	}
}

func TestKeyStore_DegradeRunsMemoryOnly(t *testing.T) {
	p := auth.NewDynamicKeyProvider()
	fs := &fakeKeyStore{}
	p.SetKeyStore(fs)
	p.DegradeKeyStore("open: permission denied")
	st := p.KeyStoreStatus()
	if !st.Degraded || st.Persistent || st.DegradedReason == "" || fs.closeCall != 1 {
		t.Fatalf("status = %+v closeCalls=%d", st, fs.closeCall)
	}
	if err := p.Upsert("1", persistEntry("k1", "o")); err != nil || !authenticates(p, "k1") {
		t.Fatalf("degraded registry must keep working in memory: %v", err)
	}
	if len(fs.opsCopy()) != 0 {
		t.Fatal("degraded registry must not write to the store")
	}
}

func TestBadgerKeyStore_MaxAgeZeroDisablesBound(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{t: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
	p := newPersistent(t, dir, clock)
	if err := p.Upsert("1", persistEntry("k1", "o")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	clock.Add(30 * 24 * time.Hour)
	fresh, rep := restart(t, p, dir, clock, auth.KeyStoreLoadOptions{MaxAge: 0})
	if rep.Restored != 1 || !authenticates(fresh, "k1") {
		t.Fatalf("MaxAge 0 must restore regardless of age, report = %+v", rep)
	}
}

// Changing the key-store secret invalidates every MAC: the snapshot is
// discarded (start empty) rather than failing startup.
func TestBadgerKeyStore_SecretRotationDiscardsSnapshot(t *testing.T) {
	dir := t.TempDir()
	p := newPersistent(t, dir, nil)
	if err := p.Upsert("1", persistEntry("k1", "o")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := p.CloseKeyStore(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rotated := auth.NewDynamicKeyProvider()
	rotated.SetKeyStore(openStore(t, dir, testSecret+"-rotated"))
	t.Cleanup(func() { rotated.CloseKeyStore() }) //nolint:errcheck
	rep, err := rotated.LoadKeyStore(auth.KeyStoreLoadOptions{MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("secret rotation must not fail startup: %v", err)
	}
	if rep.Discarded == "" || authenticates(rotated, "k1") {
		t.Fatalf("snapshot sealed with the old secret must be discarded, report = %+v", rep)
	}
}

func TestBadgerKeyStore_FullFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	exp := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	e := auth.DynamicKeyEntry{
		ID:            "full",
		KeyHash:       persistEntry("full", "o").KeyHash,
		KeyPreview:    "sk-...ull",
		Owner:         "org-full",
		Name:          "Full key",
		Enabled:       true,
		ExpiresAt:     &exp,
		AllowedModels: []string{"a/b", "c/d"},
		Quota: auth.QuotaLimits{
			RequestsPerMinute:     10,
			TokensPerDay:          1000,
			InputTokensPerMinute:  300,
			OutputTokensPerMinute: 400,
			MaxOccupancyShare:     0.25,
			PerModel: map[string]auth.PerModelQuotaLimits{
				"a/b": {RequestsPerMinutePerReplica: 3, TokensPerDay: 50},
			},
		},
	}
	p := newPersistent(t, dir, nil)
	if err := p.Upsert("1", e); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	fresh, _ := restart(t, p, dir, nil, auth.KeyStoreLoadOptions{})
	_, got := fresh.Snapshot()
	if len(got) != 1 || !reflect.DeepEqual(got[0], e) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, e)
	}
}

// Concurrent mutations must reach the store in the order they were applied
// in memory: after a restart the store equals the last in-memory state.
func TestBadgerKeyStore_ConcurrentMutationsStayOrdered(t *testing.T) {
	dir := t.TempDir()
	p := newPersistent(t, dir, nil)
	var wg sync.WaitGroup
	var verMu sync.Mutex
	seq := 0
	next := func() string { verMu.Lock(); defer verMu.Unlock(); seq++; return fmt.Sprintf("rev_%06d", seq) }
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				id := fmt.Sprintf("k%d", (w*25+i)%40)
				if i%3 == 2 {
					_ = p.Delete(next(), id) // may be stale under contention — fine
				} else {
					_ = p.Upsert(next(), persistEntry(id, "o"))
				}
			}
		}(w)
	}
	wg.Wait()
	wantVer, want := p.Snapshot()
	fresh, _ := restart(t, p, dir, nil, auth.KeyStoreLoadOptions{})
	gotVer, got := fresh.Snapshot()
	if gotVer != wantVer || !reflect.DeepEqual(got, want) {
		t.Fatalf("restored state diverges from the last in-memory state: ver %q vs %q, %d vs %d keys", gotVer, wantVer, len(got), len(want))
	}
}

func TestBadgerKeyStore_DirectoryIsPrivate(t *testing.T) {
	dir := t.TempDir() + "/ks"
	ks := openStore(t, dir, testSecret)
	defer ks.Close() //nolint:errcheck
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("key store dir mode = %o, want 700", perm)
	}
}

func TestBadgerKeyStore_FreshStoreLoadsNothing(t *testing.T) {
	p := newPersistent(t, t.TempDir(), nil)
	t.Cleanup(func() { p.CloseKeyStore() }) //nolint:errcheck
	rep, err := p.LoadKeyStore(auth.KeyStoreLoadOptions{MaxAge: time.Minute})
	if err != nil || rep.Restored != 0 || rep.Discarded != "" || p.KeyStoreStatus().Restored {
		t.Fatalf("fresh store: rep=%+v err=%v", rep, err)
	}
}
