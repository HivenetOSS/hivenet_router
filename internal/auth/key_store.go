// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// The persisted registry is a WARM-START CACHE of the upstream key-sync
// service (the source of truth), never a second source of truth. It lets a
// restarted router serve keys immediately instead of 401-ing until key-sync
// re-pushes. Everything below follows from that framing:
//   - a snapshot older than the configured max age is discarded (a key revoked
//     upstream while the router was down must not authenticate indefinitely);
//   - a restored registry is "unconfirmed" until the first ReplaceAll from
//     key-sync, and that state is observable (version endpoint + metric);
//   - a mutation that could not be persisted is reported to the caller
//     (ErrNotDurable → HTTP 503) so key-sync retries it, instead of
//     acknowledging a revocation that would come back after a restart;
//   - a broken cache never takes the router down: when the store cannot be
//     opened or read, the registry runs memory-only ("degraded") and starts
//     empty, which denies every key until key-sync pushes — exactly memory
//     mode. Only configuration errors stop startup.

// ErrNotDurable is returned (wrapped) by Upsert/Delete/ReplaceAll when the
// change was applied in memory — and is already in effect — but could not be
// written to the key store. The registry is marked dirty and a background
// retry rewrites the full registry until it succeeds; a replay of the same
// mutation (equal version is accepted) also retries the write.
var ErrNotDurable = errors.New("auth: key registry change applied in memory but not persisted")

// KeyStore is a persistence backend for the dynamic key registry. Calls are
// serialized by the provider (persistMu), but implementations must still be
// safe for concurrent use.
type KeyStore interface {
	// Load returns the persisted registry, or (nil, nil) for a fresh store.
	// An error means the backend could not be read (the caller degrades to
	// memory-only). An unusable snapshot is reported in
	// PersistedRegistry.Unusable, per-record problems in PersistedKey.Err.
	Load() (*PersistedRegistry, error)
	// Upsert durably writes one key and the registry version.
	Upsert(version string, e DynamicKeyEntry, savedAt time.Time) error
	// Delete durably removes one key and writes the registry version.
	Delete(version, id string, savedAt time.Time) error
	// ReplaceAll durably and atomically replaces the whole registry. A
	// non-zero confirmedAt records a full replace from upstream key-sync; zero
	// keeps the stored confirmation time (internal rewrites, resets).
	ReplaceAll(version string, entries []DynamicKeyEntry, savedAt, confirmedAt time.Time) error
	// Touch rewrites only the registry version and timestamps (a full
	// replace whose key set is already stored). Zero confirmedAt keeps it.
	Touch(version string, savedAt, confirmedAt time.Time) error
	// Reset durably replaces the store with an empty registry (version "")
	// and clears the confirmation time. Used when a snapshot is discarded, so
	// the discarded snapshot's confirmation cannot carry over to keys written
	// afterwards.
	Reset(savedAt time.Time) error
	// DeleteRecords removes individual records (entries skipped at load)
	// without touching the version.
	DeleteRecords(ids []string) error
	// Close releases the backend.
	Close() error
}

// PersistedRegistry is what KeyStore.Load returns.
type PersistedRegistry struct {
	Version     string
	SavedAt     time.Time // last write
	ConfirmedAt time.Time // last full replace from upstream key-sync (zero = never)
	Keys        []PersistedKey
	// Unusable, when non-empty, says why the snapshot as a whole cannot be
	// used (meta integrity failure, corrupt meta, another schema version).
	// The provider discards it and rewrites the store.
	Unusable string
	// CleanupErr reports a failed removal of leftover records (non-fatal).
	CleanupErr error
}

// PersistedKey is one persisted record. Err is non-nil when the record could
// not be verified or decoded; Entry is then zero.
type PersistedKey struct {
	ID    string
	Entry DynamicKeyEntry
	Err   error
}

// KeyStoreLoadOptions controls how LoadKeyStore restores a snapshot.
type KeyStoreLoadOptions struct {
	// MaxAge discards a snapshot whose last write OR last confirmation by
	// upstream key-sync (whichever is older) is older than this, and one that
	// was never confirmed (start empty, exactly like memory mode). Measuring
	// from the confirmation too stops single-key pushes from keeping a
	// never-confirmed restored set "young" across restarts. 0 disables the
	// check — only for deployments with no upstream key-sync, where the
	// snapshot IS the data.
	MaxAge time.Duration
	// Admit runs the same cross-config admission check the admin handlers run
	// on writes (validateKeyAdmission), against the policies in force now. A
	// restored key that fails it is skipped. Nil = no check.
	Admit func(DynamicKeyEntry) error
}

// KeyStoreLoadReport summarizes a LoadKeyStore call (for logging).
type KeyStoreLoadReport struct {
	Restored    int           // keys installed into the registry
	Skipped     int           // records dropped (see SkippedReasons)
	Discarded   string        // non-empty when the whole snapshot was ignored, with the reason
	SnapshotAge time.Duration // age of the restored snapshot (from the older of last write / last confirmation)
	Version     string        // restored registry version
	// SkippedReasons maps record ID → why it was skipped.
	SkippedReasons map[string]string
}

// KeyStoreStatus is a point-in-time view of persistence and restore state,
// exported as metrics and on GET /admin/api-keys/version.
type KeyStoreStatus struct {
	Persistent          bool // a KeyStore is wired
	Degraded            bool // badger was configured but the store is unusable; running memory-only
	DegradedReason      string
	Dirty               bool      // the last write failed; the store lags memory
	PersistErrors       uint64    // total failed writes
	LastPersistSuccess  time.Time // zero = never
	Restored            bool      // the registry was restored from the store at startup
	RestoredKeys        int64     // keys installed by that restore
	RestoredSnapshotAge time.Duration
	Confirmed           bool   // a ReplaceAll from key-sync has been applied since startup
	SkippedEntries      uint64 // records dropped at load
}

// keyStoreState holds the provider's persistence bookkeeping. The atomics are
// read lock-free by metrics scrapes; retrying/closed are guarded by persistMu.
type keyStoreState struct {
	dirty          atomic.Bool
	persistErrors  atomic.Uint64
	lastPersistOK  atomic.Int64 // unix nanos; 0 = never
	skipped        atomic.Uint64
	restored       atomic.Bool
	restoredKeys   atomic.Int64
	restoredAgeNs  atomic.Int64
	confirmed      atomic.Bool
	degraded       atomic.Bool
	degradedReason atomic.Value // string
	retrying       bool
	closed         bool
	stop           chan struct{}
	retryMin       time.Duration
	retryMax       time.Duration
	retryCompleted chan struct{} // test hook: signalled after each retry attempt (nil = off)
}

// SetKeyStore wires a persistence backend into the registry. Must be called
// once before the registry serves traffic (i.e. before the admin endpoints
// are exposed). A nil store (the default) keeps the registry memory-only.
func (r *DynamicKeyProvider) SetKeyStore(s KeyStore) {
	r.store = s
	r.ks.stop = make(chan struct{})
	if r.ks.retryMin == 0 {
		r.ks.retryMin, r.ks.retryMax = time.Second, 30*time.Second
	}
}

// SetPersistRetryBackoff overrides the background retry backoff (tests).
func (r *DynamicKeyProvider) SetPersistRetryBackoff(initial, ceiling time.Duration) {
	r.ks.retryMin, r.ks.retryMax = initial, ceiling
}

// SetOnPersistError registers the sink for persistence failures (e.g. a log
// line). It is called for every failed write, including background retries.
func (r *DynamicKeyProvider) SetOnPersistError(fn func(error)) { r.onPersistError = fn }

// CloseKeyStore stops the background retry and closes the wired store. Safe
// to call when no store is wired. Mutations after Close are applied in
// memory only and report ErrNotDurable.
func (r *DynamicKeyProvider) CloseKeyStore() error {
	if r.store == nil {
		return nil
	}
	r.persistMu.Lock()
	defer r.persistMu.Unlock()
	if r.ks.closed {
		return nil
	}
	r.ks.closed = true
	close(r.ks.stop)
	return r.store.Close()
}

// DegradeKeyStore switches the registry to memory-only after the configured
// key store turned out to be unusable at startup (cannot be opened, read, or
// reset). It closes the store if one is wired, empties the registry, and
// records reason for the version endpoint and the degraded metric. Must be
// called before the registry serves traffic.
func (r *DynamicKeyProvider) DegradeKeyStore(reason string) {
	if r.store != nil {
		r.CloseKeyStore() //nolint:errcheck // already unusable
		r.store = nil
	}
	r.mu.Lock()
	r.byHash = make(map[string]*DynamicKeyEntry)
	r.byID = make(map[string]*DynamicKeyEntry)
	r.version = ""
	r.mu.Unlock()
	r.ks.restored.Store(false)
	r.ks.restoredKeys.Store(0)
	r.ks.restoredAgeNs.Store(0)
	r.ks.degradedReason.Store(reason)
	r.ks.degraded.Store(true)
}

// KeyStoreStatus returns the current persistence/restore state.
func (r *DynamicKeyProvider) KeyStoreStatus() KeyStoreStatus {
	reason, _ := r.ks.degradedReason.Load().(string)
	st := KeyStoreStatus{
		Persistent:          r.store != nil,
		Degraded:            r.ks.degraded.Load(),
		DegradedReason:      reason,
		Dirty:               r.ks.dirty.Load(),
		PersistErrors:       r.ks.persistErrors.Load(),
		Restored:            r.ks.restored.Load(),
		RestoredKeys:        r.ks.restoredKeys.Load(),
		RestoredSnapshotAge: time.Duration(r.ks.restoredAgeNs.Load()),
		Confirmed:           r.ks.confirmed.Load(),
		SkippedEntries:      r.ks.skipped.Load(),
	}
	if ns := r.ks.lastPersistOK.Load(); ns != 0 {
		st.LastPersistSuccess = time.Unix(0, ns)
	}
	return st
}

// LoadKeyStore restores the persisted registry into this (empty) provider.
// No store wired or nothing persisted → zero report, nil error.
//
// An error means the store cannot be read, or an unusable snapshot could not
// be reset; the caller degrades to memory-only (DegradeKeyStore) and keeps
// serving. Everything below that level is handled here:
//   - an unusable snapshot (meta MAC failure, corrupt meta, older or newer
//     schema — e.g. after a release rollback), an over-age one, or one never
//     confirmed by key-sync is discarded as a whole and the store is reset to
//     an empty generation, so a later incremental write cannot resurrect it;
//   - an individual record that fails its MAC, does not decode, fails field
//     validation, is expired, fails the admission check, or duplicates an
//     earlier key's hash is skipped, counted, and deleted from the store.
//
// The snapshot is NOT rewritten on a clean load.
func (r *DynamicKeyProvider) LoadKeyStore(opts KeyStoreLoadOptions) (KeyStoreLoadReport, error) {
	var rep KeyStoreLoadReport
	if r.store == nil {
		return rep, nil
	}
	r.persistMu.Lock()
	defer r.persistMu.Unlock()

	snap, err := r.store.Load()
	if err != nil {
		return rep, err
	}
	if snap == nil {
		return rep, nil
	}
	now := r.now()

	if snap.CleanupErr != nil && r.onPersistError != nil {
		r.onPersistError(snap.CleanupErr)
	}
	discard := func(reason string) (KeyStoreLoadReport, error) {
		rep.Discarded = reason
		if err := r.store.Reset(now); err != nil {
			return rep, fmt.Errorf("key store: snapshot discarded (%s) but resetting it failed: %w", reason, err)
		}
		r.ks.lastPersistOK.Store(now.UnixNano())
		return rep, nil
	}
	if snap.Unusable != "" {
		return discard(snap.Unusable)
	}
	ref := snap.SavedAt
	if !snap.ConfirmedAt.IsZero() && snap.ConfirmedAt.Before(ref) {
		ref = snap.ConfirmedAt
	}
	age := now.Sub(ref)
	if age < 0 {
		age = 0
	}
	rep.SnapshotAge = age
	if opts.MaxAge > 0 {
		if snap.ConfirmedAt.IsZero() {
			return discard("snapshot was never confirmed by a full replace from the upstream key-sync service")
		}
		if age > opts.MaxAge {
			return discard(fmt.Sprintf("snapshot is %s old (older of last write and last key-sync confirmation), older than the configured max age %s", age.Round(time.Second), opts.MaxAge))
		}
	}

	byHash := make(map[string]*DynamicKeyEntry, len(snap.Keys))
	byID := make(map[string]*DynamicKeyEntry, len(snap.Keys))
	var skipIDs []string
	skip := func(id, reason string) {
		if rep.SkippedReasons == nil {
			rep.SkippedReasons = make(map[string]string)
		}
		rep.SkippedReasons[id] = reason
		skipIDs = append(skipIDs, id)
	}
	for _, pk := range snap.Keys {
		if pk.Err != nil {
			skip(pk.ID, pk.Err.Error())
			continue
		}
		e := pk.Entry
		if err := normalizeAndValidateKeyEntry(&e); err != nil {
			skip(pk.ID, "invalid: "+err.Error())
			continue
		}
		if e.ExpiresAt != nil && !now.Before(*e.ExpiresAt) {
			skip(pk.ID, "expired")
			continue
		}
		if opts.Admit != nil {
			if err := opts.Admit(e); err != nil {
				skip(pk.ID, "admission: "+err.Error())
				continue
			}
		}
		if other, ok := byHash[e.KeyHash]; ok {
			skip(pk.ID, fmt.Sprintf("duplicate key_hash (already restored for id %q)", other.ID))
			continue
		}
		entry := e
		byHash[entry.KeyHash] = &entry
		byID[entry.ID] = &entry
	}
	rep.Skipped = len(skipIDs)
	rep.Restored = len(byID)
	rep.Version = snap.Version
	if len(skipIDs) > 0 {
		r.ks.skipped.Add(uint64(len(skipIDs)))
		// Best-effort cleanup: a record left behind is skipped again next
		// time, so a failure here is logged by the caller, not fatal.
		if err := r.store.DeleteRecords(skipIDs); err != nil && r.onPersistError != nil {
			r.onPersistError(fmt.Errorf("key store: removing %d skipped record(s): %w", len(skipIDs), err))
		}
	}

	r.mu.Lock()
	r.byHash, r.byID = byHash, byID
	// The version is restored even when no key survived: otherwise a stale
	// push below the persisted version would be accepted after a restart.
	if snap.Version > r.version {
		r.version = snap.Version
	}
	r.mu.Unlock()

	r.ks.restored.Store(true)
	r.ks.restoredKeys.Store(int64(rep.Restored))
	r.ks.restoredAgeNs.Store(int64(age))
	r.ks.confirmed.Store(false)
	r.ks.lastPersistOK.Store(snap.SavedAt.UnixNano())
	return rep, nil
}

// lockPersist serializes a whole mutation (memory change + store write) when
// a store is wired, so writes reach the store in the same order the
// mutations were applied in memory. Persistence is incremental per key now,
// so ordering is what prevents a stale write (e.g. an Upsert overtaken by a
// later Delete) from resurrecting a revoked key on disk. Authenticate never
// takes persistMu, so a slow disk only delays other admin mutations.
func (r *DynamicKeyProvider) lockPersist() func() {
	if r.store == nil {
		return func() {}
	}
	r.persistMu.Lock()
	return r.persistMu.Unlock
}

// persistLocked runs one store write for a mutation already applied in
// memory. Caller holds persistMu. When the store is dirty (an earlier write
// failed) the incremental write is replaced by a full rewrite of the current
// registry, which also repairs everything the failed writes missed; confirm
// marks that rewrite as a key-sync confirmation (the mutation is a ReplaceAll).
func (r *DynamicKeyProvider) persistLocked(confirm bool, write func(s KeyStore, now time.Time) error) error {
	if r.store == nil {
		return nil
	}
	now := r.now()
	var err error
	switch {
	case r.ks.closed:
		err = errors.New("key store closed")
	case r.ks.dirty.Load():
		var confirmedAt time.Time
		if confirm {
			confirmedAt = now
		}
		err = r.rewriteLocked(now, confirmedAt)
	default:
		err = write(r.store, now)
	}
	if err != nil {
		r.markDirtyLocked(err)
		return fmt.Errorf("%w: %v", ErrNotDurable, err)
	}
	r.ks.dirty.Store(false)
	r.ks.lastPersistOK.Store(now.UnixNano())
	return nil
}

func (r *DynamicKeyProvider) rewriteLocked(now, confirmedAt time.Time) error {
	ver, entries := r.Snapshot()
	return r.store.ReplaceAll(ver, entries, now, confirmedAt)
}

func (r *DynamicKeyProvider) markDirtyLocked(err error) {
	r.ks.dirty.Store(true)
	r.ks.persistErrors.Add(1)
	if r.onPersistError != nil {
		r.onPersistError(err)
	}
	if !r.ks.retrying && !r.ks.closed {
		r.ks.retrying = true
		go r.retryLoop()
	}
}

// retryLoop rewrites the full registry with exponential backoff until a write
// succeeds (or a mutation's own write clears the dirty flag first), then exits.
func (r *DynamicKeyProvider) retryLoop() {
	delay := r.ks.retryMin
	for {
		select {
		case <-r.ks.stop:
			r.persistMu.Lock()
			r.ks.retrying = false
			r.persistMu.Unlock()
			return
		case <-time.After(delay):
		}
		r.persistMu.Lock()
		if r.ks.closed || !r.ks.dirty.Load() {
			r.ks.retrying = false
			r.persistMu.Unlock()
			r.signalRetry()
			return
		}
		now := r.now()
		if err := r.rewriteLocked(now, time.Time{}); err != nil {
			r.ks.persistErrors.Add(1)
			if r.onPersistError != nil {
				r.onPersistError(fmt.Errorf("background retry: %w", err))
			}
		} else {
			r.ks.dirty.Store(false)
			r.ks.lastPersistOK.Store(now.UnixNano())
			r.ks.retrying = false
			r.persistMu.Unlock()
			r.signalRetry()
			return
		}
		r.persistMu.Unlock()
		r.signalRetry()
		if delay *= 2; delay > r.ks.retryMax {
			delay = r.ks.retryMax
		}
	}
}

func (r *DynamicKeyProvider) signalRetry() {
	if r.ks.retryCompleted != nil {
		select {
		case r.ks.retryCompleted <- struct{}{}:
		default:
		}
	}
}
