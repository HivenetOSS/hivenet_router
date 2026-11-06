// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"hivenet_router/internal/config"

	"golang.org/x/time/rate"
)

// DailyQuotaStore persists per-key daily token usage to durable storage.
// Implemented by *storage.BadgerStorage; the interface avoids an import cycle
// since Go satisfies interfaces implicitly.
type DailyQuotaStore interface {
	GetTokensUsed(key string) (int64, error)
	SetTokensUsed(key string, count int64, ttl time.Duration) error
}

// RateLimiter enforces per-key quotas: requests per minute and daily token
// budget. All methods are safe for concurrent use.
//
// Every bucket belongs to one key: limits are configured per key, so each key
// is counted against its own ceiling and never drains another key's budget,
// even within one tenant. tenantID is part of the bucket too, so a key ID
// reassigned to another tenant starts fresh, and per-tenant usage can be
// reported. The model parameter selects the bucket shape: an empty string
// uses the flat per-key bucket; a non-empty string uses a per-(key, model)
// bucket. Callers driving the flat shape pass ""; callers driving the
// per-model shape pass the requested model name.
type RateLimiter interface {
	// AllowRequest checks and deducts 1 unit from the key's RPM bucket for
	// the given model. Returns (true, remaining, nil) when allowed; (false, 0,
	// nil) when the bucket is exhausted; remaining is -1 when requestsPerMinute
	// is 0 (unlimited).
	AllowRequest(tenantID, keyID, model string, requestsPerMinute int) (allowed bool, remaining int, err error)

	// AllowInputTokens checks and deducts count tokens from the key's daily
	// budget for the given model. Called before the request is forwarded to an
	// agent (prompt estimation).
	AllowInputTokens(tenantID, keyID, model string, tokensPerDay, count int) (allowed bool, remaining int, err error)

	// AllowOutputTokens checks and deducts count tokens from the key's daily
	// budget for the given model. Called after the agent returns a response
	// (actual completion token count).
	AllowOutputTokens(tenantID, keyID, model string, tokensPerDay, count int) (allowed bool, remaining int, err error)

	// RemainingTokens reports the remaining daily budget for (key, model)
	// WITHOUT deducting anything. Returns -1 when tokensPerDay is 0 (unlimited).
	// Used by the pre-queue admission check so an over-budget request can be
	// rejected without being charged.
	RemainingTokens(tenantID, keyID, model string, tokensPerDay int) (remaining int, err error)

	// Reset clears all cached limiter state. Call on SIGHUP after auth config reload
	// so updated quota limits take effect immediately.
	Reset()
}

// bucketKey builds the composite limiter-bucket key: tenant, key and, for the
// per-model shape, model, joined by NUL. An empty model gives the flat per-key
// bucket. The separator is unambiguous: key IDs and owners are validated to
// contain no control characters (validateIdentifier), and model names are
// printable.
func bucketKey(tenantID, keyID, model string) string {
	k := tenantID + "\x00" + keyID
	if model == "" {
		return k
	}
	return k + "\x00" + model
}

// NewRateLimiter builds a RateLimiter based on cfg.QuotaBackend.
//
//   - "memory" (default): in-process counters only; state is lost on restart.
//   - "badger": in-process counters with periodic flush to BadgerDB diskDB;
//     token usage survives router restarts.
//
// store may be nil when QuotaBackend is "memory" or empty.
func NewRateLimiter(cfg *config.Config, store DailyQuotaStore) (RateLimiter, error) {
	switch cfg.QuotaBackend {
	case "", "memory":
		l := NewInMemoryLimiter()
		l.SetRPMBurstWindow(cfg.RPMBurstSeconds)
		return l, nil
	case "badger":
		if store == nil {
			return nil, fmt.Errorf("quota backend 'badger' requires a DailyQuotaStore (BadgerStorage must be initialised first)")
		}
		l := NewBadgerLimiter(store)
		l.SetRPMBurstWindow(cfg.RPMBurstSeconds)
		return l, nil
	default:
		return nil, fmt.Errorf("unknown quota backend %q: valid values are 'memory' (default) or 'badger'", cfg.QuotaBackend)
	}
}

// ── InMemoryLimiter ──────────────────────────────────────────────────────────

// dailyBucket tracks token consumption for one bucket (a key, or a key and
// model) — or, in tenantUsage, a whole tenant — within one calendar day.
type dailyBucket struct {
	mu   sync.Mutex
	used int
	day  int // UTC days since Unix epoch
}

// InMemoryLimiter enforces quotas entirely in process memory.
// RPM enforcement uses golang.org/x/time/rate token buckets.
// Daily token enforcement uses calendar-day counters that reset at midnight UTC.
type InMemoryLimiter struct {
	rpmLimiters  sync.Map // bucketKey → *rate.Limiter
	dailyBuckets sync.Map // bucketKey → *dailyBucket

	// tenantUsage sums today's token usage across all of a tenant's keys and
	// models (tenantID → *dailyBucket). It is reporting only — no limit is
	// checked against it — and feeds onTokensUsed.
	tenantUsage sync.Map

	// onTokensUsed is called after every change to a tenant's usage with the
	// tenant ID and its cumulative tokens used today across all keys and
	// models. Nil-safe. Set once at startup via SetOnTokensUsed before any
	// requests arrive.
	onTokensUsed func(tenantID string, used int)

	// rpmBurstSeconds sizes the per-tenant RPM token bucket's burst as that many
	// seconds of the rate, instead of a full minute. 0 (or >= 60) keeps the
	// legacy full-minute burst. A short certified window (e.g. 10s) closes the
	// hole where a tenant could spend its entire minute's quota in one instant.
	// Set once at startup via SetRPMBurstWindow before any request is handled.
	rpmBurstSeconds int
}

// SetRPMBurstWindow sets the RPM burst window in seconds (see rpmBurstSeconds).
// Must be called before the limiter handles any requests.
func (l *InMemoryLimiter) SetRPMBurstWindow(seconds int) {
	l.rpmBurstSeconds = seconds
}

// rpmBurst returns the token-bucket burst for a per-minute request rate. With a
// window of 0 or >= 60 the burst is a full minute's quota (legacy x/time/rate
// default). A shorter window enforces the certified burst instead —
// rpm × window / 60, floored at 1.
func (l *InMemoryLimiter) rpmBurst(rpm int) int {
	if l.rpmBurstSeconds <= 0 || l.rpmBurstSeconds >= 60 {
		return rpm
	}
	if b := rpm * l.rpmBurstSeconds / 60; b > 1 {
		return b
	}
	return 1
}

// SetOnTokensUsed registers a callback that fires after every change to a
// tenant's usage. The callback receives the tenant ID and the tenant's
// cumulative tokens used today, summed over all its keys and models.
// Intended for updating the hivenet_tenant_tokens_used_today gauge.
// Must be called before the limiter handles any requests.
func (l *InMemoryLimiter) SetOnTokensUsed(fn func(tenantID string, used int)) {
	l.onTokensUsed = fn
}

// addTenantUsage adds delta to the tenant's usage for today (rolling it over
// at the UTC day boundary) and reports the new total through onTokensUsed.
// Callers hold the key bucket's lock; the lock order is always key bucket,
// then tenant bucket.
func (l *InMemoryLimiter) addTenantUsage(tenantID string, delta int) {
	if delta == 0 {
		return
	}
	v, _ := l.tenantUsage.LoadOrStore(tenantID, &dailyBucket{day: utcDayIndex()})
	b := v.(*dailyBucket)
	today := utcDayIndex()
	b.mu.Lock()
	if b.day != today {
		b.day = today
		b.used = 0
	}
	b.used += delta
	used := b.used
	b.mu.Unlock()
	if l.onTokensUsed != nil {
		l.onTokensUsed(tenantID, used)
	}
}

// NewInMemoryLimiter creates an empty InMemoryLimiter.
func NewInMemoryLimiter() *InMemoryLimiter {
	return &InMemoryLimiter{}
}

func (l *InMemoryLimiter) AllowRequest(tenantID, keyID, model string, rpm int) (bool, int, error) {
	if rpm <= 0 {
		return true, -1, nil
	}
	lim := l.getOrCreateRPM(bucketKey(tenantID, keyID, model), rpm)
	if !lim.Allow() {
		return false, 0, nil
	}
	remaining := int(lim.Tokens())
	if remaining < 0 {
		remaining = 0
	}
	return true, remaining, nil
}

func (l *InMemoryLimiter) AllowInputTokens(tenantID, keyID, model string, tpd, n int) (bool, int, error) {
	return l.allowTokens(tenantID, keyID, model, tpd, n)
}

// AllowOutputTokens shares the same deduction logic as AllowInputTokens — the
// only practical difference between input and output phases is *when* the
// caller invokes it (admission vs. post-response), not how the bucket update
// behaves. Both delegate to allowTokens so the daily-counter semantics stay in
// exactly one place.
func (l *InMemoryLimiter) AllowOutputTokens(tenantID, keyID, model string, tpd, n int) (bool, int, error) {
	return l.allowTokens(tenantID, keyID, model, tpd, n)
}

// RemainingTokens reports the remaining daily budget for (key, model) without
// deducting. Returns -1 when tpd <= 0 (unlimited). It rolls the counter over at
// the UTC day boundary, like the deducting paths, so the peek reflects today's
// usage.
func (l *InMemoryLimiter) RemainingTokens(tenantID, keyID, model string, tpd int) (int, error) {
	if tpd <= 0 {
		return -1, nil
	}
	b := l.getOrCreateDaily(bucketKey(tenantID, keyID, model))
	today := utcDayIndex()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.day != today {
		b.day = today
		b.used = 0
	}
	rem := tpd - b.used
	if rem < 0 {
		rem = 0
	}
	return rem, nil
}

func (l *InMemoryLimiter) allowTokens(tenantID, keyID, model string, tpd, n int) (bool, int, error) {
	if tpd <= 0 || n <= 0 {
		return true, -1, nil
	}
	bucket := bucketKey(tenantID, keyID, model)
	b := l.getOrCreateDaily(bucket)
	today := utcDayIndex()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.day != today {
		// New calendar day — reset the counter.
		b.day = today
		b.used = 0
	}
	if b.used+n > tpd {
		// Clamp to the limit so the gauge reaches 100% and all subsequent
		// checks also fail immediately (no gap between used and tpd).
		if b.used < tpd {
			l.addTenantUsage(tenantID, tpd-b.used)
			b.used = tpd
		}
		return false, 0, nil
	}
	b.used += n
	l.addTenantUsage(tenantID, n)
	return true, tpd - b.used, nil
}

func (l *InMemoryLimiter) getOrCreateRPM(bucket string, rpm int) *rate.Limiter {
	r := rate.Limit(float64(rpm) / 60.0)
	if v, ok := l.rpmLimiters.Load(bucket); ok {
		lim := v.(*rate.Limiter)
		// Per-replica quotas multiply the per-replica rate by the LIVE healthy
		// replica count at admission time, so the effective rpm can change
		// without SIGHUP whenever an agent joins or leaves the fleet. Apply
		// the current rate/burst on every call — rate.Limiter supports live
		// updates and the operations are concurrency-safe. No-op when the
		// values match.
		if lim.Limit() != r {
			lim.SetLimit(r)
		}
		if burst := l.rpmBurst(rpm); lim.Burst() != burst {
			lim.SetBurst(burst)
		}
		return lim
	}
	lim := rate.NewLimiter(r, l.rpmBurst(rpm)) // burst = certified window (default: full minute)
	actual, _ := l.rpmLimiters.LoadOrStore(bucket, lim)
	return actual.(*rate.Limiter)
}

func (l *InMemoryLimiter) getOrCreateDaily(bucket string) *dailyBucket {
	if v, ok := l.dailyBuckets.Load(bucket); ok {
		return v.(*dailyBucket)
	}
	b := &dailyBucket{day: utcDayIndex()}
	actual, _ := l.dailyBuckets.LoadOrStore(bucket, b)
	return actual.(*dailyBucket)
}

// Reset clears all per-key limiters so new limits loaded from auth.yaml take
// effect immediately on the next request.
func (l *InMemoryLimiter) Reset() {
	l.rpmLimiters.Range(func(k, _ any) bool { l.rpmLimiters.Delete(k); return true })
	l.dailyBuckets.Range(func(k, _ any) bool { l.dailyBuckets.Delete(k); return true })
	l.tenantUsage.Range(func(k, _ any) bool { l.tenantUsage.Delete(k); return true })
}

// SweepIdle evicts limiter state that carries no information, bounding memory
// when keys are minted programmatically (every key×model pair that ever made a
// request otherwise lives in these maps until restart). Returns how many
// entries were removed.
//
//   - An RPM bucket is evicted once it is full: a token bucket refills within
//     its burst window, so a full bucket is byte-for-byte what a fresh one
//     would be. Eviction is lossless up to one race: requests that loaded the
//     limiter pointer in the instant the sweep deleted it deduct on the
//     orphaned bucket, and those deductions are forgotten when the next
//     request recreates a fresh one. That forgives at most the requests in
//     flight during that microsecond window, once per sweep — negligible
//     against any per-minute rate, and not worth the tombstone protocol that
//     true losslessness would need.
//   - A daily bucket is evicted once its UTC day has passed: rollover discards
//     its counter anyway, so a stale-day bucket is dead weight.
func (l *InMemoryLimiter) SweepIdle() int {
	removed := 0
	now := time.Now()
	l.rpmLimiters.Range(func(k, v any) bool {
		lim := v.(*rate.Limiter)
		if lim.TokensAt(now) >= float64(lim.Burst()) {
			l.rpmLimiters.Delete(k)
			removed++
		}
		return true
	})
	today := utcDayIndex()
	for _, m := range []*sync.Map{&l.dailyBuckets, &l.tenantUsage} {
		m.Range(func(k, v any) bool {
			b := v.(*dailyBucket)
			b.mu.Lock()
			stale := b.day != today
			b.mu.Unlock()
			if stale {
				m.Delete(k)
				removed++
			}
			return true
		})
	}
	return removed
}

// utcDayIndex returns the number of complete UTC days since the Unix epoch.
func utcDayIndex() int {
	return int(time.Now().UTC().Unix() / 86400)
}

// ── BadgerLimiter ─────────────────────────────────────────────────────────────

// BadgerLimiter extends InMemoryLimiter with persistence: daily token counters
// are flushed to BadgerDB diskDB so quota usage survives router restarts.
//
// RPM enforcement is fully in-memory (rate.Limiter); it does not need
// persistence because the per-minute window is always shorter than any restart.
//
// Daily token counters are:
//   - Lazy-loaded from BadgerDB on the first request for each bucket each day.
//   - Held in memory as the hot path for all subsequent checks.
//   - Flushed to BadgerDB on every StartPeriodicFlush tick and on Reset/shutdown.
//
// Key schema in diskDB: "quotaTPDk:{bucketKey}:{dayIndex}" with a 48 h TTL,
// where bucketKey is tenant NUL key [NUL model].
// The 48 h TTL provides one full day of overlap so a counter written late at
// night is still readable early the next morning if the router restarts.
type BadgerLimiter struct {
	*InMemoryLimiter
	store    DailyQuotaStore
	initOnce sync.Map // "bucketKey:dayIndex" → *sync.Once  (lazy preload gate)

	// onFlushError is called whenever a diskDB write fails during Flush.
	// Intended for incrementing the QuotaBackendError Prometheus counter.
	// Nil-safe — set once at startup via SetOnFlushError.
	onFlushError func()
}

// NewBadgerLimiter creates a BadgerLimiter backed by the given store.
func NewBadgerLimiter(store DailyQuotaStore) *BadgerLimiter {
	return &BadgerLimiter{
		InMemoryLimiter: NewInMemoryLimiter(),
		store:           store,
	}
}

// SetOnFlushError registers a callback that fires on every diskDB write failure
// inside Flush. Intended for incrementing the hivenet_quota_backend_errors_total
// Prometheus counter. Must be called before the limiter handles any requests.
func (l *BadgerLimiter) SetOnFlushError(fn func()) {
	l.onFlushError = fn
}

func (l *BadgerLimiter) AllowInputTokens(tenantID, keyID, model string, tpd, n int) (bool, int, error) {
	l.ensurePreloaded(tenantID, keyID, model)
	return l.InMemoryLimiter.AllowInputTokens(tenantID, keyID, model, tpd, n)
}

func (l *BadgerLimiter) AllowOutputTokens(tenantID, keyID, model string, tpd, n int) (bool, int, error) {
	l.ensurePreloaded(tenantID, keyID, model)
	return l.InMemoryLimiter.AllowOutputTokens(tenantID, keyID, model, tpd, n)
}

func (l *BadgerLimiter) RemainingTokens(tenantID, keyID, model string, tpd int) (int, error) {
	l.ensurePreloaded(tenantID, keyID, model)
	return l.InMemoryLimiter.RemainingTokens(tenantID, keyID, model, tpd)
}

// ensurePreloaded initialises the (key, model) daily bucket from BadgerDB
// exactly once per calendar day. Subsequent calls for the same (key, model,
// day) are no-ops. sync.Once guarantees exactly-once execution under heavy
// concurrency. The BadgerDB key includes the model so per-model token budgets
// survive restarts independently.
func (l *BadgerLimiter) ensurePreloaded(tenantID, keyID, model string) {
	today := utcDayIndex()
	bucket := bucketKey(tenantID, keyID, model)
	mapKey := fmt.Sprintf("%s:%d", bucket, today)

	once := &sync.Once{}
	actual, _ := l.initOnce.LoadOrStore(mapKey, once)
	actual.(*sync.Once).Do(func() {
		badgerKey := badgerTPDKey(bucket, today)
		stored, err := l.store.GetTokensUsed(badgerKey)
		if err != nil || stored == 0 {
			return // nothing stored yet or read error — start fresh
		}
		b := l.getOrCreateDaily(bucket)
		b.mu.Lock()
		// Only adopt the stored value if the bucket is for today and the
		// stored count exceeds what is already in memory (avoids overwriting
		// concurrent increments that arrived before the preload completed).
		if b.day == today && int(stored) > b.used {
			l.addTenantUsage(tenantID, int(stored)-b.used)
			b.used = int(stored)
		}
		b.mu.Unlock()
	})
}

// badgerTPDKey builds the BadgerDB key for a daily TPD counter from the
// already-composed in-memory bucket key.
//
// The "quotaTPDk:" prefix marks per-key counters. Counters written before
// buckets were per key used "quotaTPD:" with a per-tenant bucket; they are
// never read again (a per-key bucket "tenant NUL key" could otherwise equal
// an old per-model bucket "tenant NUL model") and expire through their 48 h
// TTL. Upgrading therefore restarts today's per-key counts from zero once.
//
// Using bucketKey (instead of a ':'-joined string) means identifiers that
// contain ':' cannot collide across distinct buckets — e.g. ("a:b", "c") and
// ("a", "b:c") would format to the same string under a naive ':' join. The
// NUL separator cannot appear in any identifier (validateIdentifier), so the
// mapping is unambiguous.
func badgerTPDKey(bucket string, day int) string {
	return fmt.Sprintf("quotaTPDk:%s:%d", bucket, day)
}

// Flush writes all current daily token counters to BadgerDB.
// Called on periodic tick and on shutdown/Reset.
//
// The in-memory dailyBuckets map is already keyed by the composite bucket
// string, so badgerTPDKey consumes it directly — no need to split and rejoin. This also makes the persisted key shape match
// ensurePreloaded's reads byte-for-byte by construction.
func (l *BadgerLimiter) Flush() {
	today := utcDayIndex()
	l.dailyBuckets.Range(func(k, v any) bool {
		bucket := k.(string)
		b := v.(*dailyBucket)
		b.mu.Lock()
		if b.day == today {
			badgerKey := badgerTPDKey(bucket, today)
			if err := l.store.SetTokensUsed(badgerKey, int64(b.used), 48*time.Hour); err != nil {
				log.Warnf("quota flush failed for bucket %q: %v", bucket, err)
				if l.onFlushError != nil {
					l.onFlushError()
				}
			}
		}
		b.mu.Unlock()
		return true
	})
}

// SweepIdle extends the in-memory sweep with the badger preload gates: an
// initOnce entry is keyed by "(bucket):(dayIndex)" and exists to make the
// first touch of a bucket on a given day re-read the persisted counter. Past
// days' gates will never fire again, so they are dropped. Today's gates (and
// today's daily buckets) are deliberately kept — deleting either would let a
// bucket reappear without its persisted used-count.
func (l *BadgerLimiter) SweepIdle() int {
	removed := l.InMemoryLimiter.SweepIdle()
	todaySuffix := fmt.Sprintf(":%d", utcDayIndex())
	l.initOnce.Range(func(k, _ any) bool {
		if !strings.HasSuffix(k.(string), todaySuffix) {
			l.initOnce.Delete(k)
			removed++
		}
		return true
	})
	return removed
}

// StartPeriodicFlush launches a background goroutine that calls Flush on every
// tick. The goroutine exits when ctx is cancelled (router shutdown).
func (l *BadgerLimiter) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l.Flush()
			}
		}
	}()
}

// Reset flushes current counters to BadgerDB, then clears all in-memory state
// so fresh limits from auth.yaml reload take effect on the next request.
func (l *BadgerLimiter) Reset() {
	l.Flush()
	l.InMemoryLimiter.Reset()
	// Clear the preload gate so buckets are re-loaded from BadgerDB on next access.
	l.initOnce.Range(func(k, _ any) bool { l.initOnce.Delete(k); return true })
}
