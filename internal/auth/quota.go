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

// DailyQuotaStore persists per-tenant daily token usage to durable storage.
// Implemented by *storage.BadgerStorage; the interface avoids an import cycle
// since Go satisfies interfaces implicitly.
type DailyQuotaStore interface {
	GetTokensUsed(key string) (int64, error)
	SetTokensUsed(key string, count int64, ttl time.Duration) error
}

// RateLimiter enforces per-tenant quotas: requests per minute and daily token budget.
// All methods are safe for concurrent use.
//
// The model parameter selects the bucket: an empty string uses the legacy
// per-tenant bucket (one bucket per tenant); a non-empty string uses a
// per-(tenant, model) bucket. Callers driving the legacy flat shape pass "";
// callers driving the per-model shape pass the requested model name so each
// (tenant, model) pair gets its own independent budget.
type RateLimiter interface {
	// AllowRequest checks and deducts 1 unit from the tenant's RPM bucket for
	// the given model. Returns (true, remaining, nil) when allowed; (false, 0,
	// nil) when the bucket is exhausted; remaining is -1 when requestsPerMinute
	// is 0 (unlimited).
	AllowRequest(tenantID, model string, requestsPerMinute int) (allowed bool, remaining int, err error)

	// AllowInputTokens checks and deducts count tokens from the tenant's daily
	// budget for the given model. Called before the request is forwarded to an
	// agent (prompt estimation).
	AllowInputTokens(tenantID, model string, tokensPerDay, count int) (allowed bool, remaining int, err error)

	// AllowOutputTokens checks and deducts count tokens from the tenant's
	// daily budget for the given model. Called after the agent returns a
	// response (actual completion token count).
	AllowOutputTokens(tenantID, model string, tokensPerDay, count int) (allowed bool, remaining int, err error)

	// RemainingTokens reports the remaining daily budget for (tenant, model)
	// WITHOUT deducting anything. Returns -1 when tokensPerDay is 0 (unlimited).
	// Used by the pre-queue admission check so an over-budget request can be
	// rejected without being charged.
	RemainingTokens(tenantID, model string, tokensPerDay int) (remaining int, err error)

	// Reset clears all cached limiter state. Call on SIGHUP after auth config reload
	// so updated quota limits take effect immediately.
	Reset()
}

// bucketKey builds the composite limiter-bucket key. An empty model preserves
// the legacy "one bucket per tenant" shape; a non-empty model gives each
// (tenant, model) pair an independent bucket. The "\x00" separator can never
// appear in a model name (model identifiers are printable ASCII).
func bucketKey(tenantID, model string) string {
	if model == "" {
		return tenantID
	}
	return tenantID + "\x00" + model
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

// dailyBucket tracks token consumption for a single tenant within one calendar day.
type dailyBucket struct {
	mu   sync.Mutex
	used int
	day  int // UTC days since Unix epoch

	// Shared counters only. loadedDay is the day the policloud total was read
	// from the fleet; epoch/base snapshot the usage when an outage began, so
	// during it this router spends only its share of what was left.
	loadedDay int
	epoch     uint64
	base      int

	// Admission lease (shared counters only): between two syncs this router
	// may admit at most its share of what was left at the last sync.
	leaseSeq   uint64
	leaseSpent int
	leaseCap   int

	// Peer usage applied during the last sync (peerSeq is the sync it was
	// applied in). The lease assumes the other routers keep spending at that
	// rate until the next sync, which their usage has not reached us yet for.
	peerSeq  uint64
	peerLast int
}

// InMemoryLimiter enforces quotas entirely in process memory.
// RPM enforcement uses golang.org/x/time/rate token buckets.
// Daily token enforcement uses calendar-day counters that reset at midnight UTC.
type InMemoryLimiter struct {
	rpmLimiters  sync.Map // tenantID → *rate.Limiter
	dailyBuckets sync.Map // tenantID → *dailyBucket

	// onTokensUsed is called after every successful token deduction with the
	// tenant ID and the new cumulative used count for today. Nil-safe.
	// Set once at startup via SetOnTokensUsed before any requests arrive.
	onTokensUsed func(tenantID string, used int)

	// rpmBurstSeconds sizes the per-tenant RPM token bucket's burst as that many
	// seconds of the rate, instead of a full minute. 0 (or >= 60) keeps the
	// legacy full-minute burst. A short certified window (e.g. 10s) closes the
	// hole where a tenant could spend its entire minute's quota in one instant.
	// Set once at startup via SetRPMBurstWindow before any request is handled.
	rpmBurstSeconds int

	// fleet, when set, shares usage with the other routers of the policloud.
	// Set once at startup via SetFleet.
	fleet  Fleet
	leases leases
}

// SetFleet shares this limiter's usage with the other routers of the
// policloud. Must be called before the limiter handles any requests.
func (l *InMemoryLimiter) SetFleet(f Fleet) { l.fleet = f }

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

// SetOnTokensUsed registers a callback that fires after every successful token
// deduction. The callback receives the tenant ID and the cumulative tokens used
// today. Intended for updating the hivenet_tenant_tokens_used_today gauge.
// Must be called before the limiter handles any requests.
func (l *InMemoryLimiter) SetOnTokensUsed(fn func(tenantID string, used int)) {
	l.onTokensUsed = fn
}

// reportUsage fires the onTokensUsed callback if one is registered.
func (l *InMemoryLimiter) reportUsage(tenantID string, used int) {
	if l.onTokensUsed != nil {
		l.onTokensUsed(tenantID, used)
	}
}

// NewInMemoryLimiter creates an empty InMemoryLimiter.
func NewInMemoryLimiter() *InMemoryLimiter {
	return &InMemoryLimiter{}
}

func (l *InMemoryLimiter) AllowRequest(tenantID, model string, rpm int) (bool, int, error) {
	if rpm <= 0 {
		return true, -1, nil
	}
	bucket := bucketKey(tenantID, model)
	lim := l.getOrCreateRPM(bucket, rpm)
	if l.fleet != nil {
		if !allowShared(l.fleet, &l.leases, bucket, lim, 1, false) {
			return false, 0, nil
		}
		l.fleet.Record(usageRPM, bucket, rpm, 1)
	} else if !lim.Allow() {
		return false, 0, nil
	}
	remaining := int(lim.Tokens())
	if remaining < 0 {
		remaining = 0
	}
	return true, remaining, nil
}

func (l *InMemoryLimiter) AllowInputTokens(tenantID, model string, tpd, n int) (bool, int, error) {
	return l.allowTokens(tenantID, model, tpd, n, true)
}

// AllowOutputTokens shares the same deduction logic as AllowInputTokens — the
// only practical difference between input and output phases is *when* the
// caller invokes it (admission vs. post-response), not how the bucket update
// behaves. Both delegate to allowTokens so the daily-counter semantics stay in
// exactly one place.
func (l *InMemoryLimiter) AllowOutputTokens(tenantID, model string, tpd, n int) (bool, int, error) {
	return l.allowTokens(tenantID, model, tpd, n, false)
}

// RemainingTokens reports the remaining daily budget for (tenant, model) without
// deducting. Returns -1 when tpd <= 0 (unlimited). It rolls the counter over at
// the UTC day boundary, like the deducting paths, so the peek reflects today's
// usage.
func (l *InMemoryLimiter) RemainingTokens(tenantID, model string, tpd int) (int, error) {
	if tpd <= 0 {
		return -1, nil
	}
	b := l.getOrCreateDaily(bucketKey(tenantID, model))
	if !l.loadFleetDaily(bucketKey(tenantID, model), b) {
		return 0, nil // policloud total unknown: refuse until the store answers
	}
	today := utcDayIndex()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.day != today {
		b.day = today
		b.used = 0
	}
	rem := l.dailyAllowance(b, tpd) - b.used
	if rem < 0 {
		rem = 0
	}
	return rem, nil
}

// admission marks the pre-forward input charge. Only admission is held to this
// router's share of the budget (outage allowance, per-sync lease): output is
// charged after the response exists, so it is judged against the policloud
// total alone and never rejected because of this router's share.
func (l *InMemoryLimiter) allowTokens(tenantID, model string, tpd, n int, admission bool) (bool, int, error) {
	if tpd <= 0 || n <= 0 {
		return true, -1, nil
	}
	bucket := bucketKey(tenantID, model)
	b := l.getOrCreateDaily(bucket)
	if !l.loadFleetDaily(bucket, b) {
		return false, 0, nil // policloud total unknown: refuse until the store answers
	}
	today := utcDayIndex()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.day != today {
		// New calendar day — reset the counter.
		b.day = today
		b.used = 0
	}
	if admission {
		if allowance := l.dailyAllowance(b, tpd); allowance < tpd && b.used+n > allowance {
			return false, 0, nil // outage: this router's share of the rest is spent
		}
		if !l.dailyLease(b, tpd, n) {
			return false, 0, nil // this router's share since the last sync is spent
		}
	}
	if b.used+n > tpd {
		// Clamp to the limit so the gauge reaches 100% and all subsequent
		// checks also fail immediately (no gap between used and tpd).
		if b.used < tpd {
			b.used = tpd
			l.reportUsage(tenantID, b.used)
		}
		return false, 0, nil
	}
	b.used += n
	if admission {
		b.leaseSpent += n
	}
	if l.fleet != nil {
		l.fleet.Record(usageDaily, bucket, tpd, n)
	}
	l.reportUsage(tenantID, b.used)
	return true, tpd - b.used, nil
}

// loadFleetDaily reads the policloud's usage today for a daily bucket the
// first time this router touches it each day. It reports false when the total
// cannot be read: the router does not know how much is left, so it refuses
// (fail closed) rather than grant a full day's budget on top of the others'.
func (l *InMemoryLimiter) loadFleetDaily(bucket string, b *dailyBucket) bool {
	if l.fleet == nil {
		return true
	}
	today := utcDayIndex()
	b.mu.Lock()
	loaded := b.loadedDay == today
	b.mu.Unlock()
	if loaded {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	total, err := l.fleet.DailyTotal(ctx, bucket)
	if err != nil {
		return false
	}
	b.mu.Lock()
	if b.day != today {
		b.day = today
		b.used = 0
	}
	b.used = max(b.used, total)
	b.loadedDay = today
	b.mu.Unlock()
	return true
}

// dailyAllowance is the daily limit this router may spend up to. While the
// shared store is down it is the usage when the outage began plus this
// router's share of what was left, so the routers together stay within tpd.
// Caller holds b.mu.
func (l *InMemoryLimiter) dailyAllowance(b *dailyBucket, tpd int) int {
	if l.fleet == nil {
		return tpd
	}
	e := l.fleet.DegradedEpoch()
	if e == 0 {
		return tpd
	}
	if b.epoch != e {
		b.epoch, b.base = e, b.used
	}
	n := l.fleet.Size()
	if n <= 1 || b.base >= tpd {
		return tpd
	}
	return b.base + (tpd-b.base)/n
}

// dailyLease applies the per-sync share to a daily budget: until the next
// sync this router may admit at most (what was left at the last sync) ÷ live
// routers, so routers that cannot see each other's latest spend still stay
// within the budget together. The first admission in a window always passes
// if the budget has room, so a request larger than the share is not starved.
// Caller holds b.mu.
func (l *InMemoryLimiter) dailyLease(b *dailyBucket, tpd, n int) bool {
	if l.fleet == nil || l.fleet.DegradedEpoch() != 0 {
		return true
	}
	nr := l.fleet.Size()
	if nr <= 1 {
		return true
	}
	if seq := l.fleet.SyncSeq(); b.leaseSeq != seq {
		// What is left, minus what the others are likely spending right now
		// (as much as they spent in the last sync window), split evenly.
		inFlight := 0
		if b.peerSeq+1 == seq {
			inFlight = b.peerLast
		}
		b.leaseSeq, b.leaseSpent = seq, 0
		b.leaseCap = max(1, (tpd-b.used-inFlight)/nr)
	}
	return b.leaseSpent == 0 || b.leaseSpent+n <= b.leaseCap
}

// ApplyPeerUsage takes usage another router of the policloud admitted.
func (l *InMemoryLimiter) ApplyPeerUsage(kind byte, bucket string, limit, n int, age time.Duration) {
	switch kind {
	case usageRPM:
		if limit <= 0 {
			return
		}
		lim := l.getOrCreateRPM(bucket, limit)
		window := time.Duration(float64(lim.Burst()) / float64(lim.Limit()) * float64(time.Second))
		debit(lim, decayed(n, age, window))
	case usageDaily:
		if age > time.Hour {
			return
		}
		b := l.getOrCreateDaily(bucket)
		today := utcDayIndex()
		b.mu.Lock()
		if b.day != today {
			b.day = today
			b.used = 0
		}
		b.used += n
		if l.fleet != nil {
			if seq := l.fleet.SyncSeq(); b.peerSeq != seq {
				b.peerSeq, b.peerLast = seq, 0
			}
			b.peerLast += n
		}
		b.mu.Unlock()
	}
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

func (l *InMemoryLimiter) getOrCreateDaily(tenantID string) *dailyBucket {
	if v, ok := l.dailyBuckets.Load(tenantID); ok {
		return v.(*dailyBucket)
	}
	b := &dailyBucket{day: utcDayIndex()}
	actual, _ := l.dailyBuckets.LoadOrStore(tenantID, b)
	return actual.(*dailyBucket)
}

// Reset clears all per-tenant limiters so new limits loaded from auth.yaml take
// effect immediately on the next request.
func (l *InMemoryLimiter) Reset() {
	l.rpmLimiters.Range(func(k, _ any) bool { l.rpmLimiters.Delete(k); return true })
	l.leases.reset()
	l.dailyBuckets.Range(func(k, _ any) bool { l.dailyBuckets.Delete(k); return true })
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
			l.leases.drop(k.(string))
			removed++
		}
		return true
	})
	today := utcDayIndex()
	l.dailyBuckets.Range(func(k, v any) bool {
		b := v.(*dailyBucket)
		b.mu.Lock()
		stale := b.day != today
		b.mu.Unlock()
		if stale {
			l.dailyBuckets.Delete(k)
			removed++
		}
		return true
	})
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
//   - Lazy-loaded from BadgerDB on the first request for each tenant each day.
//   - Held in memory as the hot path for all subsequent checks.
//   - Flushed to BadgerDB on every StartPeriodicFlush tick and on Reset/shutdown.
//
// Key schema in diskDB: "quotaTPD:{tenantID}:{dayIndex}" with a 48 h TTL.
// The 48 h TTL provides one full day of overlap so a counter written late at
// night is still readable early the next morning if the router restarts.
type BadgerLimiter struct {
	*InMemoryLimiter
	store    DailyQuotaStore
	initOnce sync.Map // "tenantID:dayIndex" → *sync.Once  (lazy preload gate)

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

func (l *BadgerLimiter) AllowInputTokens(tenantID, model string, tpd, n int) (bool, int, error) {
	l.ensurePreloaded(tenantID, model)
	return l.InMemoryLimiter.AllowInputTokens(tenantID, model, tpd, n)
}

func (l *BadgerLimiter) AllowOutputTokens(tenantID, model string, tpd, n int) (bool, int, error) {
	l.ensurePreloaded(tenantID, model)
	return l.InMemoryLimiter.AllowOutputTokens(tenantID, model, tpd, n)
}

func (l *BadgerLimiter) RemainingTokens(tenantID, model string, tpd int) (int, error) {
	l.ensurePreloaded(tenantID, model)
	return l.InMemoryLimiter.RemainingTokens(tenantID, model, tpd)
}

// ensurePreloaded initialises the (tenant, model) daily bucket from BadgerDB
// exactly once per calendar day. Subsequent calls for the same (tenant, model,
// day) are no-ops. sync.Once guarantees exactly-once execution under heavy
// concurrency. The BadgerDB key includes the model so per-model token budgets
// survive restarts independently — and the legacy flat path (model == "")
// keeps its original key shape for back-compat with restarted routers.
func (l *BadgerLimiter) ensurePreloaded(tenantID, model string) {
	today := utcDayIndex()
	bucket := bucketKey(tenantID, model)
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
		if b.day == today {
			b.used = max(b.used, int(stored))
			l.reportUsage(tenantID, b.used)
		}
		b.mu.Unlock()
	})
}

// badgerTPDKey builds the BadgerDB key for a daily TPD counter from the
// already-composed in-memory bucket key. The "{bucket}:{day}" shape is the
// same for legacy and per-model paths because bucketKey collapses to plain
// tenantID when model == "" — so legacy on-disk counters written before
// per-model support landed are still found by the same prefix lookup, with
// no migration needed.
//
// Critically, using bucketKey here (instead of a ':'-joined "tenant:model"
// string) means tenant or model values that themselves contain ':' cannot
// collide across distinct (tenant, model) pairs — e.g. ("a:b", "c") and
// ("a", "b:c") would format to the same string under a naive ':' join, which
// would silently merge their daily-token counters on disk. The NUL byte in
// bucketKey can never appear in a tenant ID or model identifier (both are
// printable ASCII), so the mapping is unambiguous.
func badgerTPDKey(bucket string, day int) string {
	return fmt.Sprintf("quotaTPD:%s:%d", bucket, day)
}

// Flush writes all current daily token counters to BadgerDB.
// Called on periodic tick and on shutdown/Reset.
//
// The in-memory dailyBuckets map is already keyed by the composite bucket
// string (legacy buckets collapse to bare tenantID, per-model buckets are
// "tenantID\x00model"), so badgerTPDKey consumes it directly — no need to
// split and rejoin. This also makes the persisted key shape match
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
	// Clear the preload gate so tenants are re-loaded from BadgerDB on next access.
	l.initOnce.Range(func(k, _ any) bool { l.initOnce.Delete(k); return true })
}
