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
	"hivenet_router/internal/shared"

	"golang.org/x/sync/singleflight"
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
// budgets. All methods are safe for concurrent use.
//
// Every request-rate and key-budget bucket belongs to one key: limits are
// configured per key, so each key is counted against its own ceiling and
// never drains another key's budget, even within one tenant. tenantID is part
// of the bucket too, so a key ID reassigned to another tenant starts fresh.
// The model parameter selects the bucket shape: an empty string uses the flat
// per-key bucket; a non-empty string uses a per-(key, model) bucket.
//
// On top of its keys' budgets a tenant may have one daily budget of its own
// (DailyBudget.TenantTokensPerDay) that all its keys spend together. Every
// daily charge counts in the tenant's usage today, limited or not.
//
// With shared counters (SetFleet) every bucket is site-wide: the routers of a
// site spend one budget together.
type RateLimiter interface {
	// AllowRequest checks and deducts 1 unit from the key's RPM bucket for
	// the given model. Returns (true, remaining, nil) when allowed; (false, 0,
	// nil) when the bucket is exhausted; remaining is -1 when requestsPerMinute
	// is 0 (unlimited).
	AllowRequest(tenantID, keyID, model string, requestsPerMinute int) (allowed bool, remaining int, err error)

	// AllowInputTokens checks count tokens against the key's and the
	// tenant's daily budgets and, when both fit, charges both. A refused
	// charge is charged to neither. Called before the request is forwarded to
	// an agent (prompt estimation). remaining is what is left of the tighter
	// budget, -1 when neither is limited.
	AllowInputTokens(b DailyBudget, count int) (allowed bool, remaining int, err error)

	// AllowOutputTokens charges count tokens, already produced, to the key's
	// and the tenant's daily budgets: each is charged what it has left of
	// them, and allowed is false when either ran out. Called after the agent
	// returns a response (actual completion token count).
	AllowOutputTokens(b DailyBudget, count int) (allowed bool, remaining int, err error)

	// RemainingTokens reports what is left of the tighter of the key's and
	// the tenant's daily budgets WITHOUT deducting anything; -1 when neither
	// is limited. Used by the pre-queue admission check so an over-budget
	// request can be rejected without being charged.
	RemainingTokens(b DailyBudget) (remaining int, err error)

	// HoldTokens is AllowInputTokens that holds the count instead of
	// charging it: when both budgets fit it, count is set aside, so that
	// concurrent requests see it, until the returned hold is settled with
	// the tokens actually used, or released. A refused hold holds nothing
	// and is nil.
	HoldTokens(b DailyBudget, count int) (hold DailyHold, allowed bool, remaining int, err error)

	// Reset clears all cached limiter state. Call on SIGHUP after auth config reload
	// so updated quota limits take effect immediately.
	Reset()
}

// DailyHold is tokens set aside on a request's daily budgets when it is
// admitted (RateLimiter.HoldTokens), until it is known how many it used.
// Both methods are safe to call more than once, in any order, and on a nil
// hold.
type DailyHold interface {
	// Settle frees the hold and charges actual tokens, which the request
	// used, as AllowOutputTokens does: each budget is charged what it has
	// left of them, and allowed is false when either ran out. Called once
	// the response's usage is known; a second call charges nothing.
	Settle(actual int) (allowed bool, remaining int, err error)
	// Release frees the hold without charging anything: the request was
	// refused or failed before it used tokens. After Settle it does nothing.
	Release()
}

// DailyBudget names the daily token budgets one charge counts against: the
// key's own (for one model in the per-model shape) and its tenant's, which
// every key of the tenant spends together. 0 means unlimited. A charge
// against two unlimited budgets is still counted in the tenant's usage today.
type DailyBudget struct {
	TenantID string
	KeyID    string
	// Model selects the key's bucket: "" for the flat shape, the requested
	// model for the per-model shape.
	Model string
	// KeyTokensPerDay is the key's own daily budget (tokens_per_day).
	KeyTokensPerDay int
	// TenantTokensPerDay is the tenant's daily budget, shared by all its
	// keys (QuotaLimits.TenantTokensPerDay).
	TenantTokensPerDay int
}

// bucketVersion and tenantUsageVersion mark the shape of a bucket key. Bucket
// keys name shared-store and Badger counters too, so routers running
// different shapes during a rolling upgrade never read or add to each
// other's counters: the per-tenant keys of earlier versions carry no marker,
// and a new shape gets a new marker.
const (
	bucketVersion      = "k1"
	tenantUsageVersion = "u1"
)

// bucketKey builds the composite limiter-bucket key: version marker, tenant,
// key and, for the per-model shape, model, joined by NUL. An empty model
// gives the flat per-key bucket. The separator is unambiguous: key IDs and
// owners are validated to contain no control characters
// (validateIdentifier), and model names are printable.
func bucketKey(tenantID, keyID, model string) string {
	k := bucketVersion + "\x00" + tenantID + "\x00" + keyID
	if model == "" {
		return k
	}
	return k + "\x00" + model
}

// tenantUsageKey names the daily counter that sums a tenant's usage over all
// its keys and models: its usage today, and the bucket its daily budget
// (DailyBudget.TenantTokensPerDay) is checked against.
func tenantUsageKey(tenantID string) string {
	return tenantUsageVersion + "\x00" + tenantID
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
// model) — or, under tenantUsageKey, a whole tenant — within one calendar day.
type dailyBucket struct {
	mu   sync.Mutex
	used int
	day  int // UTC days since Unix epoch
	// held is what this router's admitted requests have set aside and not
	// settled yet (HoldTokens). It is never published or persisted: it is
	// this router's, and a restart drops the requests that held it.
	held int

	// Shared counters only. loadedDay is the day the site total was read
	// from the fleet; epoch/base snapshot the usage when an outage began, so
	// during it this router spends only its share of what was left.
	loadedDay  int
	loadedSync uint64 // the fleet's ResyncEpoch when the total was read
	mark       string // last usage entry the loaded total includes
	epoch      uint64
	base       int

	// Admission lease (shared counters only): between two syncs this router
	// may admit at most its share of what was left at the last sync.
	leaseSeq   uint64
	leaseSpent int
	leaseCap   int

	// What the silent routers may still spend on their own (their share of
	// what was left when they went silent), kept free by this router.
	silentEp      uint64
	silentReserve int

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
	rpmLimiters  sync.Map // bucketKey → *rate.Limiter
	dailyBuckets sync.Map // bucketKey or tenantUsageKey → *dailyBucket

	// rpmBurstSeconds sizes the per-tenant RPM token bucket's burst as that many
	// seconds of the rate, instead of a full minute. 0 (or >= 60) keeps the
	// legacy full-minute burst. A short certified window (e.g. 10s) closes the
	// hole where a tenant could spend its entire minute's quota in one instant.
	// Set once at startup via SetRPMBurstWindow before any request is handled.
	rpmBurstSeconds int

	// fleet, when set, shares usage with the other routers of the site.
	// Set once at startup via SetFleet.
	fleet  Fleet
	leases leases
	// dailyReads lets the requests that need the same daily total share one
	// read of the shared store.
	dailyReads singleflight.Group
}

// SetFleet shares this limiter's usage with the other routers of the
// site. Must be called before the limiter handles any requests.
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

// addTenantUsage adds n served tokens to the usage today of a tenant without
// a daily budget. With shared counters it publishes them so every router sees
// the site total, and counts them locally only once this router has read that
// total (TenantUsageToday reads it off the request path, so a tenant without
// a budget never waits on the store); the read includes this router's
// unpublished usage, so nothing is counted twice.
func (l *InMemoryLimiter) addTenantUsage(tenantID string, n int) {
	if n <= 0 {
		return
	}
	key := tenantUsageKey(tenantID)
	if l.fleet != nil {
		l.fleet.Record(shared.KindDaily, key, 0, n)
	}
	b := l.getOrCreateDaily(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	rollDay(b, utcDayIndex())
	if l.fleet == nil || b.loadedDay == b.day {
		b.used += n
	}
}

// TenantUsageToday returns the tenant's tokens used today over all its keys
// and models: the site total with shared counters (read from the shared store
// the first time each day, then kept current by the other routers' usage),
// else this router's. known is false when the site total could not be read:
// the router then leaves the gauge at its last value instead of reporting 0.
// The router publishes it for every tenant on each sync interval, so every
// router of a site exports the same value.
func (l *InMemoryLimiter) TenantUsageToday(tenantID string) (used int, known bool) {
	key := tenantUsageKey(tenantID)
	b := l.getOrCreateDaily(key)
	if !l.loadFleetDaily(key, b) {
		return 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	rollDay(b, utcDayIndex())
	return b.used, true
}

// Refusal is why a daily budget cannot take tokens now, so the client is
// told the truth: which budget is spent, or that it is not spent and a retry
// shortly can succeed.
type Refusal int

const (
	// RefusalNone: the budgets themselves have room for the tokens.
	RefusalNone Refusal = iota
	// RefusalKeyBudget: the key's tokens_per_day cannot cover them.
	RefusalKeyBudget
	// RefusalTenantBudget: the tenant's tenant_tokens_per_day cannot cover
	// them; no other key of the tenant can either.
	RefusalTenantBudget
	// RefusalShare: the budgets have room, but this router's share of it is
	// spent: its share until the next sync with the other routers of the
	// site (dailyLease), or, while the shared store is down, its share of
	// what was left (dailyAllowance). The routers hold to these shares so
	// together they never overshoot; near the end of a budget, a request can
	// fit the site but not this router's share for the moment.
	RefusalShare
	// RefusalUnknown: the site's usage today cannot be read yet, so the
	// router refuses rather than grant a full budget (loadFleetDaily).
	RefusalUnknown
)

// Message is the client's message for the refusal: def for a key's budget
// (and for RefusalNone, a refusal the budgets no longer explain).
func (r Refusal) Message(def string) string {
	switch r {
	case RefusalTenantBudget:
		return "your organization's daily token budget is exhausted"
	case RefusalShare:
		return "the daily token budget is nearly spent and this router's share of it is used up for the moment; retry in a second"
	case RefusalUnknown:
		return "today's token usage cannot be checked right now; retry in a second"
	default:
		return def
	}
}

// RetryAfter is the Retry-After, in seconds, of a refusal a retry can
// overcome (the share frees at the next sync, the total is read once the
// store answers); 0 for a spent budget.
func (r Refusal) RetryAfter() int {
	if r == RefusalShare || r == RefusalUnknown {
		return 1
	}
	return 0
}

// ExplainRefusal asks l why b refused need tokens, when l can tell
// (InMemoryLimiter, BadgerLimiter); RefusalNone otherwise.
func ExplainRefusal(l RateLimiter, b DailyBudget, need int) Refusal {
	if e, ok := l.(interface {
		DailyRefusal(DailyBudget, int) Refusal
	}); ok {
		return e.DailyRefusal(b, need)
	}
	return RefusalNone
}

// ExplainSpent asks l which of b's budgets is spent, when l can tell:
// for a charge the budgets refused after it was applied (an output charge
// fills the budget it overflowed), where DailyRefusal would count the
// charge twice. RefusalNone when l cannot tell or neither is spent.
func ExplainSpent(l RateLimiter, b DailyBudget) Refusal {
	if e, ok := l.(interface {
		SpentRefusal(DailyBudget) Refusal
	}); ok {
		return e.SpentRefusal(b)
	}
	return RefusalNone
}

// SpentRefusal reports which of b's budgets is used up now (used >= its
// limit), the tenant's first. An output charge that overflowed fills the
// budget it could not cover, so after it this names that budget; a budget
// that still has room is not blamed.
func (l *InMemoryLimiter) SpentRefusal(b DailyBudget) Refusal {
	if b.KeyTokensPerDay <= 0 && b.TenantTokensPerDay <= 0 {
		return RefusalNone
	}
	key, ten, known := l.dailyTargets(b)
	if !known {
		return RefusalUnknown
	}
	for _, t := range []struct {
		dailyTarget
		r Refusal
	}{{ten, RefusalTenantBudget}, {key, RefusalKeyBudget}} {
		if t.tpd <= 0 {
			continue
		}
		t.b.mu.Lock()
		rollDay(t.b, utcDayIndex())
		full := t.b.used >= t.tpd
		t.b.mu.Unlock()
		if full {
			return t.r
		}
	}
	return RefusalNone
}

// DailyRefusal explains why b cannot take need tokens now. It is asked
// after a charge or an admission check refused: the tenant's budget is
// checked first (when both are spent, no other key helps), then the key's;
// when both have room, what refused was this router's share of them.
func (l *InMemoryLimiter) DailyRefusal(b DailyBudget, need int) Refusal {
	if b.KeyTokensPerDay <= 0 && b.TenantTokensPerDay <= 0 {
		return RefusalNone
	}
	key, ten, known := l.dailyTargets(b)
	if !known {
		return RefusalUnknown
	}
	for _, t := range []struct {
		dailyTarget
		r Refusal
	}{{ten, RefusalTenantBudget}, {key, RefusalKeyBudget}} {
		if t.tpd <= 0 {
			continue
		}
		t.b.mu.Lock()
		rollDay(t.b, utcDayIndex())
		over := t.b.used+t.b.held+need > t.tpd
		t.b.mu.Unlock()
		if over {
			return t.r
		}
	}
	return RefusalShare
}

// rollDay resets a daily bucket that last counted an earlier UTC day. Caller
// holds b.mu.
func rollDay(b *dailyBucket, today int) {
	if b.day != today {
		b.day, b.used, b.base, b.held = today, 0, 0, 0
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
	bucket := bucketKey(tenantID, keyID, model)
	lim := l.getOrCreateRPM(bucket, rpm)
	if l.fleet != nil {
		ok, charged := allowShared(l.fleet, &l.leases, bucket, lim, 1, false)
		if !ok {
			return false, 0, nil
		}
		l.fleet.Record(shared.KindRPM, bucket, rpm, charged)
	} else if !lim.Allow() {
		return false, 0, nil
	}
	remaining := int(lim.Tokens())
	if remaining < 0 {
		remaining = 0
	}
	return true, remaining, nil
}

func (l *InMemoryLimiter) AllowInputTokens(b DailyBudget, n int) (bool, int, error) {
	return l.allowTokens(b, n, true)
}

// AllowOutputTokens shares the same deduction logic as AllowInputTokens — the
// only practical difference between input and output phases is *when* the
// caller invokes it (admission vs. post-response), not how the bucket update
// behaves. Both delegate to allowTokens so the daily-counter semantics stay in
// exactly one place.
func (l *InMemoryLimiter) AllowOutputTokens(b DailyBudget, n int) (bool, int, error) {
	return l.allowTokens(b, n, false)
}

// HoldTokens admits n tokens on b like AllowInputTokens, but holds them
// instead of charging them: they count in this router's admission checks
// (and its per-sync lease) until the hold is settled or released. Nothing is
// published to the other routers until Settle charges what was used.
func (l *InMemoryLimiter) HoldTokens(b DailyBudget, n int) (DailyHold, bool, int, error) {
	return l.holdTokens(b, n, l)
}

// holdTokens is HoldTokens settling through settler, so a BadgerLimiter's
// settle charges through its own AllowOutputTokens.
func (l *InMemoryLimiter) holdTokens(bud DailyBudget, n int, settler RateLimiter) (DailyHold, bool, int, error) {
	h := &dailyHold{settler: settler, budget: bud, day: utcDayIndex()}
	if n <= 0 || (bud.KeyTokensPerDay <= 0 && bud.TenantTokensPerDay <= 0) {
		return h, true, -1, nil // nothing to hold: Settle counts the usage
	}
	key, ten, known := l.dailyTargets(bud)
	if !known {
		return nil, false, 0, nil // site total unknown: refuse until the store answers
	}
	limited := make([]dailyTarget, 0, 2)
	for _, t := range []dailyTarget{key, ten} {
		if t.tpd > 0 {
			t.b.mu.Lock()
			defer t.b.mu.Unlock()
			rollDay(t.b, h.day)
			limited = append(limited, t)
		}
	}
	for _, t := range limited {
		if !l.admits(t, n) {
			// As AllowInputTokens: a key whose own budget is too small for
			// the request is left full, so its next checks fail at once. Not
			// when only holds are in the way: they free up when settled.
			if key.tpd > 0 && key.b.used+n > key.tpd && key.b.used < key.tpd {
				key.b.used = key.tpd
			}
			return nil, false, 0, nil
		}
	}
	rem := -1
	for _, t := range limited {
		t.b.held += n
		t.b.leaseSpent += n
		h.buckets = append(h.buckets, t.b)
		if r := max(0, t.tpd-t.b.used-t.b.held); rem < 0 || r < rem {
			rem = r
		}
	}
	h.n = n
	return h, true, rem, nil
}

// dailyHold is InMemoryLimiter's DailyHold.
type dailyHold struct {
	settler RateLimiter
	budget  DailyBudget
	day     int            // the UTC day it was held on
	n       int            // tokens held on each bucket
	buckets []*dailyBucket // the limited buckets it is held on

	mu       sync.Mutex
	released bool
	settled  bool
}

func (h *dailyHold) Release() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.free()
}

// free drops the hold from its buckets, once. A bucket that has rolled over
// to another day since dropped it already. Caller holds h.mu.
func (h *dailyHold) free() {
	if h.released {
		return
	}
	h.released = true
	for _, b := range h.buckets {
		b.mu.Lock()
		if b.day == h.day {
			b.held = max(0, b.held-h.n)
		}
		b.mu.Unlock()
	}
}

func (h *dailyHold) Settle(actual int) (bool, int, error) {
	if h == nil {
		return true, -1, nil
	}
	h.mu.Lock()
	h.free()
	if h.settled {
		h.mu.Unlock()
		return true, -1, nil
	}
	h.settled = true
	h.mu.Unlock()
	return h.settler.AllowOutputTokens(h.budget, actual)
}

// RemainingTokens reports what is left of the tighter of the key's and the
// tenant's daily budgets without deducting; -1 when neither is limited. It
// rolls the counters over at the UTC day boundary, like the charging paths,
// so the peek reflects today's usage.
func (l *InMemoryLimiter) RemainingTokens(b DailyBudget) (int, error) {
	if b.KeyTokensPerDay <= 0 && b.TenantTokensPerDay <= 0 {
		return -1, nil
	}
	key, ten, known := l.dailyTargets(b)
	if !known {
		return 0, nil // site total unknown: refuse until the store answers
	}
	rem := -1
	for _, t := range []dailyTarget{key, ten} {
		if t.tpd <= 0 {
			continue
		}
		t.b.mu.Lock()
		rollDay(t.b, utcDayIndex())
		r := max(0, l.dailyAllowance(t.b, t.tpd)-t.b.used-t.b.held)
		t.b.mu.Unlock()
		if rem < 0 || r < rem {
			rem = r
		}
	}
	return rem, nil
}

// dailyTarget is one daily bucket a charge applies to.
type dailyTarget struct {
	name string
	b    *dailyBucket
	tpd  int // its daily limit; 0 = not limited (never set for the key's bucket)
}

// dailyTargets returns the key's bucket (when the key has a daily limit) and
// the tenant's (always: it counts the tenant's usage), each limited one with
// its site total read first. known is false when a limited bucket's total
// cannot be read. A tenant without a budget is counted without a read.
func (l *InMemoryLimiter) dailyTargets(b DailyBudget) (key, tenant dailyTarget, known bool) {
	if b.KeyTokensPerDay > 0 {
		name := bucketKey(b.TenantID, b.KeyID, b.Model)
		key = dailyTarget{name: name, b: l.getOrCreateDaily(name), tpd: b.KeyTokensPerDay}
		if !l.loadFleetDaily(name, key.b) {
			return key, tenant, false
		}
	}
	name := tenantUsageKey(b.TenantID)
	tenant = dailyTarget{name: name, b: l.getOrCreateDaily(name), tpd: b.TenantTokensPerDay}
	if tenant.tpd > 0 && !l.loadFleetDaily(name, tenant.b) {
		return key, tenant, false
	}
	return key, tenant, true
}

// admission marks the pre-forward input charge. Only admission is held to this
// router's share of the budget (outage allowance, per-sync lease): output is
// charged after the response exists, so it is judged against the site
// total alone and never rejected because of this router's share.
//
// The key's and the tenant's buckets are locked together, key first, so the
// two checks and the two charges are one step: no concurrent charge can slip
// between them, and a refused admission charges neither.
func (l *InMemoryLimiter) allowTokens(bud DailyBudget, n int, admission bool) (bool, int, error) {
	if n <= 0 {
		return true, -1, nil
	}
	if bud.KeyTokensPerDay <= 0 && bud.TenantTokensPerDay <= 0 {
		l.addTenantUsage(bud.TenantID, n) // counted, never refused
		return true, -1, nil
	}
	key, ten, known := l.dailyTargets(bud)
	if !known {
		return false, 0, nil // site total unknown: refuse until the store answers
	}
	today := utcDayIndex()
	limited := make([]dailyTarget, 0, 2)
	for _, t := range []dailyTarget{key, ten} {
		if t.tpd > 0 {
			t.b.mu.Lock()
			defer t.b.mu.Unlock()
			rollDay(t.b, today)
			limited = append(limited, t)
		}
	}

	var charged []int
	allowed := true
	if admission {
		for _, t := range limited {
			if !l.admits(t, n) {
				allowed = false
			}
		}
		if !allowed {
			// Neither budget is charged. A key whose own budget is too small
			// for the request is left full, as before, so its next checks fail
			// at once; the tenant's bucket never is: one key's oversized
			// request must not lock the tenant's other keys out.
			if key.tpd > 0 && key.b.used+n > key.tpd && key.b.used < key.tpd {
				key.b.used = key.tpd
			}
			return false, 0, nil
		}
		for _, t := range limited {
			charged = append(charged, l.charge(t, n, true))
		}
	} else {
		// Output tokens are already spent: each budget is charged what it has
		// left of them and, when they do not fit, is left full, so the other
		// routers see it full too.
		for _, t := range limited {
			c := min(n, max(0, t.tpd-t.b.used))
			if c < n {
				allowed = false
			}
			charged = append(charged, l.charge(t, c, false))
			if c < n {
				t.b.used = t.tpd
			}
		}
	}
	rem := -1
	for _, t := range limited {
		if r := max(0, t.tpd-t.b.used-t.b.held); rem < 0 || r < rem {
			rem = r
		}
	}
	if ten.tpd <= 0 {
		// The tenant has no budget, so only the key's is limited: count what
		// it took in the tenant's usage. Its bucket is not locked here; the
		// lock order (key, then tenant) is the same as above.
		l.addTenantUsage(bud.TenantID, charged[0])
	}
	if !allowed {
		return false, 0, nil
	}
	return true, rem, nil
}

// admits reports whether n more tokens fit a limited daily bucket at
// admission: the site budget, this router's share of it while the shared
// store is down, and its per-sync lease. Caller holds t.b.mu.
func (l *InMemoryLimiter) admits(t dailyTarget, n int) bool {
	if allowance := l.dailyAllowance(t.b, t.tpd); allowance < t.tpd && t.b.used+t.b.held+n > allowance {
		return false // outage: this router's share of the rest is spent
	}
	if !l.dailyLease(t.b, t.tpd, n) {
		return false // this router's share since the last sync is spent
	}
	return t.b.used+t.b.held+n <= t.tpd
}

// charge adds n tokens to a limited daily bucket and publishes them to the
// other routers. Caller holds t.b.mu. Returns n.
func (l *InMemoryLimiter) charge(t dailyTarget, n int, admission bool) int {
	if n <= 0 {
		return 0
	}
	t.b.used += n
	if admission {
		t.b.leaseSpent += n
	}
	if l.fleet != nil {
		l.fleet.Record(shared.KindDaily, t.name, t.tpd, n)
	}
	return n
}

// loadFleetDaily reads the site's usage today for a daily bucket the
// first time this router touches it each day, and again after this router may
// have missed other routers' usage (see Fleet.ResyncEpoch). It reports false
// when a first read fails: the router does not know how much is left, so it
// refuses (fail closed) rather than grant a full day's budget on top of the
// others'. A failed read again keeps the total it has. While the store is down
// the read fails at once; while it hangs, it waits at most 200 ms, and the
// requests for the same bucket share one read.
//
// A bucket this router knew yesterday needs no read at midnight: a new day
// starts at zero for every router, and during an outage each one then spends
// at most its share of it.
func (l *InMemoryLimiter) loadFleetDaily(bucket string, b *dailyBucket) bool {
	if l.fleet == nil {
		return true
	}
	today := utcDayIndex()
	sync := l.fleet.ResyncEpoch()
	degraded := l.fleet.DegradedEpoch() != 0
	b.mu.Lock()
	if b.loadedDay == today && (b.loadedSync == sync || degraded) {
		b.mu.Unlock()
		return true
	}
	if b.loadedDay == today-1 && degraded {
		b.day, b.used, b.base, b.loadedDay, b.mark, b.loadedSync = today, 0, 0, today, "", sync
		b.mu.Unlock()
		return true
	}
	known := b.loadedDay == today
	b.mu.Unlock()
	v, err, _ := l.dailyReads.Do(bucket, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		total, mark, err := l.fleet.DailyTotal(ctx, bucket)
		return dailyRead{total, mark}, err
	})
	if err != nil {
		return known
	}
	r := v.(dailyRead)
	b.mu.Lock()
	if b.loadedDay != today || b.loadedSync != sync {
		// The total holds every router's usage up to mark, ours included
		// (published or not).
		b.day, b.used, b.base, b.loadedDay, b.mark, b.loadedSync = today, r.total, 0, today, r.mark, sync
	}
	b.mu.Unlock()
	return true
}

type dailyRead struct {
	total int
	mark  string
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
	silent := l.fleet.Silent()
	if ep := l.fleet.SilentEpoch(); b.silentEp != ep {
		// The silent routers changed: keep free the share of what is left
		// that each may still spend while cut off (it enforces 1/N of it).
		b.silentEp, b.silentReserve = ep, 0
		if silent > 0 && b.used < tpd {
			b.silentReserve = (tpd - b.used) * silent / (nr + silent)
		}
	}
	if nr <= 1 && silent == 0 {
		return true
	}
	if b.used+b.held+n > tpd-b.silentReserve {
		return false
	}
	if seq := l.fleet.SyncSeq(); b.leaseSeq != seq {
		// What is left, minus what the others are likely spending right now
		// (as much as they spent in the last sync window), split evenly.
		inFlight := 0
		if b.peerSeq+1 == seq {
			inFlight = b.peerLast
		}
		b.leaseSeq, b.leaseSpent = seq, 0
		b.leaseCap = max(1, (tpd-b.used-b.held-inFlight-b.silentReserve)/max(nr, 1))
	}
	return b.leaseSpent == 0 || b.leaseSpent+n <= b.leaseCap
}

// ApplyPeerUsage takes usage another router of the site admitted. day is
// the UTC day of a daily usage, id the stream entry that carried it.
func (l *InMemoryLimiter) ApplyPeerUsage(kind byte, bucket string, limit, n int, age time.Duration, day int, id string) {
	switch kind {
	case shared.KindRPM:
		if limit <= 0 {
			return
		}
		lim := l.peerRPM(bucket, limit)
		window := time.Duration(float64(lim.Burst()) / float64(lim.Limit()) * float64(time.Second))
		debit(lim, decayed(n, age, window))
	case shared.KindDaily:
		today := utcDayIndex()
		if day != today {
			return // another day's budget
		}
		v, ok := l.dailyBuckets.Load(bucket)
		if !ok {
			return // read in full with the total when this router first needs it
		}
		b := v.(*dailyBucket)
		b.mu.Lock()
		if b.loadedDay != today || !shared.StreamBefore(b.mark, id) {
			b.mu.Unlock()
			return // already in the total this router read
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

// peerRPM returns the bucket another router's usage applies to. An existing
// bucket keeps this router's limit: a peer may still run older limits while
// auth.yaml rolls out.
func (l *InMemoryLimiter) peerRPM(bucket string, rpm int) *rate.Limiter {
	if v, ok := l.rpmLimiters.Load(bucket); ok {
		return v.(*rate.Limiter)
	}
	return l.getOrCreateRPM(bucket, rpm)
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
//
// With shared counters the buckets are kept: every call passes the current
// limits, a rate bucket follows them in place, and dropping the usage would
// hand out a fresh budget (or, during an outage, refuse every daily key).
func (l *InMemoryLimiter) Reset() {
	l.leases.reset()
	if l.fleet != nil {
		return
	}
	l.rpmLimiters.Range(func(k, _ any) bool { l.rpmLimiters.Delete(k); return true })
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
		// Yesterday's buckets are kept while the shared store is down: the
		// midnight reset needs them (see loadFleetDaily).
		stale := b.day < today-1 || (b.day == today-1 && (l.fleet == nil || l.fleet.DegradedEpoch() == 0))
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
//   - Lazy-loaded from BadgerDB on the first request for each bucket each day.
//   - Held in memory as the hot path for all subsequent checks.
//   - Flushed to BadgerDB on every StartPeriodicFlush tick and on Reset/shutdown.
//
// Key schema in diskDB: "quotaTPD:{bucket}:{dayIndex}" with a 48 h TTL, where
// bucket is a versioned bucketKey or tenantUsageKey.
// The 48 h TTL provides one full day of overlap so a counter written late at
// night is still readable early the next morning if the router restarts.
type BadgerLimiter struct {
	*InMemoryLimiter
	store    DailyQuotaStore
	initOnce sync.Map // "bucket:dayIndex" → *sync.Once  (lazy preload gate)

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

func (l *BadgerLimiter) AllowInputTokens(b DailyBudget, n int) (bool, int, error) {
	l.preloadBudget(b)
	return l.InMemoryLimiter.AllowInputTokens(b, n)
}

func (l *BadgerLimiter) AllowOutputTokens(b DailyBudget, n int) (bool, int, error) {
	l.preloadBudget(b)
	return l.InMemoryLimiter.AllowOutputTokens(b, n)
}

// HoldTokens is InMemoryLimiter.HoldTokens with the counters first read
// back from BadgerDB after a restart; its Settle persists like
// AllowOutputTokens.
func (l *BadgerLimiter) HoldTokens(b DailyBudget, n int) (DailyHold, bool, int, error) {
	l.preloadBudget(b)
	return l.holdTokens(b, n, l)
}

func (l *BadgerLimiter) RemainingTokens(b DailyBudget) (int, error) {
	l.preloadBudget(b)
	return l.InMemoryLimiter.RemainingTokens(b)
}

// SpentRefusal is InMemoryLimiter.SpentRefusal with the counters first read
// back from BadgerDB after a restart.
func (l *BadgerLimiter) SpentRefusal(b DailyBudget) Refusal {
	l.preloadBudget(b)
	return l.InMemoryLimiter.SpentRefusal(b)
}

// DailyRefusal is InMemoryLimiter.DailyRefusal with the counters first
// read back from BadgerDB after a restart.
func (l *BadgerLimiter) DailyRefusal(b DailyBudget, need int) Refusal {
	l.preloadBudget(b)
	return l.InMemoryLimiter.DailyRefusal(b, need)
}

// TenantUsageToday is InMemoryLimiter.TenantUsageToday with the tenant's
// counter first read back from BadgerDB after a restart.
func (l *BadgerLimiter) TenantUsageToday(tenantID string) (int, bool) {
	l.ensurePreloaded(tenantUsageKey(tenantID))
	return l.InMemoryLimiter.TenantUsageToday(tenantID)
}

// preloadBudget reads back from BadgerDB, once a day, the counters a charge
// against b touches: the key's (when it has a daily limit) and the tenant's.
func (l *BadgerLimiter) preloadBudget(b DailyBudget) {
	if b.KeyTokensPerDay > 0 {
		l.ensurePreloaded(bucketKey(b.TenantID, b.KeyID, b.Model))
	}
	l.ensurePreloaded(tenantUsageKey(b.TenantID))
}

// ensurePreloaded initialises a daily bucket (a key's, or a tenant's usage)
// from BadgerDB exactly once per calendar day. Subsequent calls for the same
// (bucket, day) are no-ops. sync.Once guarantees exactly-once execution under
// heavy concurrency.
func (l *BadgerLimiter) ensurePreloaded(bucket string) {
	today := utcDayIndex()
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
		}
		b.mu.Unlock()
	})
}

// badgerTPDKey builds the BadgerDB key for a daily TPD counter from the
// already-composed in-memory bucket key. The bucket's version marker keeps
// counters of different bucket shapes apart: per-tenant counters written
// before buckets were per key are never read again and expire through their
// 48 h TTL, so upgrading restarts today's per-key counts from zero once.
//
// Using bucketKey (instead of a ':'-joined string) means identifiers that
// contain ':' cannot collide across distinct buckets — e.g. ("a:b", "c") and
// ("a", "b:c") would format to the same string under a naive ':' join. The
// NUL separator cannot appear in any identifier (validateIdentifier), so the
// mapping is unambiguous.
func badgerTPDKey(bucket string, day int) string {
	return fmt.Sprintf("quotaTPD:%s:%d", bucket, day)
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
