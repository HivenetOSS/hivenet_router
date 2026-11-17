// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"sync"
	"time"

	"hivenet_router/internal/shared"

	"golang.org/x/time/rate"
)

// MinuteRateLimiter enforces the serverless per-key tokens-per-minute caps
// (ITPM/OTPM). It is separate from RateLimiter so the RPM/daily interface (and
// its fakes) stay stable. Each (tenant, key, model, direction) gets its own
// x/time/rate token bucket measured in tokens: rate = limit/60 tokens per
// second, burst = limit (one minute's capacity). The tenant is part of the
// bucket, like RateLimiter's, so a key ID reassigned to another tenant starts
// fresh. Safe for concurrent use.
type MinuteRateLimiter struct {
	buckets sync.Map // direction+bucketKey → *rate.Limiter

	// fleet, when set, shares usage with the other routers (SetFleet).
	fleet  Fleet
	leases leases
}

// SetFleet shares this limiter's usage with the other routers of the
// site. Must be called before the limiter handles any requests.
func (l *MinuteRateLimiter) SetFleet(f Fleet) { l.fleet = f }

// ApplyPeerUsage takes tokens another router of the site admitted.
func (l *MinuteRateLimiter) ApplyPeerUsage(kind byte, key string, limit, n int, age time.Duration, _ int, _ string) {
	if (kind != shared.KindITPM && kind != shared.KindOTPM) || limit <= 0 {
		return
	}
	// An existing bucket keeps this router's limit (see InMemoryLimiter.peerRPM).
	var lim *rate.Limiter
	if v, ok := l.buckets.Load(key); ok {
		lim = v.(*rate.Limiter)
	} else {
		lim = l.bucket(key, limit)
	}
	debit(lim, decayed(n, age, time.Minute))
}

// NewMinuteRateLimiter creates an empty MinuteRateLimiter.
func NewMinuteRateLimiter() *MinuteRateLimiter { return &MinuteRateLimiter{} }

// inKey and outKey build the input and output bucket keys: the direction,
// then the versioned key bucket name (bucketKey). They name shared-store
// counters too, so routers running the earlier key-only shape during a
// rolling upgrade do not add to these buckets.
func inKey(tenantID, keyID, model string) string {
	return "i\x00" + bucketKey(tenantID, keyID, model)
}

func outKey(tenantID, keyID, model string) string {
	return "o\x00" + bucketKey(tenantID, keyID, model)
}

// bucket returns the token bucket for key, sized to limit. Like the RPM bucket
// it applies the current rate/burst on every call so a reloaded limit takes
// effect without recreating the bucket.
func (l *MinuteRateLimiter) bucket(key string, limit int) *rate.Limiter {
	r := rate.Limit(float64(limit) / 60.0)
	if v, ok := l.buckets.Load(key); ok {
		lim := v.(*rate.Limiter)
		if lim.Limit() != r {
			lim.SetLimit(r)
		}
		if lim.Burst() != limit {
			lim.SetBurst(limit)
		}
		return lim
	}
	lim := rate.NewLimiter(r, limit)
	actual, _ := l.buckets.LoadOrStore(key, lim)
	return actual.(*rate.Limiter)
}

// AllowInputTokens deducts count input tokens from the key's ITPM bucket for
// model, returning true when within the per-minute rate and false when it would
// exceed it. limit <= 0 (or count <= 0) disables the cap. The loader guarantees
// ITPM capacity >= max_input_tokens, so a single request's input always fits the
// burst and can be admitted when the bucket is full.
func (l *MinuteRateLimiter) AllowInputTokens(tenantID, keyID, model string, limit, count int) bool {
	if limit <= 0 || count <= 0 {
		return true
	}
	key := inKey(tenantID, keyID, model)
	lim := l.bucket(key, limit)
	if l.fleet == nil {
		return lim.AllowN(time.Now(), count)
	}
	// While the store is down this router's share of the limit can be smaller
	// than one maximum-size prompt; a full share still admits it.
	ok, charged := allowShared(l.fleet, &l.leases, key, lim, count, true)
	if !ok {
		return false
	}
	l.fleet.Record(shared.KindITPM, key, limit, charged)
	return true
}

// OutputExhausted reports whether the key's OTPM bucket for model has run dry —
// the admission-time check that throttles new requests once a key's recent
// output has blown its per-minute rate. Non-deducting. limit <= 0 disables it
// (never exhausted). Output is metered post-response (ChargeOutputTokens), so
// this is how OTPM gates: a key that over-produces is held off on its next call.
func (l *MinuteRateLimiter) OutputExhausted(tenantID, keyID, model string, limit int) bool {
	if limit <= 0 {
		return false
	}
	return l.bucket(outKey(tenantID, keyID, model), limit).TokensAt(time.Now()) < 1
}

// ChargeOutputTokens deducts count output tokens from the OTPM bucket after a
// response completes, so a burst of output drains the bucket and throttles the
// key's subsequent requests via OutputExhausted. The charge is capped at one
// minute's capacity so a single huge response drains at most the full bucket.
// limit <= 0 (or count <= 0) is a no-op.
func (l *MinuteRateLimiter) ChargeOutputTokens(tenantID, keyID, model string, limit, count int) {
	if limit <= 0 || count <= 0 {
		return
	}
	if count > limit {
		count = limit
	}
	key := outKey(tenantID, keyID, model)
	count = silentCharge(l.fleet, count)
	debit(l.bucket(key, limit), count)
	if l.fleet != nil {
		l.fleet.Record(shared.KindOTPM, key, limit, count)
	}
}

// ChargeInputTokens deducts count input tokens from the ITPM bucket after a
// response: what the backend reported beyond the estimate AllowInputTokens
// admitted, so the input rate counts the prompt actually processed. Capped
// at one minute's capacity, like ChargeOutputTokens. limit <= 0 (or count
// <= 0) is a no-op.
func (l *MinuteRateLimiter) ChargeInputTokens(tenantID, keyID, model string, limit, count int) {
	if limit <= 0 || count <= 0 {
		return
	}
	if count > limit {
		count = limit
	}
	key := inKey(tenantID, keyID, model)
	count = silentCharge(l.fleet, count)
	debit(l.bucket(key, limit), count)
	if l.fleet != nil {
		l.fleet.Record(shared.KindITPM, key, limit, count)
	}
}

// Reset clears all buckets so limits reloaded from auth.yaml take effect
// immediately. Safe to call concurrently with request handling.
func (l *MinuteRateLimiter) Reset() {
	l.leases.reset()
	if l.fleet != nil {
		return // kept with shared counters, as InMemoryLimiter.Reset
	}
	l.buckets.Range(func(k, _ any) bool { l.buckets.Delete(k); return true })
}

// SweepIdle evicts every full bucket, bounding memory when keys are minted
// programmatically. A tokens-per-minute bucket refills completely within one
// minute of idleness, so a full bucket is identical to the fresh one the next
// request would create — eviction is lossless. Returns how many were removed.
func (l *MinuteRateLimiter) SweepIdle() int {
	removed := 0
	now := time.Now()
	l.buckets.Range(func(k, v any) bool {
		lim := v.(*rate.Limiter)
		if lim.TokensAt(now) >= float64(lim.Burst()) {
			l.buckets.Delete(k)
			l.leases.drop(k.(string))
			removed++
		}
		return true
	})
	return removed
}
