// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// MinuteRateLimiter enforces the serverless per-key tokens-per-minute caps
// (ITPM/OTPM). It is separate from RateLimiter so the RPM/daily interface (and
// its fakes) stay stable. Each (tenant, model, direction) pair gets its own
// x/time/rate token bucket measured in tokens: rate = limit/60 tokens per
// second, burst = limit (one minute's capacity). Safe for concurrent use.
type MinuteRateLimiter struct {
	buckets sync.Map // direction+bucketKey → *rate.Limiter
}

// NewMinuteRateLimiter creates an empty MinuteRateLimiter.
func NewMinuteRateLimiter() *MinuteRateLimiter { return &MinuteRateLimiter{} }

func inKey(tenantID, model string) string  { return "i\x00" + bucketKey(tenantID, model) }
func outKey(tenantID, model string) string { return "o\x00" + bucketKey(tenantID, model) }

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

// AllowInputTokens deducts count input tokens from the (tenant, model) ITPM
// bucket, returning true when within the per-minute rate and false when it would
// exceed it. limit <= 0 (or count <= 0) disables the cap. The loader guarantees
// ITPM capacity >= max_input_tokens, so a single request's input always fits the
// burst and can be admitted when the bucket is full.
func (l *MinuteRateLimiter) AllowInputTokens(tenantID, model string, limit, count int) bool {
	if limit <= 0 || count <= 0 {
		return true
	}
	return l.bucket(inKey(tenantID, model), limit).AllowN(time.Now(), count)
}

// OutputExhausted reports whether the (tenant, model) OTPM bucket has run dry —
// the admission-time check that throttles new requests once a key's recent
// output has blown its per-minute rate. Non-deducting. limit <= 0 disables it
// (never exhausted). Output is metered post-response (ChargeOutputTokens), so
// this is how OTPM gates: a key that over-produces is held off on its next call.
func (l *MinuteRateLimiter) OutputExhausted(tenantID, model string, limit int) bool {
	if limit <= 0 {
		return false
	}
	return l.bucket(outKey(tenantID, model), limit).TokensAt(time.Now()) < 1
}

// ChargeOutputTokens deducts count output tokens from the OTPM bucket after a
// response completes, so a burst of output drains the bucket and throttles the
// key's subsequent requests via OutputExhausted. The charge is capped at one
// minute's capacity so a single huge response drains at most the full bucket.
// limit <= 0 (or count <= 0) is a no-op.
func (l *MinuteRateLimiter) ChargeOutputTokens(tenantID, model string, limit, count int) {
	if limit <= 0 || count <= 0 {
		return
	}
	if count > limit {
		count = limit
	}
	l.bucket(outKey(tenantID, model), limit).ReserveN(time.Now(), count)
}

// Reset clears all buckets so limits reloaded from auth.yaml take effect
// immediately. Safe to call concurrently with request handling.
func (l *MinuteRateLimiter) Reset() {
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
			removed++
		}
		return true
	})
	return removed
}
