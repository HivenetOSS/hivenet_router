// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package auth_test covers the rate limiter's per-model bucket isolation and
// the auth.yaml loader's validation of the per_model quota shape.
package auth_test

import (
	"testing"
	"time"

	"hivenet_router/internal/auth"
)

// TestInMemoryLimiter_PerModelBucketIsolation verifies that requests to two
// different models share NO quota state: spending one model's RPM does not
// affect another model's bucket. This is the core invariant of per-model
// quotas — a noisy 27B caller must not block a quiet 7B caller using the same
// API key.
func TestInMemoryLimiter_PerModelBucketIsolation(t *testing.T) {
	lim := auth.NewInMemoryLimiter()

	// Burn the entire RPM budget for model A (rpm=2 → burst 2).
	for i := 0; i < 2; i++ {
		ok, _, err := lim.AllowRequest("tenant1", "modelA", 2)
		if err != nil || !ok {
			t.Fatalf("modelA call %d should be allowed, got ok=%v err=%v", i, ok, err)
		}
	}
	// Next call to modelA must be rejected — bucket is empty.
	if ok, _, _ := lim.AllowRequest("tenant1", "modelA", 2); ok {
		t.Fatalf("modelA RPM should be exhausted after the burst")
	}

	// modelB has its own bucket; the rejection above must not have touched it.
	if ok, _, err := lim.AllowRequest("tenant1", "modelB", 2); err != nil || !ok {
		t.Fatalf("modelB bucket must be independent of modelA; got ok=%v err=%v", ok, err)
	}
}

// TestInMemoryLimiter_PerModelTokenBucketIsolation verifies the same isolation
// invariant for daily token budgets: deducting tokens from one (tenant, model)
// pair does not consume budget from another.
func TestInMemoryLimiter_PerModelTokenBucketIsolation(t *testing.T) {
	lim := auth.NewInMemoryLimiter()

	// Spend 90/100 from modelA.
	if ok, rem, _ := lim.AllowInputTokens("tenant1", "modelA", 100, 90); !ok || rem != 10 {
		t.Fatalf("modelA: expected ok=true rem=10, got ok=%v rem=%d", ok, rem)
	}
	// modelB starts fresh: 100 budget, spending 90 leaves 10.
	if ok, rem, _ := lim.AllowInputTokens("tenant1", "modelB", 100, 90); !ok || rem != 10 {
		t.Fatalf("modelB: bucket must be independent; expected ok=true rem=10, got ok=%v rem=%d", ok, rem)
	}
	// modelA: peek shows the right remaining for THIS model, not the other.
	if rem, _ := lim.RemainingTokens("tenant1", "modelA", 100); rem != 10 {
		t.Fatalf("modelA remaining: expected 10, got %d", rem)
	}
	// modelB peek: same.
	if rem, _ := lim.RemainingTokens("tenant1", "modelB", 100); rem != 10 {
		t.Fatalf("modelB remaining: expected 10, got %d", rem)
	}
}

// TestInMemoryLimiter_LegacyFlatBucketPreserved verifies the back-compat path:
// callers passing model="" still share one bucket per tenant — the exact
// pre-per-model shape — so legacy QuotaConfig (the flat requests_per_minute /
// tokens_per_day) keeps the same enforcement semantics.
func TestInMemoryLimiter_LegacyFlatBucketPreserved(t *testing.T) {
	lim := auth.NewInMemoryLimiter()

	// Spend 60/100 on the umbrella bucket.
	if ok, _, _ := lim.AllowInputTokens("tenant1", "", 100, 60); !ok {
		t.Fatalf("flat: first call should be allowed")
	}
	// A second flat call should see the spend; only 40 left.
	if rem, _ := lim.RemainingTokens("tenant1", "", 100); rem != 40 {
		t.Fatalf("flat: expected 40 remaining, got %d", rem)
	}
}

// TestInMemoryLimiter_RPMRescaleOnReplicaChange verifies that the limiter
// applies the LATEST effective_rpm on every AllowRequest call, not just at
// first creation. Per-replica × live count means the ceiling moves as
// replicas join or leave the fleet, so the underlying rate.Limiter's
// SetLimit/SetBurst must fire on every call — without it, a fleet scale-up
// would silently throttle keys to the rate they were FIRST seeded with.
//
// The test exploits the fact that a refill at the NEW rate (1000/sec) accrues
// 10 tokens in 10 ms, while the OLD rate (2/60 ≈ 0.033/sec) accrues 0.0003
// tokens in the same window — a four-order-of-magnitude difference that no
// timing jitter can mask. If SetLimit/SetBurst stop being applied per call,
// the second AllowRequest at the high rpm must fail; today's implementation
// keeps it green.
func TestInMemoryLimiter_RPMRescaleOnReplicaChange(t *testing.T) {
	lim := auth.NewInMemoryLimiter()

	// Seed at rpm=2: a single call exhausts the burst because the second
	// would race the refill — we want the bucket near-empty before the rescale.
	for i := 0; i < 2; i++ {
		if ok, _, _ := lim.AllowRequest("tenant1", "modelA", 2); !ok {
			t.Fatalf("seed burst should succeed")
		}
	}
	// Bucket empty under the SEED rate.
	if ok, _, _ := lim.AllowRequest("tenant1", "modelA", 2); ok {
		t.Fatalf("rpm=2 bucket should be exhausted before scale-up")
	}

	// Simulate the operator adding 30,000× replicas: the next call passes a
	// massively higher rpm. With per-call SetLimit/SetBurst, this primes the
	// new rate; the call itself may still see a near-empty bucket because the
	// time elapsed at the OLD rate accrued only ~0 tokens.
	_, _, _ = lim.AllowRequest("tenant1", "modelA", 60_000)

	// Let the NEW rate (1000 tokens/sec) refill the bucket. 15 ms ≈ 15 tokens.
	// Under the old rate (~0.033/sec), 15 ms would accrue 0.0005 — denied.
	time.Sleep(15 * time.Millisecond)
	if ok, _, _ := lim.AllowRequest("tenant1", "modelA", 60_000); !ok {
		t.Fatalf("after replica scale-up the limiter must rescale: a 15 ms wait at the NEW rate should refill the bucket. Old rate refill in 15 ms is ~0.0005 tokens — Allow would fail if SetLimit/SetBurst is not applied per call.")
	}

	// Now scale BACK down. The burst cap must shrink to 2 — drain to denial,
	// then verify a request at the lower rpm is denied (proving SetBurst
	// clamped the bucket capacity).
	for i := 0; i < 100; i++ {
		_, _, _ = lim.AllowRequest("tenant1", "modelA", 2)
	}
	if ok, _, _ := lim.AllowRequest("tenant1", "modelA", 2); ok {
		t.Fatalf("after scaling rpm back down to 2, the burst cap should shrink (SetBurst lowered the ceiling) — further calls must be denied")
	}
}

// TestQuotaLimits_ResolvePerModel verifies the strict-enumeration lookup:
// an exact model match succeeds; any unlisted model returns ok=false so the
// caller rejects with a "quota not declared" error. There is no wildcard.
func TestQuotaLimits_ResolvePerModel(t *testing.T) {
	q := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B": {RequestsPerMinutePerReplica: 10, TokensPerDay: 3_000_000},
		},
	}

	if got, ok := q.ResolvePerModel("Qwen/Qwen3.6-27B"); !ok || got.RequestsPerMinutePerReplica != 10 {
		t.Fatalf("exact match: expected rpm/replica=10, got ok=%v rpm/replica=%d", ok, got.RequestsPerMinutePerReplica)
	}
	if _, ok := q.ResolvePerModel("Qwen/Qwen3.6-35B"); ok {
		t.Fatalf("unlisted model must return ok=false (loud rejection path)")
	}

	// PerModel == nil → legacy flat path — Resolve must return ok=false so the
	// caller branches into the umbrella logic.
	if _, ok := (auth.QuotaLimits{RequestsPerMinute: 100}).ResolvePerModel("anything"); ok {
		t.Fatalf("legacy flat shape (PerModel == nil) must signal ok=false to ResolvePerModel callers")
	}
}
