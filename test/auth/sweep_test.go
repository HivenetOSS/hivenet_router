// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Tests for the limiter janitor: idle bucket state is evicted losslessly (a
// full token bucket is identical to the fresh one the next request creates),
// while any bucket still carrying information — a partial drain — survives.
package auth_test

import (
	"testing"
	"time"

	"hivenet_router/internal/auth"
)

// TestSweepIdle_KeepsDrainedRPMBucket verifies a bucket that still remembers a
// recent deduction is not evicted — evicting it would forgive the drain.
func TestSweepIdle_KeepsDrainedRPMBucket(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	// rpm 60 → refilling the deducted token takes a full second; the bucket is
	// still partial when the sweep runs immediately after.
	if allowed, _, _ := l.AllowRequest("t1", "", 60); !allowed {
		t.Fatal("first request must pass")
	}
	if n := l.SweepIdle(); n != 0 {
		t.Fatalf("SweepIdle evicted %d entries, want 0 (bucket is mid-refill)", n)
	}
}

// TestSweepIdle_EvictsRefilledRPMBucket verifies a bucket is evicted once it
// has refilled to full — at that point it is byte-for-byte the bucket a fresh
// request would create, so eviction is lossless.
func TestSweepIdle_EvictsRefilledRPMBucket(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	// rpm 60000 → the single deducted token refills in ~1ms.
	if allowed, _, _ := l.AllowRequest("t1", "", 60000); !allowed {
		t.Fatal("first request must pass")
	}
	time.Sleep(50 * time.Millisecond)
	if n := l.SweepIdle(); n != 1 {
		t.Fatalf("SweepIdle evicted %d entries, want 1 (bucket refilled to full)", n)
	}
	// The next request recreates the bucket transparently.
	if allowed, _, _ := l.AllowRequest("t1", "", 60000); !allowed {
		t.Error("request after eviction must pass on a fresh bucket")
	}
}

// TestSweepIdle_KeepsTodaysDailyBucket verifies the daily counter for the
// current UTC day survives the sweep — it holds the tenant's used count and
// must live until its day rolls over.
func TestSweepIdle_KeepsTodaysDailyBucket(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	allowed, remaining, _ := l.AllowInputTokens("t1", "", 1000, 400)
	if !allowed {
		t.Fatal("input tokens must be allowed")
	}
	l.SweepIdle()
	// The used count must be intact: a second charge sees the earlier one.
	allowed, remaining2, _ := l.AllowInputTokens("t1", "", 1000, 100)
	if !allowed {
		t.Fatal("second charge must be allowed")
	}
	if remaining2 >= remaining {
		t.Errorf("daily used count must survive the sweep: remaining went %d -> %d", remaining, remaining2)
	}
}

// TestMinuteSweepIdle_FullEvictedPartialKept verifies the ITPM/OTPM janitor:
// an untouched (full) bucket is evicted, a partially-drained one is kept.
func TestMinuteSweepIdle_FullEvictedPartialKept(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	// OutputExhausted creates the bucket without deducting — it stays full.
	if l.OutputExhausted("t-full", "m", 1000) {
		t.Fatal("fresh bucket must not be exhausted")
	}
	// AllowInputTokens deducts half the bucket — it carries state.
	if !l.AllowInputTokens("t-drained", "m", 1000, 500) {
		t.Fatal("input charge must be allowed")
	}

	if n := l.SweepIdle(); n != 1 {
		t.Fatalf("SweepIdle evicted %d buckets, want exactly 1 (the full one)", n)
	}
	// The drained bucket still remembers its charge: another 600 must not fit.
	if l.AllowInputTokens("t-drained", "m", 1000, 600) {
		t.Error("drained bucket must have survived the sweep and deny an over-charge")
	}
}
