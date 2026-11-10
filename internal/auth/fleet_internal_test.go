// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/shared"
)

// fakeFleet is a Fleet whose state each test sets by hand.
type fakeFleet struct {
	mu       sync.Mutex
	recorded map[byte]int
	degraded uint64
	resync   uint64
	reads    atomic.Int32
	readWait time.Duration
	total    int
}

func (f *fakeFleet) Record(kind byte, _ string, _, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recorded == nil {
		f.recorded = map[byte]int{}
	}
	f.recorded[kind] += n
}
func (f *fakeFleet) Size() int             { return 2 }
func (f *fakeFleet) DegradedEpoch() uint64 { return f.degraded }
func (f *fakeFleet) SyncSeq() uint64       { return 1 }
func (f *fakeFleet) Silent() int           { return 0 }
func (f *fakeFleet) SilentEpoch() uint64   { return 0 }
func (f *fakeFleet) ResyncEpoch() uint64   { return f.resync }
func (f *fakeFleet) DailyTotal(ctx context.Context, _ string) (int, string, error) {
	f.reads.Add(1)
	select {
	case <-time.After(f.readWait):
	case <-ctx.Done():
		return 0, "", ctx.Err()
	}
	return f.total, "0-0", nil
}

// While the shared store is down, the sweep keeps yesterday's buckets: the
// midnight reset needs them to start the day at zero instead of refusing.
func TestSweepKeepsYesterdayWhileDegraded(t *testing.T) {
	f := &fakeFleet{degraded: 1}
	l := NewInMemoryLimiter()
	l.SetFleet(f)
	b := l.getOrCreateDaily(bucketKey("t", ""))
	b.day, b.loadedDay = utcDayIndex()-1, utcDayIndex()-1
	l.SweepIdle()
	if _, ok := l.dailyBuckets.Load(bucketKey("t", "")); !ok {
		t.Fatal("yesterday's bucket swept during an outage")
	}
	if ok, _, _ := l.AllowInputTokens("t", "", 1000, 10); !ok {
		t.Fatal("daily key refused after midnight during an outage")
	}
	f.degraded = 0
	b.day = utcDayIndex() - 1
	l.SweepIdle()
	if _, ok := l.dailyBuckets.Load(bucketKey("t", "")); ok {
		t.Fatal("yesterday's bucket kept while the store answers")
	}
}

// Output tokens past the daily limit are already spent: the other routers
// must see the bucket full.
func TestOutputOverflowIsShared(t *testing.T) {
	f := &fakeFleet{}
	l := NewInMemoryLimiter()
	l.SetFleet(f)
	if ok, _, _ := l.AllowInputTokens("t", "", 1000, 900); !ok {
		t.Fatal("input refused")
	}
	if ok, _, _ := l.AllowOutputTokens("t", "", 1000, 300); ok {
		t.Fatal("output past the limit reported as allowed")
	}
	if got := f.recorded[shared.KindDaily]; got != 1000 {
		t.Fatalf("published %d daily tokens, want 1000 (900 + the 100 left)", got)
	}
}

// Concurrent first requests for one key share a single read of the total.
func TestDailyReadShared(t *testing.T) {
	f := &fakeFleet{readWait: 50 * time.Millisecond}
	l := NewInMemoryLimiter()
	l.SetFleet(f)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); l.AllowInputTokens("t", "", 1000, 1) }()
	}
	wg.Wait()
	if n := f.reads.Load(); n != 1 {
		t.Fatalf("%d reads of the daily total, want 1", n)
	}
}

// After a resync the total is read again; if that read fails the router
// keeps the total it had instead of refusing.
func TestDailyReadAgainAfterResync(t *testing.T) {
	f := &fakeFleet{total: 100}
	l := NewInMemoryLimiter()
	l.SetFleet(f)
	if rem, _ := l.RemainingTokens("t", "", 1000); rem != 900 {
		t.Fatalf("remaining %d, want 900", rem)
	}
	f.total, f.resync = 600, 1
	if rem, _ := l.RemainingTokens("t", "", 1000); rem != 400 {
		t.Fatalf("remaining %d after a resync, want 400", rem)
	}
	f.resync, f.readWait = 2, time.Second // the read times out
	if rem, _ := l.RemainingTokens("t", "", 1000); rem != 400 {
		t.Fatalf("remaining %d when the reload failed, want the last total's 400", rem)
	}
}
