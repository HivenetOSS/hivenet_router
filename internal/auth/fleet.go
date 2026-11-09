// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Fleet shares rate and daily usage with the other routers of a policloud
// (implemented by *shared.Store). Each router keeps deciding locally; the
// fleet publishes what it admitted and hands back what the others admitted.
type Fleet interface {
	// Record publishes usage this router admitted.
	Record(kind byte, bucket string, limit, n int)
	// Size is the number of live routers, this one included.
	Size() int
	// DegradedEpoch is 0 while the shared store answers, else an outage number.
	DegradedEpoch() uint64
	// SyncSeq changes each time the other routers' usage has been applied.
	SyncSeq() uint64
	// DailyTotal returns the policloud's usage today for a daily bucket and
	// the last usage entry the total includes. It fails at once, without
	// waiting, while the store does not answer.
	DailyTotal(ctx context.Context, bucket string) (total int, mark string, err error)
}

// streamAfter reports whether stream entry id comes after mark ("ms-seq").
func streamAfter(id, mark string) bool {
	if mark == "" {
		return true
	}
	im, is := splitStreamID(id)
	mm, ms := splitStreamID(mark)
	return im > mm || (im == mm && is > ms)
}

func splitStreamID(id string) (int64, int64) {
	a, b, _ := strings.Cut(id, "-")
	ms, _ := strconv.ParseInt(a, 10, 64)
	seq, _ := strconv.ParseInt(b, 10, 64)
	return ms, seq
}

// Usage kinds (must match package shared).
const (
	usageRPM   byte = 'r'
	usageDaily byte = 'd'
	usageITPM  byte = 'i'
	usageOTPM  byte = 'o'
)

// fleetShare divides a burst among the live routers, never below 1.
func fleetShare(burst int, f Fleet) int {
	if f == nil {
		return burst
	}
	n := f.Size()
	if n <= 1 {
		return burst
	}
	if b := burst / n; b > 1 {
		return b
	}
	return 1
}

// lease is one bucket's per-router state with shared counters.
//
// Each router's bucket is its view of the policloud-wide bucket: full burst,
// refilled at the full rate, minus what every router spent (its own at once,
// the others' at each sync). Between two syncs it cannot see the others, so it
// may spend at most its share — 1/N — of what the bucket held at the last
// sync; the routers together then never spend more than the bucket held.
//
// While the shared store is down there are no syncs: each router enforces its
// share of the limit on its own (rate ÷ N, burst ÷ N).
type lease struct {
	mu    sync.Mutex
	seq   uint64
	spent int
	cap   int
	epoch uint64
	deg   *rate.Limiter
}

// leases maps a bucket key to its lease.
type leases struct{ m sync.Map }

func (ls *leases) get(key string) *lease {
	if v, ok := ls.m.Load(key); ok {
		return v.(*lease)
	}
	v, _ := ls.m.LoadOrStore(key, &lease{})
	return v.(*lease)
}

func (ls *leases) drop(key string) { ls.m.Delete(key) }

func (ls *leases) reset() {
	ls.m.Range(func(k, _ any) bool { ls.m.Delete(k); return true })
}

// allowShared admits n tokens from lim under the fleet rules above. oversize
// lets a full bucket admit more than its burst (one maximum-size prompt).
func allowShared(f Fleet, ls *leases, key string, lim *rate.Limiter, n int, oversize bool) bool {
	now := time.Now()
	nr := f.Size()
	le := ls.get(key)
	le.mu.Lock()
	defer le.mu.Unlock()
	if e := f.DegradedEpoch(); e != 0 {
		if le.epoch != e || le.deg == nil {
			b := fleetShare(lim.Burst(), f)
			le.epoch = e
			le.deg = rate.NewLimiter(lim.Limit()/rate.Limit(max(nr, 1)), b)
			// Start from this router's share of what is left, not a full bucket.
			if left := int(lim.TokensAt(now)) / max(nr, 1); left < b {
				le.deg.ReserveN(now, b-max(left, 0))
			}
		}
		ok := le.deg.AllowN(now, n)
		if !ok && oversize && n > le.deg.Burst() && le.deg.TokensAt(now) >= float64(le.deg.Burst()) {
			debit(le.deg, n)
			ok = true
		}
		if ok {
			debit(lim, n)
		}
		return ok
	}
	le.deg = nil
	if nr <= 1 {
		return allowLocal(lim, n, oversize, now)
	}
	if seq := f.SyncSeq(); le.seq != seq {
		le.seq, le.spent = seq, 0
		le.cap = max(1, int(lim.TokensAt(now))/nr)
	}
	if le.spent > 0 && le.spent+n > le.cap {
		return false
	}
	if !allowLocal(lim, n, oversize, now) {
		return false
	}
	le.spent += n
	return true
}

func allowLocal(lim *rate.Limiter, n int, oversize bool, now time.Time) bool {
	if lim.AllowN(now, n) {
		return true
	}
	if oversize && n > lim.Burst() && lim.TokensAt(now) >= float64(lim.Burst()) {
		debit(lim, n)
		return true
	}
	return false
}

// debit removes n tokens from a bucket that other routers spent. A token
// bucket can go below zero (ReserveN), which is what makes a peer's burst
// count here too. Chunks stay within the burst so ReserveN accepts them; the
// total is capped at two minutes of rate, beyond which the bucket is just empty.
func debit(lim *rate.Limiter, n int) {
	b := lim.Burst()
	if b <= 0 || n <= 0 {
		return
	}
	if max := int(float64(lim.Limit())*120) + b; n > max {
		n = max
	}
	now := time.Now()
	for n > 0 {
		k := min(n, b)
		lim.ReserveN(now, k)
		n -= k
	}
}

// decayed scales replayed usage by how much of it the bucket would already
// have refilled: usage older than the refill window no longer counts.
func decayed(n int, age, window time.Duration) int {
	if age <= 2*time.Second || window <= 0 {
		return n
	}
	f := 1 - float64(age)/float64(window)
	if f <= 0 {
		return 0
	}
	return int(float64(n) * f)
}
