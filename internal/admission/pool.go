// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package admission

import (
	"context"
	"time"
)

// Pool holds a budget for every router of a site. Each router admits against
// the share of it that it holds, so the routers together never admit more
// than the budget (implemented by shared.Credits).
type Pool interface {
	// Take reserves weight and one request under key if the router's share
	// of the site-wide budget has room, refilling the share when it is short.
	Take(ctx context.Context, key string, weight, budget int64, maxInflight int) bool
	// Adjust reports a change to what key holds: a release (negative weight,
	// count -1), streamed growth or a true-up.
	Adjust(key string, dw int64, dc int)
}

// SetPool makes the controller admit against a site-wide budget. Call once at
// startup, before any request.
func (c *Controller) SetPool(p Pool) { c.pool = p }

// poolPoll is how often a request parked on a site-wide budget re-checks it:
// releases on other routers do not wake it.
const poolPoll = 20 * time.Millisecond

// tryAdmit is tryReserve, against the site-wide pool when there is one. The
// pool decides; this controller's sums then record this router's share, for
// the gauges and the release path.
func (c *Controller) tryAdmit(ctx context.Context, s *modelState, w, budget int64, maxInflight int) bool {
	if c.pool == nil {
		return s.tryReserve(w, budget, maxInflight)
	}
	if !c.pool.Take(ctx, s.model, w, budget, maxInflight) {
		return false
	}
	s.mu.Lock()
	s.budget, s.maxInflight = budget, maxInflight
	s.sumW += w
	s.count++
	s.mu.Unlock()
	s.emit()
	return true
}

// track records r as live until it is released. Caller does not hold s.mu.
func (s *modelState) track(r *Reservation) *Reservation {
	s.mu.Lock()
	if s.live == nil {
		s.live = map[*Reservation]struct{}{}
	}
	s.live[r] = struct{}{}
	s.mu.Unlock()
	return r
}

func (s *modelState) untrack(r *Reservation) {
	s.mu.Lock()
	delete(s.live, r)
	s.mu.Unlock()
}

// ReapStale releases every reservation admitted longer than maxAge ago and
// returns how many it released. A request never outlives its deadline, so a
// reservation older than that was leaked by an exit path that skipped its
// release: reaping it gives the budget back (and, with a site-wide pool, the
// credits) instead of holding it forever. Each reap is a bug caught.
func (c *Controller) ReapStale(maxAge time.Duration) int {
	cutoff := time.Now().Add(-maxAge)
	c.mu.Lock()
	states := make([]*modelState, 0, len(c.models))
	for _, s := range c.models {
		states = append(states, s)
	}
	c.mu.Unlock()
	var stale []*Reservation
	for _, s := range states {
		s.mu.Lock()
		for r := range s.live {
			if r.at.Before(cutoff) {
				stale = append(stale, r)
			}
		}
		s.mu.Unlock()
	}
	for _, r := range stale {
		r.Release()
	}
	return len(stale)
}
