// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package admission implements the KV-occupancy admit budget: a token-weighted
// count of in-flight work per model. A request's footprint is its input tokens
// plus its output — the declared max_tokens reserved up front, or, when the
// output is undeclared, a live count that grows as the response streams. A new
// request is admitted only while the running weighted sum plus its own footprint
// stays within the budget, so a burst of large requests degrades to a clean 429
// rather than overcommitting the engine's KV cache and inflating everyone's
// latency. The budget is the engine's per-replica token capacity scaled by an
// operator admit fraction; both are supplied by the caller per request.
//
// The accounting is pure and engine-agnostic: it counts tokens the router
// already estimates and never reads engine internals. All state is per model
// name; nothing here knows about replicas, agents, or a specific engine.
package admission

import (
	"context"
	"sync"
	"time"
)

// Controller holds the per-model weighted in-flight state and applies the admit
// fraction to each request's budget. It is safe for concurrent use.
type Controller struct {
	admitFraction float64       // scales the caller's budget tokens; (0,1]
	parkFor       time.Duration // how long an over-budget request waits for room before 429

	mu     sync.Mutex
	models map[string]*modelState
}

// NewController returns a Controller. admitFraction scales every request's
// budget (e.g. 0.85 admits up to 85% of the engine's KV token capacity); values
// outside (0,1] are clamped to 1. parkFor bounds how long an over-budget request
// waits for capacity to free before it is rejected; 0 means reject immediately.
func NewController(admitFraction float64, parkFor time.Duration) *Controller {
	if admitFraction <= 0 || admitFraction > 1 {
		admitFraction = 1
	}
	return &Controller{
		admitFraction: admitFraction,
		parkFor:       parkFor,
		models:        make(map[string]*modelState),
	}
}

// modelState is the weighted in-flight accounting for one model. Its own mutex
// guards the counters and the broadcast channel; the Controller mutex guards
// only the map that hands these out.
type modelState struct {
	mu     sync.Mutex
	sumW   int64         // Σ footprint over in-flight requests for this model
	count  int           // number of in-flight requests (the max_inflight backstop)
	notify chan struct{} // closed-and-replaced on every release to wake parked waiters
}

func (c *Controller) stateFor(model string) *modelState {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.models[model]
	if s == nil {
		s = &modelState{notify: make(chan struct{})}
		c.models[model] = s
	}
	return s
}

// Admit reserves footprint tokens for a request against model's budget, parking
// up to the configured park duration if there is no room yet, until capacity
// frees or ctx is done.
//
//   - footprint is the weight to charge now: input + declared max_tokens for a
//     declared request, or just input for an undeclared one.
//   - grows marks an undeclared request whose reservation is grown via
//     Reservation.Grow as output streams; a declared request passes false.
//   - budgetTokens is the model's per-replica KV token capacity; the admit
//     fraction is applied here. budgetTokens <= 0 disables the budget check.
//   - maxInflight caps concurrent in-flight requests; <= 0 disables the backstop.
//
// It returns a Reservation to release exactly once when the request finishes, or
// nil if the request could not be admitted within the park window. The caller
// owns release (a single deferred Release on every exit path); the reservation
// is never released here.
func (c *Controller) Admit(ctx context.Context, model string, footprint int, grows bool, budgetTokens, maxInflight int) *Reservation {
	s := c.stateFor(model)
	w := int64(footprint)
	budget := c.effectiveBudget(budgetTokens)

	if s.tryReserve(w, budget, maxInflight) {
		return &Reservation{state: s, weight: w, grows: grows}
	}
	// A request whose own footprint exceeds the budget can never fit — releases
	// only lower the in-flight sum toward zero — so reject it now rather than
	// parking a doomed request for the full window. The max_inflight dimension is
	// deliberately not short-circuited: a release frees a count slot, so parking
	// for one is worthwhile.
	if budget > 0 && w > budget {
		return nil
	}
	if c.parkFor <= 0 {
		return nil
	}

	timer := time.NewTimer(c.parkFor)
	defer timer.Stop()
	for {
		// Grab the current wake channel BEFORE re-checking, so a release that
		// happens between the check and the wait cannot be missed.
		ch := s.notifyCh()
		if s.tryReserve(w, budget, maxInflight) {
			return &Reservation{state: s, weight: w, grows: grows}
		}
		select {
		case <-ch:
			// A request released; loop and retry.
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// effectiveBudget applies the admit fraction. A non-positive budget disables the
// check (tryReserve treats <= 0 as "no budget limit"). A positive budget is
// floored at 1 so the fraction can never truncate it to 0 — which tryReserve
// would read as "unlimited", admitting everything, the exact opposite of what a
// small budget should do.
func (c *Controller) effectiveBudget(budgetTokens int) int64 {
	if budgetTokens <= 0 {
		return 0
	}
	b := int64(c.admitFraction * float64(budgetTokens))
	if b < 1 {
		b = 1
	}
	return b
}

// Occupancy returns the current weighted in-flight sum and request count for a
// model, for metrics and tests. A model never seen returns zero.
func (c *Controller) Occupancy(model string) (sumW int64, count int) {
	c.mu.Lock()
	s := c.models[model]
	c.mu.Unlock()
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sumW, s.count
}

// tryReserve adds weight if it fits both the budget and the in-flight backstop,
// reporting whether it did. A budget or backstop of <= 0 disables that check.
func (s *modelState) tryReserve(weight, budget int64, maxInflight int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if budget > 0 && s.sumW+weight > budget {
		return false
	}
	if maxInflight > 0 && s.count+1 > maxInflight {
		return false
	}
	s.sumW += weight
	s.count++
	return true
}

// release returns weight to the budget and wakes every parked waiter by closing
// the current notify channel and installing a fresh one.
func (s *modelState) release(weight int64) {
	s.mu.Lock()
	s.sumW -= weight
	if s.sumW < 0 {
		s.sumW = 0
	}
	s.count--
	if s.count < 0 {
		s.count = 0
	}
	ch := s.notify
	s.notify = make(chan struct{})
	s.mu.Unlock()
	close(ch)
}

// grow tightens occupancy as an undeclared request produces output. It never
// frees capacity, so it does not wake waiters.
func (s *modelState) grow(delta int64) {
	s.mu.Lock()
	s.sumW += delta
	s.mu.Unlock()
}

func (s *modelState) notifyCh() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}
