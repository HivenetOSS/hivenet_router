// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package admission

import "sync"

// Reservation is the budget a request holds for its lifetime. The handler owns
// it: it holds the footprint charged at admission, grows it as undeclared output
// streams, and must Release it exactly once when the request finishes — on every
// exit path (success, error, timeout, disconnect, failover). Release is
// idempotent, and Grow after Release is a no-op, so a late streaming callback
// cannot leak budget.
type Reservation struct {
	state *modelState
	grows bool // only undeclared requests grow; declared reserved max_tokens up front

	mu       sync.Mutex
	weight   int64
	released bool
}

// Grow adds streamed output tokens to an undeclared request's footprint so the
// occupancy sum reflects live output. It is a no-op for a declared request
// (which reserved its max_tokens up front), after Release, or for a nil
// reservation, so the caller can invoke it unconditionally per output chunk.
func (r *Reservation) Grow(tokens int) {
	if r == nil || tokens <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released || !r.grows {
		return
	}
	r.weight += int64(tokens)
	r.state.grow(int64(tokens))
}

// Release returns the reservation's full footprint to the budget. It is safe to
// call more than once and on a nil reservation; only the first call has effect.
func (r *Reservation) Release() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return
	}
	r.released = true
	w := r.weight
	r.mu.Unlock()
	r.state.release(w)
}

// Weight returns the footprint currently charged, for tests and metrics.
func (r *Reservation) Weight() int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.weight
}

// Reservations bundles the reservations a single request holds — the global
// occupancy budget plus, on a serverless replica, the per-key occupancy share —
// so the handler releases them together and the processor grows them together.
// It satisfies domain.Reservation. A nil or empty slice is a valid no-op, so the
// handler can always attach and defer-release it unconditionally.
type Reservations []*Reservation

// Grow forwards streamed output growth to every held reservation.
func (rs Reservations) Grow(tokens int) {
	for _, r := range rs {
		r.Grow(tokens)
	}
}

// Release returns every held reservation to its budget. Idempotent per
// reservation, so calling it more than once is safe.
func (rs Reservations) Release() {
	for _, r := range rs {
		r.Release()
	}
}
