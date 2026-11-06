// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
)

// ErrQueueFull is returned by WaitQueue.Wait when the per-model queue is at its
// capacity bound. The caller should escalate to the next fallback policy step
// rather than waiting indefinitely — the queue itself is overloaded.
var ErrQueueFull = errors.New("request queue full: too many requests waiting for capacity")

// releasedFromHoldKey marks the context of a request released by a wake.
// The value is the request's onSlot hook (possibly nil).
type releasedFromHoldKey struct{}

// WithReleasedFromHold marks ctx as the context of a request that was held
// while its model woke from zero, then released. Such a request may wait in
// the capacity queue past the queue's bound: the hold limits already bound
// how many there are, and refusing them right after the wait would turn a
// wake into a burst of errors. onSlot, if not nil, is called when selection
// gives the request an agent slot: it stops waiting for capacity.
func WithReleasedFromHold(ctx context.Context, onSlot func()) context.Context {
	return context.WithValue(ctx, releasedFromHoldKey{}, &onSlot)
}

func releasedFromHold(ctx context.Context) bool {
	_, ok := ctx.Value(releasedFromHoldKey{}).(*func())
	return ok
}

// gotSlot calls the onSlot hook of a released request, if any.
func gotSlot(ctx context.Context) {
	if f, ok := ctx.Value(releasedFromHoldKey{}).(*func()); ok && *f != nil {
		(*f)()
	}
}

// WaitQueue is a bounded, per-model FIFO queue of goroutines waiting for a
// concurrency slot to become available.
//
// When all agents for a model are at capacity, goroutines park in the queue via
// Wait() instead of immediately escalating to a fallback policy step.
// When an agent frees a slot (DecrementLoad), Signal() wakes the head-of-queue
// goroutine so it can retry selection. When a new agent registers it brings
// several slots at once, so Broadcast() wakes every waiter; those that find no
// free slot park again.
//
// onEnqueue and onDequeue are optional callbacks invoked immediately after the
// goroutine is appended to (or removed from) the waiter list, while the mutex
// is NOT held. Use them to emit depth-gauge metrics without coupling queue.go
// to any metrics package.
//
// WaitQueue is safe for concurrent use.
type WaitQueue struct {
	mu        sync.Mutex
	waiters   []waiter // ordered by ticket: first come, first served
	next      uint64   // the last ticket handed out
	bound     int
	onEnqueue func() // called after goroutine is appended; nil = no-op
	onDequeue func() // called after goroutine is removed on any path; nil = no-op
}

// waiter is one parked goroutine. ticket is its place in line, kept when it
// parks again (see WaitInLine).
type waiter struct {
	ch     chan struct{}
	ticket uint64
}

// newWaitQueue creates a WaitQueue with the given maximum depth.
// bound must be > 0.
func newWaitQueue(bound int) *WaitQueue {
	return &WaitQueue{bound: bound}
}

// Wait parks the calling goroutine in the queue until one of three things happens:
//   - Signal() is called (a slot became available) → returns nil.
//   - ctx is cancelled or its deadline expires → returns ctx.Err().
//   - The queue is already at its bound → returns ErrQueueFull immediately (no
//     parking), unless ctx is marked WithReleasedFromHold.
//
// On nil return, the caller should retry agent selection.
// On non-nil return, the caller should escalate to the next fallback step.
func (q *WaitQueue) Wait(ctx context.Context) error {
	var ticket uint64
	return q.WaitInLine(ctx, &ticket)
}

// WaitInLine is Wait for a goroutine that may park more than once for the
// same request. *ticket is its place in line: 0 the first time, when it gets
// a new place at the tail; it keeps that place when it parks again after a
// wake that gave it no free slot (Broadcast wakes every waiter, and only
// some get a slot). Parking again, it goes back in line by ticket, ahead of
// later arrivals and in its original order, and the bound does not apply:
// it already had its place.
func (q *WaitQueue) WaitInLine(ctx context.Context, ticket *uint64) error {
	ch := make(chan struct{}, 1)

	q.mu.Lock()
	if *ticket == 0 {
		if len(q.waiters) >= q.bound && !releasedFromHold(ctx) {
			q.mu.Unlock()
			return ErrQueueFull
		}
		q.next++
		*ticket = q.next
	}
	i := sort.Search(len(q.waiters), func(i int) bool { return q.waiters[i].ticket > *ticket })
	q.waiters = slices.Insert(q.waiters, i, waiter{ch: ch, ticket: *ticket})
	q.mu.Unlock()

	if q.onEnqueue != nil {
		q.onEnqueue()
	}

	select {
	case <-ch:
		// Signal() already removed us from q.waiters before sending.
		if q.onDequeue != nil {
			q.onDequeue()
		}
		return nil
	case <-ctx.Done():
		// Remove ourselves from the waiter list.
		q.mu.Lock()
		found := false
		for i, w := range q.waiters {
			if w.ch == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				found = true
				break
			}
		}
		q.mu.Unlock()
		if !found {
			// Signal() already removed ch and sent to it before ctx expired.
			// The select non-deterministically chose ctx.Done(), so the capacity
			// slot carried by ch would be lost. Forward it to the next waiter.
			q.Signal()
		}
		if q.onDequeue != nil {
			q.onDequeue()
		}
		return ctx.Err()
	}
}

// Signal wakes the head-of-queue goroutine, if any.
// Called whenever a concurrency slot is freed or new capacity arrives.
func (q *WaitQueue) Signal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.waiters) == 0 {
		return
	}
	w := q.waiters[0]
	q.waiters = q.waiters[1:]
	w.ch <- struct{}{}
}

// Broadcast wakes every goroutine in the queue, in FIFO order. Called when
// new capacity of unknown size arrives (a new agent registered): each woken
// goroutine retries selection, and those that find no free slot park again
// with WaitInLine, keeping their place.
func (q *WaitQueue) Broadcast() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, w := range q.waiters {
		w.ch <- struct{}{}
	}
	q.waiters = nil
}

// Depth returns the current number of goroutines parked in the queue.
func (q *WaitQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiters)
}
