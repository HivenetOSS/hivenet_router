// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"context"
	"errors"
	"sync"
)

// ErrQueueFull is returned by WaitQueue.Wait when the per-model queue is at its
// capacity bound. The caller should escalate to the next fallback policy step
// rather than waiting indefinitely — the queue itself is overloaded.
var ErrQueueFull = errors.New("request queue full: too many requests waiting for capacity")

// WaitQueue is a bounded, per-model FIFO queue of goroutines waiting for a
// concurrency slot to become available.
//
// When all agents for a model are at capacity, goroutines park in the queue via
// Wait() instead of immediately escalating to a fallback policy step.
// When any agent frees a slot (DecrementLoad) or a new agent registers,
// Signal() wakes the head-of-queue goroutine so it can retry selection.
//
// onEnqueue and onDequeue are optional callbacks invoked immediately after the
// goroutine is appended to (or removed from) the waiter list, while the mutex
// is NOT held. Use them to emit depth-gauge metrics without coupling queue.go
// to any metrics package.
//
// WaitQueue is safe for concurrent use.
type WaitQueue struct {
	mu        sync.Mutex
	waiters   []chan struct{}
	bound     int
	onEnqueue func() // called after goroutine is appended; nil = no-op
	onDequeue func() // called after goroutine is removed on any path; nil = no-op
}

// newWaitQueue creates a WaitQueue with the given maximum depth.
// bound must be > 0.
func newWaitQueue(bound int) *WaitQueue {
	return &WaitQueue{bound: bound}
}

// Wait parks the calling goroutine in the queue until one of three things happens:
//   - Signal() is called (a slot became available) → returns nil.
//   - ctx is cancelled or its deadline expires → returns ctx.Err().
//   - The queue is already at its bound → returns ErrQueueFull immediately (no parking).
//
// On nil return, the caller should retry agent selection.
// On non-nil return, the caller should escalate to the next fallback step.
func (q *WaitQueue) Wait(ctx context.Context) error {
	ch := make(chan struct{}, 1)

	q.mu.Lock()
	if len(q.waiters) >= q.bound {
		q.mu.Unlock()
		return ErrQueueFull
	}
	q.waiters = append(q.waiters, ch)
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
			if w == ch {
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
	ch := q.waiters[0]
	q.waiters = q.waiters[1:]
	ch <- struct{}{}
}

// Depth returns the current number of goroutines parked in the queue.
func (q *WaitQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiters)
}
