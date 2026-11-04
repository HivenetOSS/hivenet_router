// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// park starts a goroutine waiting on q and returns a channel that receives
// its result. It returns once the goroutine is in the queue.
func park(t *testing.T, q *WaitQueue, ctx context.Context) <-chan error {
	t.Helper()
	before := q.Depth()
	done := make(chan error, 1)
	go func() { done <- q.Wait(ctx) }()
	deadline := time.Now().Add(time.Second)
	for q.Depth() == before {
		if time.Now().After(deadline) {
			t.Fatal("waiter did not enter the queue")
		}
		time.Sleep(time.Millisecond)
	}
	return done
}

func result(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("waiter was not woken")
		return nil
	}
}

func stillParked(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("waiter returned %v, want it still parked", err)
	case <-time.After(30 * time.Millisecond):
	}
}

// TestWaitQueue_SignalWakesHeadFirst checks FIFO order: each Signal wakes the
// oldest waiter and only that one.
func TestWaitQueue_SignalWakesHeadFirst(t *testing.T) {
	q := newWaitQueue(10)
	first := park(t, q, context.Background())
	second := park(t, q, context.Background())

	q.Signal()
	if err := result(t, first); err != nil {
		t.Fatalf("first waiter: %v", err)
	}
	stillParked(t, second)

	q.Signal()
	if err := result(t, second); err != nil {
		t.Fatalf("second waiter: %v", err)
	}
	if d := q.Depth(); d != 0 {
		t.Fatalf("depth = %d, want 0", d)
	}
}

// TestWaitQueue_BroadcastWakesEveryWaiter checks that Broadcast empties the
// queue: new capacity of unknown size must reach every waiter.
func TestWaitQueue_BroadcastWakesEveryWaiter(t *testing.T) {
	q := newWaitQueue(10)
	var waiters []<-chan error
	for range 5 {
		waiters = append(waiters, park(t, q, context.Background()))
	}
	q.Broadcast()
	for i, w := range waiters {
		if err := result(t, w); err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
	if d := q.Depth(); d != 0 {
		t.Fatalf("depth = %d, want 0", d)
	}
	q.Broadcast() // empty queue: a no-op
}

// TestWaitQueue_FullQueueRefusesAtOnce checks the bound: a waiter beyond it
// gets ErrQueueFull without parking.
func TestWaitQueue_FullQueueRefusesAtOnce(t *testing.T) {
	q := newWaitQueue(1)
	parked := park(t, q, context.Background())
	if err := q.Wait(context.Background()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Wait on a full queue = %v, want ErrQueueFull", err)
	}
	q.Signal()
	if err := result(t, parked); err != nil {
		t.Fatalf("parked waiter: %v", err)
	}
}

// TestWaitQueue_ExpiredWaiterLeavesQueue checks that a waiter whose context
// ends is removed, so a later Signal goes to a live waiter.
func TestWaitQueue_ExpiredWaiterLeavesQueue(t *testing.T) {
	q := newWaitQueue(10)
	ctx, cancel := context.WithCancel(context.Background())
	expired := park(t, q, ctx)
	live := park(t, q, context.Background())

	cancel()
	if err := result(t, expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired waiter = %v, want context.Canceled", err)
	}
	if d := q.Depth(); d != 1 {
		t.Fatalf("depth after expiry = %d, want 1", d)
	}
	q.Signal()
	if err := result(t, live); err != nil {
		t.Fatalf("live waiter: %v", err)
	}
}

// TestWaitQueue_SignalRacingExpiryIsNotLost races a Signal against the head
// waiter's context ending. Either the head takes the slot, or it passes the
// slot on to the next waiter: a freed slot is never lost.
func TestWaitQueue_SignalRacingExpiryIsNotLost(t *testing.T) {
	for i := range 200 {
		q := newWaitQueue(10)
		ctx, cancel := context.WithCancel(context.Background())
		head := park(t, q, ctx)
		next := park(t, q, context.Background())

		go cancel()
		q.Signal()

		if err := result(t, head); err == nil {
			// The head took the slot; the next waiter must still be parked.
			stillParked(t, next)
			continue
		}
		if err := result(t, next); err != nil {
			t.Fatalf("iteration %d: the head expired but the slot was not passed on: %v", i, err)
		}
	}
}

// TestWaitQueue_DepthCallbacks checks that the depth callbacks fire on enqueue
// and on every exit path.
func TestWaitQueue_DepthCallbacks(t *testing.T) {
	q := newWaitQueue(10)
	var enq, deq atomic.Int64
	q.onEnqueue = func() { enq.Add(1) }
	q.onDequeue = func() { deq.Add(1) }

	woken := park(t, q, context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	expired := park(t, q, ctx)

	q.Signal()
	_ = result(t, woken)
	cancel()
	_ = result(t, expired)

	if enq.Load() != 2 || deq.Load() != 2 {
		t.Fatalf("callbacks: enqueue %d, dequeue %d; want 2 and 2", enq.Load(), deq.Load())
	}
}

// TestWaitQueue_ReleasedFromHoldPassesTheBound checks that a request released
// by a wake may wait past the queue's bound, while any other request is still
// refused.
func TestWaitQueue_ReleasedFromHoldPassesTheBound(t *testing.T) {
	q := newWaitQueue(1)
	first := park(t, q, context.Background())
	if err := q.Wait(context.Background()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("a normal request on a full queue = %v, want ErrQueueFull", err)
	}
	released := park(t, q, WithReleasedFromHold(context.Background()))
	if d := q.Depth(); d != 2 {
		t.Fatalf("depth = %d, want 2", d)
	}
	q.Broadcast()
	for _, w := range []<-chan error{first, released} {
		if err := result(t, w); err != nil {
			t.Fatalf("waiter: %v", err)
		}
	}
}
