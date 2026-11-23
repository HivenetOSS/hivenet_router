// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package wake_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/wake"
)

// demandSource is a fake demand the feed reads.
type demandSource struct {
	mu sync.Mutex
	m  map[string]wake.DemandEntry
}

func (d *demandSource) set(model string, pending, active int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.m[model] = wake.DemandEntry{Pending: pending, Active: active}
}

func (d *demandSource) drop(model string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.m, model)
}

func (d *demandSource) read() map[string]wake.DemandEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]wake.DemandEntry, len(d.m))
	for k, v := range d.m {
		out[k] = v
	}
	return out
}

// startFeed runs a feed whose safety-net tick is too slow to matter, so
// only Notify publishes, and waits for its first publication.
func startFeed(t *testing.T, src *demandSource) *wake.Feed {
	t.Helper()
	f := wake.NewFeed(src.read, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snap, _, unsub := f.Subscribe()
		unsub()
		if len(snap) == len(src.read()) {
			return f
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("feed did not publish its first snapshot")
	return nil
}

func next(t *testing.T, ch <-chan wake.DemandEntry) wake.DemandEntry {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("channel closed")
		}
		return e
	case <-time.After(time.Second):
		t.Fatal("no event within 1s")
		return wake.DemandEntry{}
	}
}

func quiet(t *testing.T, ch <-chan wake.DemandEntry) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("unexpected event %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestFeed_SnapshotThenChanges checks the subscriber's view: every model
// at once, then one event per change, and nothing when nothing changed.
func TestFeed_SnapshotThenChanges(t *testing.T) {
	src := &demandSource{m: map[string]wake.DemandEntry{}}
	src.set("b", 0, 0)
	src.set("a", 0, 1)
	f := startFeed(t, src)

	snap, ch, cancel := f.Subscribe()
	defer cancel()
	if len(snap) != 2 || snap[0].Model != "a" || snap[1].Model != "b" || snap[0].Active != 1 {
		t.Fatalf("snapshot = %+v, want a (active 1) then b", snap)
	}

	src.set("b", 1, 0)
	f.Notify()
	if e := next(t, ch); e.Model != "b" || e.Pending != 1 || e.Active != 0 || e.TS.IsZero() {
		t.Errorf("event = %+v, want b pending 1 with a timestamp", e)
	}

	f.Notify() // nothing changed
	quiet(t, ch)

	src.set("b", 0, 1) // woke: the request got its agent
	f.Notify()
	if e := next(t, ch); e.Model != "b" || e.Pending != 0 || e.Active != 1 {
		t.Errorf("event = %+v, want b pending 0 active 1", e)
	}
}

// TestFeed_RemovedModel checks that a model leaving the sibling map is
// announced once, then forgotten.
func TestFeed_RemovedModel(t *testing.T) {
	src := &demandSource{m: map[string]wake.DemandEntry{}}
	src.set("a", 0, 0)
	f := startFeed(t, src)
	_, ch, cancel := f.Subscribe()
	defer cancel()

	src.drop("a")
	f.Notify()
	if e := next(t, ch); e.Model != "a" || !e.Removed || e.Pending != 0 {
		t.Errorf("event = %+v, want a removed", e)
	}
	if snap, _, c := f.Subscribe(); len(snap) != 0 {
		c()
		t.Errorf("snapshot after removal = %+v, want empty", snap)
	} else {
		c()
	}
}

// TestFeed_SlowSubscriberIsDropped checks that a subscriber that stops
// reading is cut off instead of blocking the feed, and that the others keep
// receiving.
func TestFeed_SlowSubscriberIsDropped(t *testing.T) {
	src := &demandSource{m: map[string]wake.DemandEntry{}}
	src.set("a", 0, 0)
	f := startFeed(t, src)
	_, slow, cancelSlow := f.Subscribe()
	defer cancelSlow()
	_, fast, cancelFast := f.Subscribe()
	defer cancelFast()

	for i := 1; i <= 300; i++ {
		src.set("a", i, 0)
		f.Notify()
		if e := next(t, fast); e.Pending != i {
			t.Fatalf("fast subscriber got %+v, want pending %d", e, i)
		}
	}
	closed := false
	for !closed {
		select {
		case _, ok := <-slow:
			closed = !ok
		case <-time.After(time.Second):
			t.Fatal("slow subscriber was not dropped")
		}
	}
	cancelSlow() // safe after the drop
}

// TestFeed_TickCatchesUnnotifiedChange checks the safety net: a change
// nothing notified (an agent turning unhealthy) still reaches subscribers.
func TestFeed_TickCatchesUnnotifiedChange(t *testing.T) {
	src := &demandSource{m: map[string]wake.DemandEntry{}}
	src.set("a", 0, 1)
	f := wake.NewFeed(src.read, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)
	time.Sleep(30 * time.Millisecond)
	_, ch, unsub := f.Subscribe()
	defer unsub()

	src.set("a", 0, 0)
	if e := next(t, ch); e.Active != 0 {
		t.Errorf("event = %+v, want active 0", e)
	}
}

// TestFeed_StopClosesSubscribers checks that stopping the feed ends every
// stream, so clients reconnect to the next router process.
func TestFeed_StopClosesSubscribers(t *testing.T) {
	src := &demandSource{m: map[string]wake.DemandEntry{}}
	f := wake.NewFeed(src.read, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	_, ch, unsub := f.Subscribe()
	defer unsub()
	cancel()
	<-done
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("got an event, want a closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after stop")
	}
}

// TestHold_OnChangeFollowsDemand checks the holder's hook: it fires when a
// request enters the demand and again when it leaves it, so the feed is told
// at once.
func TestHold_OnChangeFollowsDemand(t *testing.T) {
	h, f, _ := newHolder(t, 10)
	var mu sync.Mutex
	var calls []string
	h.SetOnChange(func(model string) {
		mu.Lock()
		calls = append(calls, model)
		mu.Unlock()
	})
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(calls)
	}

	out := hold(t, h, context.Background(), "m", "r1", long)
	if n := count(); n != 1 {
		t.Fatalf("calls after entering = %d, want 1", n)
	}
	f.set("m", 1)
	h.Wake("m")
	outcome(t, out, time.Second)
	deadline := time.Now().Add(time.Second)
	for count() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := count(); n != 2 {
		t.Errorf("calls after release = %d, want 2", n)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, m := range calls {
		if m != "m" {
			t.Errorf("call for %q, want m", m)
		}
	}
}
