// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package wake

import (
	"context"
	"sort"
	"sync"
	"time"
)

// DemandEntry is one model's demand on this router: how many distinct
// requests wait for it (the pending demand) and how many healthy agents
// serve it.
type DemandEntry struct {
	Model   string `json:"model"`
	Pending int    `json:"pending"`
	Active  int    `json:"active"`
	// Removed is set on the last event of a model this router no longer
	// holds requests for (it left the sibling map). Its counts are 0.
	Removed bool `json:"removed,omitempty"`
	// LastRouted is when the router last sent a request for the model to an
	// agent, to the nearest RoutedGranularity: it moves at most once per
	// RoutedGranularity, so a busy model sends at most one event per
	// interval. Zero when no request was routed since the router started.
	LastRouted time.Time `json:"last_routed,omitzero"`
	TS         time.Time `json:"ts"`
}

func (e DemandEntry) same(o DemandEntry) bool {
	return e.Pending == o.Pending && e.Active == o.Active && e.LastRouted.Equal(o.LastRouted)
}

// RoutedGranularity is how often LastRouted may move: an autoscaler that
// keeps a model up while it serves traffic needs to know it is used, not
// every request.
const RoutedGranularity = 30 * time.Second

// subscriberBuffer is how many events a subscriber may lag behind. A
// subscriber that falls further behind is dropped: its channel closes, and it
// reconnects to get a fresh snapshot instead of a stale backlog.
const subscriberBuffer = 256

// Feed publishes every change of the per-model demand to its subscribers, so
// an external autoscaler hears a wake request at once instead of at its next
// poll. It never decides anything: it reports numbers.
//
// A change is noticed when Notify is called (a request entered or left the
// demand, an agent registered) and, as a safety net, at every tick of Run's
// interval (an agent turning unhealthy, a model entering or leaving the
// sibling map).
type Feed struct {
	source   func() map[string]DemandEntry
	interval time.Duration
	now      func() time.Time
	signal   chan struct{}

	mu   sync.Mutex
	last map[string]DemandEntry
	subs map[chan DemandEntry]struct{}

	routedMu sync.Mutex
	routed   map[string]time.Time
}

// NewFeed returns a Feed that reads the current demand from source: every
// model that may be held here, with its pending demand and healthy agents.
// interval is the safety-net recheck; it must be > 0.
func NewFeed(source func() map[string]DemandEntry, interval time.Duration) *Feed {
	return &Feed{
		source:   source,
		interval: interval,
		now:      time.Now,
		signal:   make(chan struct{}, 1),
		last:     make(map[string]DemandEntry),
		subs:     make(map[chan DemandEntry]struct{}),
		routed:   make(map[string]time.Time),
	}
}

// MarkRouted records that a request for model was sent to an agent. Called
// on every routed request, so it only takes a lock and compares a time; the
// recorded time moves (and an event is sent) at most once per
// RoutedGranularity.
func (f *Feed) MarkRouted(model string) {
	now := f.now().UTC()
	f.routedMu.Lock()
	prev, ok := f.routed[model]
	moved := !ok || now.Sub(prev) >= RoutedGranularity
	if moved {
		f.routed[model] = now
	}
	f.routedMu.Unlock()
	if moved {
		f.Notify()
	}
}

func (f *Feed) lastRouted(model string) time.Time {
	f.routedMu.Lock()
	defer f.routedMu.Unlock()
	return f.routed[model]
}

// Notify asks the feed to recheck the demand now. It never blocks.
func (f *Feed) Notify() {
	select {
	case f.signal <- struct{}{}:
	default:
	}
}

// Run publishes changes until ctx ends.
func (f *Feed) Run(ctx context.Context) {
	t := time.NewTicker(f.interval)
	defer t.Stop()
	f.publish()
	for {
		select {
		case <-ctx.Done():
			f.closeAll()
			return
		case <-f.signal:
			f.publish()
		case <-t.C:
			f.publish()
		}
	}
}

// Subscribe returns the current demand of every model, sorted by model, and a
// channel that receives each later change. The channel closes when the
// subscriber falls behind or the feed stops. cancel must be called.
func (f *Feed) Subscribe() ([]DemandEntry, <-chan DemandEntry, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan DemandEntry, subscriberBuffer)
	f.subs[ch] = struct{}{}
	snapshot := make([]DemandEntry, 0, len(f.last))
	for _, e := range f.last {
		snapshot = append(snapshot, e)
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].Model < snapshot[j].Model })
	return snapshot, ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := f.subs[ch]; ok {
			delete(f.subs, ch)
			close(ch)
		}
	}
}

// publish reads the demand and sends every change to every subscriber.
func (f *Feed) publish() {
	cur := f.source()
	now := f.now().UTC()
	f.mu.Lock()
	defer f.mu.Unlock()
	var changes []DemandEntry
	for model, e := range cur {
		e.Model, e.TS = model, now
		e.LastRouted = f.lastRouted(model)
		if old, ok := f.last[model]; ok && old.same(e) {
			continue
		}
		f.last[model] = e
		changes = append(changes, e)
	}
	for model := range f.last {
		if _, ok := cur[model]; !ok {
			delete(f.last, model)
			changes = append(changes, DemandEntry{Model: model, Removed: true, TS: now})
		}
	}
	if len(changes) == 0 {
		return
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Model < changes[j].Model })
	for ch := range f.subs {
		for _, e := range changes {
			select {
			case ch <- e:
			default:
				// Too far behind: drop it, it resubscribes for a fresh
				// snapshot.
				delete(f.subs, ch)
				close(ch)
			}
			if _, ok := f.subs[ch]; !ok {
				break
			}
		}
	}
}

func (f *Feed) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.subs {
		delete(f.subs, ch)
		close(ch)
	}
}
