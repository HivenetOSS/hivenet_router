// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package wake holds requests for a model that is scaled to zero until an
// agent for it is healthy again.
//
// The router never starts or stops models. It holds the request, publishes
// how many distinct requests are waiting for each model (the pending demand),
// and releases them when an agent serving the model registers. A released
// request stays in the pending demand until the model starts answering it, so
// the demand also shows the requests still waiting for capacity right after
// the wake. An external autoscaler reads the pending demand and scales the
// model.
package wake

import (
	"context"
	"sync"
	"time"
)

// Outcome is how a hold ended.
type Outcome string

const (
	// Woken: an agent for the model is healthy; serve the request.
	Woken Outcome = "woken"
	// Expired: the hold budget ran out before the model woke.
	Expired Outcome = "expired"
	// Full: a hold limit is reached (per model, per caller or in total).
	Full Outcome = "full"
	// Gone: the client went away during the hold.
	Gone Outcome = "gone"
)

// Options configure one hold.
type Options struct {
	// Budget is the longest the request is held. Required (> 0).
	Budget time.Duration
	// KeepAlive, when > 0, calls OnKeepAlive after the request has been held
	// that long, and again at the same interval, so the connection carries
	// bytes while the model wakes. An error from OnKeepAlive ends the hold
	// as Gone.
	KeepAlive   time.Duration
	OnKeepAlive func() error
}

// Limits bound how many requests a Holder holds at once. Each must be > 0.
type Limits struct {
	PerModel  int // per model
	PerCaller int // per caller (API key) and model, so one caller cannot fill a model's share; not applied to caller ""
	Total     int // across all models, which bounds the memory held requests use
}

// Holder holds requests per model. It is safe for concurrent use.
type Holder struct {
	limits  Limits
	healthy func(model string) int
	poll    time.Duration
	onDone  func(model string, outcome Outcome, held time.Duration)

	mu     sync.Mutex
	total  int // requests held now, all models
	models map[string]*modelHold
}

type modelHold struct {
	holds   int               // requests held now, duplicates included
	callers map[string]int    // caller → requests held now
	demand  map[[2]string]int // distinct (caller, request ID) → requests held or released and not yet answered
	wake    chan struct{}     // closed by Wake, then replaced
	stop    chan struct{}     // closes the health poll; nil while no request is held
}

// NewHolder returns a Holder bounded by limits. healthy reports how many
// healthy agents serve a model; a hold ends as Woken as soon as it is above
// zero. onDone, if not nil, is called when a hold ends, with how long the
// request was held (0 for Full).
func NewHolder(limits Limits, healthy func(model string) int, onDone func(model string, outcome Outcome, held time.Duration)) *Holder {
	return &Holder{
		limits:  limits,
		healthy: healthy,
		poll:    time.Second,
		onDone:  onDone,
		models:  make(map[string]*modelHold),
	}
}

// SetPollInterval sets how often the model's health is rechecked between
// wake-ups. Wake releases holds at once; the poll covers an agent becoming
// healthy without registering again. There is one poll per model while it
// has held requests, not one per request. Call it before the first Hold. For tests.
func (h *Holder) SetPollInterval(d time.Duration) { h.poll = d }

// Hold holds the calling request until the model has a healthy agent
// (Woken), the budget runs out (Expired), ctx ends (Gone), or OnKeepAlive
// fails (Gone). It returns Full at once when a limit is reached.
//
// caller identifies the client (its API key; "" when unknown, which has no
// per-caller limit) and requestID the request: the
// client's own ID when it sent one, else the router's. In the pending
// demand, holds with the same caller and request ID (a client retrying the
// same request) count once; another caller's request with the same ID
// counts separately.
//
// The hold limits count a request only while it is held. The pending demand
// counts a Woken request until the caller calls the returned release, which
// it does when the model starts answering the request or the request ends.
// For any other outcome the request has already left the demand, and
// release does nothing. release may be called more than once.
func (h *Holder) Hold(ctx context.Context, model, caller, requestID string, opts Options) (Outcome, func()) {
	start := time.Now()
	key := [2]string{caller, requestID}
	wake, ok := h.enter(model, caller, key)
	if !ok {
		h.done(model, Full, 0)
		return Full, func() {}
	}
	outcome := h.wait(ctx, model, wake, opts)
	h.leave(model, caller)
	h.done(model, outcome, time.Since(start))
	if outcome != Woken {
		h.leaveDemand(model, key)
		return outcome, func() {}
	}
	var once sync.Once
	return Woken, func() { once.Do(func() { h.leaveDemand(model, key) }) }
}

func (h *Holder) wait(ctx context.Context, model string, wake <-chan struct{}, opts Options) Outcome {
	budget := time.NewTimer(opts.Budget)
	defer budget.Stop()
	var keepAlive <-chan time.Time
	if opts.KeepAlive > 0 && opts.OnKeepAlive != nil {
		t := time.NewTicker(opts.KeepAlive)
		defer t.Stop()
		keepAlive = t.C
	}
	for {
		// Checked after entering the hold, so an agent that registered
		// between the caller's check and now is never missed.
		if h.healthy(model) > 0 {
			return Woken
		}
		select {
		case <-wake:
			wake = h.wakeChan(model)
		case <-keepAlive:
			if err := opts.OnKeepAlive(); err != nil {
				return Gone
			}
		case <-budget.C:
			if h.healthy(model) > 0 {
				return Woken
			}
			return Expired
		case <-ctx.Done():
			return Gone
		}
	}
}

// Wake releases every request held for model so it rechecks the model's
// health now. Call it when an agent serving model registers.
func (h *Holder) Wake(model string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m, ok := h.models[model]; ok {
		close(m.wake)
		m.wake = make(chan struct{})
	}
}

// Demand returns the pending demand: for every model with requests held, or
// released and not yet answered, how many distinct requests there are.
func (h *Holder) Demand() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.models))
	for model, m := range h.models {
		out[model] = len(m.demand)
	}
	return out
}

func (h *Holder) enter(model, caller string, key [2]string) (<-chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.models[model]
	if h.total >= h.limits.Total || (ok && (m.holds >= h.limits.PerModel || (caller != "" && m.callers[caller] >= h.limits.PerCaller))) {
		return nil, false
	}
	if !ok {
		m = &modelHold{
			callers: make(map[string]int),
			demand:  make(map[[2]string]int),
			wake:    make(chan struct{}),
		}
		h.models[model] = m
	}
	if m.stop == nil {
		// The first request held: poll the model's health until none is.
		m.stop = make(chan struct{})
		go h.pollHealth(model, m.stop)
	}
	h.total++
	m.holds++
	m.callers[caller]++
	m.demand[key]++
	return m.wake, true
}

// leave ends a hold: the request no longer counts against the hold limits.
func (h *Holder) leave(model, caller string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.models[model]
	h.total--
	m.holds--
	if m.callers[caller]--; m.callers[caller] == 0 {
		delete(m.callers, caller)
	}
	if m.holds == 0 {
		// Released requests still in the demand need no health poll.
		close(m.stop)
		m.stop = nil
	}
	h.removeIfIdle(model, m)
}

// leaveDemand removes the request from the pending demand.
func (h *Holder) leaveDemand(model string, key [2]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.models[model]
	if m.demand[key]--; m.demand[key] == 0 {
		delete(m.demand, key)
	}
	h.removeIfIdle(model, m)
}

// removeIfIdle removes the model's entry once no request is held or counted
// in its demand, so the state never grows with the number of models ever
// held. Called with h.mu held.
func (h *Holder) removeIfIdle(model string, m *modelHold) {
	if m.holds == 0 && len(m.demand) == 0 {
		delete(h.models, model)
	}
}

// pollHealth wakes the model's held requests when an agent for it is
// healthy, until stop is closed (when no request is held any more). It covers an agent that becomes healthy
// without registering again, which calls Wake.
func (h *Holder) pollHealth(model string, stop <-chan struct{}) {
	t := time.NewTicker(h.poll)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if h.healthy(model) > 0 {
				h.Wake(model)
			}
		}
	}
}

func (h *Holder) wakeChan(model string) <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.models[model].wake
}

func (h *Holder) done(model string, outcome Outcome, held time.Duration) {
	if h.onDone != nil {
		h.onDone(model, outcome, held)
	}
}
