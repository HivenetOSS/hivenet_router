// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package wake holds requests for a model that is scaled to zero until an
// agent for it is healthy again.
//
// The router never starts or stops models. It holds the request, publishes
// how many distinct requests are waiting for each model (the pending demand),
// and releases them when an agent serving the model registers. An external
// autoscaler reads the pending demand and scales the model from zero.
package wake

import (
	"context"
	"sort"
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
	// Full: the model already holds the maximum number of requests.
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

// Holder holds requests per model. It is safe for concurrent use.
type Holder struct {
	maxPerModel int
	healthy     func(model string) int
	poll        time.Duration
	onDone      func(model string, outcome Outcome, held time.Duration)

	mu     sync.Mutex
	models map[string]*modelHold
}

type modelHold struct {
	holds  int            // requests held now, duplicates included
	demand map[string]int // distinct demand keys → holds carrying that key
	wake   chan struct{}  // closed by Wake, then replaced
}

// NewHolder returns a Holder that holds at most maxPerModel requests per
// model. healthy reports how many healthy agents serve a model; a hold ends
// as Woken as soon as it is above zero. onDone, if not nil, is called when a
// hold ends, with how long the request was held (0 for Full).
func NewHolder(maxPerModel int, healthy func(model string) int, onDone func(model string, outcome Outcome, held time.Duration)) *Holder {
	return &Holder{
		maxPerModel: maxPerModel,
		healthy:     healthy,
		poll:        time.Second,
		onDone:      onDone,
		models:      make(map[string]*modelHold),
	}
}

// SetPollInterval sets how often a held request rechecks the model's health
// between wake-ups. Wake releases holds at once; the poll covers an agent
// becoming healthy without registering again. For tests.
func (h *Holder) SetPollInterval(d time.Duration) { h.poll = d }

// Hold holds the calling request until the model has a healthy agent
// (Woken), the budget runs out (Expired), ctx ends (Gone), or OnKeepAlive
// fails (Gone). It returns Full at once when the model already holds the
// maximum number of requests.
//
// demandKey identifies the request in the pending demand: holds with the
// same key (a client retrying the same request) count once.
func (h *Holder) Hold(ctx context.Context, model, demandKey string, opts Options) Outcome {
	start := time.Now()
	wake, ok := h.enter(model, demandKey)
	if !ok {
		h.done(model, Full, 0)
		return Full
	}
	outcome := h.wait(ctx, model, wake, opts)
	h.leave(model, demandKey)
	h.done(model, outcome, time.Since(start))
	return outcome
}

func (h *Holder) wait(ctx context.Context, model string, wake <-chan struct{}, opts Options) Outcome {
	budget := time.NewTimer(opts.Budget)
	defer budget.Stop()
	poll := time.NewTicker(h.poll)
	defer poll.Stop()
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
		case <-poll.C:
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

// Demand returns the pending demand: for every model with held requests,
// how many distinct requests are held.
func (h *Holder) Demand() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.models))
	for model, m := range h.models {
		out[model] = len(m.demand)
	}
	return out
}

// Held returns the models with held requests, sorted.
func (h *Holder) Held() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.models))
	for model := range h.models {
		out = append(out, model)
	}
	sort.Strings(out)
	return out
}

func (h *Holder) enter(model, demandKey string) (<-chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.models[model]
	if !ok {
		m = &modelHold{demand: make(map[string]int), wake: make(chan struct{})}
		h.models[model] = m
	}
	if m.holds >= h.maxPerModel {
		return nil, false
	}
	m.holds++
	m.demand[demandKey]++
	return m.wake, true
}

func (h *Holder) leave(model, demandKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.models[model]
	m.holds--
	if m.demand[demandKey]--; m.demand[demandKey] == 0 {
		delete(m.demand, demandKey)
	}
	if m.holds == 0 {
		// No entry is kept for a model nobody waits for, so the state never
		// grows with the number of models ever held.
		delete(h.models, model)
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
