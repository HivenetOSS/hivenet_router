// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixhash"
	"hivenet_router/internal/prefixindex"

	"github.com/cespare/xxhash/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PrefixAwareParams tunes the prefix-aware strategy. A zero field takes the
// default (DefaultPrefix* below).
type PrefixAwareParams struct {
	// KVThreshold excludes an agent whose KV-cache use (0–1) is at or above
	// it: a block cached there now would likely be evicted before reuse.
	// Agents that do not report it are never excluded for it.
	KVThreshold float64 `yaml:"kv_threshold" json:"kv_threshold,omitempty"`
	// MaxWaiting excludes an agent with more requests waiting in its engine.
	MaxWaiting int `yaml:"max_waiting" json:"max_waiting,omitempty"`
	// BusyFactor excludes an agent whose requests in flight are above
	// BusyFactor times the candidates' mean (and above 4).
	BusyFactor float64 `yaml:"busy_factor" json:"busy_factor,omitempty"`
	// TieEpsilon makes candidates whose estimated cost is within this share of
	// the best (or 5 ms) a tie, broken the same way on every router.
	TieEpsilon float64 `yaml:"tie_epsilon" json:"tie_epsilon,omitempty"`
	// PrefillRate is the prefill speed assumed for every agent, in tokens per
	// second; it converts tokens to seconds in the cost.
	PrefillRate float64 `yaml:"prefill_rate" json:"prefill_rate,omitempty"`
	// PredictedTTL is how long blocks sent to or served by an agent count as
	// held by it without confirmation from its engine.
	PredictedTTL time.Duration `yaml:"predicted_ttl" json:"predicted_ttl,omitempty"`
	// PendingTTL is how long new text keeps going to the agent it was first
	// sent to, until the agent reports what it served.
	PendingTTL time.Duration `yaml:"pending_ttl" json:"pending_ttl,omitempty"`
}

// Defaults of the prefix_aware block.
const (
	DefaultPrefixKVThreshold  = 0.92
	DefaultPrefixMaxWaiting   = 8
	DefaultPrefixBusyFactor   = 2.0
	DefaultPrefixTieEpsilon   = 0.05
	DefaultPrefixPrefillRate  = 10000.0
	DefaultPrefixPredictedTTL = 120 * time.Second
	DefaultPrefixPendingTTL   = 5 * time.Second
	defaultITLSeconds         = 0.02
	busyFloor                 = 4
)

// Outcomes of a prefix-aware selection: the label of PrefixMetrics.
const (
	PrefixOutcomeHit      = "hit"       // the chosen agent holds blocks of the prompt
	PrefixOutcomePending  = "pending"   // the same new text was just sent to the chosen agent
	PrefixOutcomeMiss     = "miss"      // no candidate holds anything of the prompt
	PrefixOutcomeBusy     = "busy"      // every candidate was busy: least-loaded
	PrefixOutcomeNoAnchor = "no_anchor" // no prompt text: least-loaded
)

// PrefixMetrics observes prefix-aware selections. Implemented by
// *metrics.RouterMetrics; may be nil.
type PrefixMetrics interface {
	PrefixRouted(model, outcome string)
}

// PrefixIndex is the router's prefix memory (*prefixindex.Index).
type PrefixIndex interface {
	Lookup(model string, anchors []uint64, candidates []peer.ID) prefixindex.Match
	Commit(model string, agent peer.ID, anchors []uint64, match prefixindex.Match, blockTTL, pendingTTL time.Duration)
}

// PrefixRequest is the prefix information of one request, set by the
// processor (Session.SetPrefix). The strategy keeps its last lookup here so
// Commit records the choice it made.
type PrefixRequest struct {
	Anchors []uint64 // see prefixhash.Keys.Anchors
	Sizes   []int    // estimated tokens per message

	match    prefixindex.Match
	newToks  int
	selected bool
	// reserved is the agent whose prefill queue holds reservedToks of this
	// request; released at the first response byte (Session.ReleasePrefill).
	reserved     peer.ID
	reservedToks int
	queue        *prefillQueue
}

// release returns the request's reservation in its agent's prefill queue.
func (pr *PrefixRequest) release() {
	if pr == nil || pr.reserved == "" || pr.queue == nil {
		return
	}
	pr.queue.add(pr.reserved, -pr.reservedToks)
	pr.reserved, pr.reservedToks = "", 0
}

// prefillQueue counts, per agent, the prompt tokens this router sent that
// the agent has not started answering yet.
type prefillQueue struct {
	mu sync.Mutex
	n  map[peer.ID]int
}

func (q *prefillQueue) get(id peer.ID) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.n[id]
}

func (q *prefillQueue) add(id peer.ID, d int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.n == nil {
		q.n = map[peer.ID]int{}
	}
	q.n[id] += d
	if q.n[id] <= 0 {
		delete(q.n, id)
	}
}

func (p *PrefixAwareParams) resolved() PrefixAwareParams {
	var r PrefixAwareParams
	if p != nil {
		r = *p
	}
	if r.KVThreshold == 0 {
		r.KVThreshold = DefaultPrefixKVThreshold
	}
	if r.MaxWaiting == 0 {
		r.MaxWaiting = DefaultPrefixMaxWaiting
	}
	if r.BusyFactor == 0 {
		r.BusyFactor = DefaultPrefixBusyFactor
	}
	if r.TieEpsilon == 0 {
		r.TieEpsilon = DefaultPrefixTieEpsilon
	}
	if r.PrefillRate == 0 {
		r.PrefillRate = DefaultPrefixPrefillRate
	}
	if r.PredictedTTL == 0 {
		r.PredictedTTL = DefaultPrefixPredictedTTL
	}
	if r.PendingTTL == 0 {
		r.PendingTTL = DefaultPrefixPendingTTL
	}
	return r
}

func validatePrefixAware(name string, step PolicyStep) error {
	p := step.PrefixAware
	if p == nil {
		return nil
	}
	if step.Strategy != "prefix-aware" {
		return fmt.Errorf("policy: step %q: prefix_aware is only valid with strategy: prefix-aware", name)
	}
	switch {
	case p.KVThreshold != 0 && (p.KVThreshold <= 0 || p.KVThreshold > 1):
		return fmt.Errorf("policy: step %q: prefix_aware.kv_threshold must be in (0, 1], got %g", name, p.KVThreshold)
	case p.MaxWaiting < 0 || p.MaxWaiting > 1000:
		return fmt.Errorf("policy: step %q: prefix_aware.max_waiting must be between 0 and 1000, got %d", name, p.MaxWaiting)
	case p.BusyFactor != 0 && (p.BusyFactor < 1 || p.BusyFactor > 10):
		return fmt.Errorf("policy: step %q: prefix_aware.busy_factor must be between 1 and 10, got %g", name, p.BusyFactor)
	case p.TieEpsilon < 0 || p.TieEpsilon > 1:
		return fmt.Errorf("policy: step %q: prefix_aware.tie_epsilon must be between 0 and 1, got %g", name, p.TieEpsilon)
	case p.PrefillRate != 0 && (p.PrefillRate < 100 || p.PrefillRate > 10_000_000):
		return fmt.Errorf("policy: step %q: prefix_aware.prefill_rate must be between 100 and 10000000 tokens/s, got %g", name, p.PrefillRate)
	case p.PredictedTTL != 0 && (p.PredictedTTL < time.Second || p.PredictedTTL > time.Hour):
		return fmt.Errorf("policy: step %q: prefix_aware.predicted_ttl must be between 1s and 1h, got %s", name, p.PredictedTTL)
	case p.PendingTTL != 0 && (p.PendingTTL < 100*time.Millisecond || p.PendingTTL > time.Minute):
		return fmt.Errorf("policy: step %q: prefix_aware.pending_ttl must be between 100ms and 1m, got %s", name, p.PendingTTL)
	}
	return nil
}

// prefixAwareStrategy sends a request to the candidate with the lowest
// estimated time to first token: the prompt tokens that candidate does not
// hold in cache plus the prompt tokens queued on it, at the prefill rate,
// plus the time its running requests take per token. It finds what each
// candidate holds through the router's prefix index (memo and board), and
// never tokenizes the request.
type prefixAwareStrategy struct {
	PrefixAwareParams // resolved
	index             PrefixIndex
	queue             *prefillQueue
	metrics           PrefixMetrics
}

func (e *Executor) newPrefixAware(p *PrefixAwareParams) prefixAwareStrategy {
	return prefixAwareStrategy{PrefixAwareParams: p.resolved(), index: e.prefixIndex, queue: &e.prefill, metrics: e.prefixMetrics}
}

// WantsPrefix implements PrefixStrategy.
func (prefixAwareStrategy) WantsPrefix() bool { return true }

func (s prefixAwareStrategy) Select(candidates []ScoredCandidate, req SelectRequest) *domain.Agent {
	if len(candidates) == 0 {
		panic("policy: Select called with empty candidates slice — this is a bug in the executor")
	}
	pr := req.Prefix
	if pr == nil || len(pr.Anchors) == 0 || s.index == nil {
		s.observe(req.Model, PrefixOutcomeNoAnchor)
		return leastLoadedStrategy{}.Select(candidates, req)
	}

	// Busy filter: never pile onto a full or queued-up agent, whatever it holds.
	total := 0
	for _, c := range candidates {
		total += c.Agent.InFlight()
	}
	mean := float64(total) / float64(len(candidates))
	ok := make([]ScoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		if s.busy(c, mean) {
			continue
		}
		ok = append(ok, c)
	}
	if len(ok) == 0 {
		pr.match, pr.newToks, pr.selected = prefixindex.Match{}, sum(pr.Sizes), true
		s.observe(req.Model, PrefixOutcomeBusy)
		return leastLoadedStrategy{}.Select(candidates, req)
	}

	ids := make([]peer.ID, len(ok))
	for i, c := range ok {
		ids[i] = c.Agent.ID
	}
	m := s.index.Lookup(req.Model, pr.Anchors, ids)
	prompt := max(sum(pr.Sizes), len(m.Blocks)*prefixhash.BlockTokens)
	pendingToks := 0
	if m.Pending != "" {
		pendingToks = sum(pr.Sizes[:m.PendingTo])
	}

	hit := func(id peer.ID) int {
		h := m.Depth[id] * prefixhash.BlockTokens
		if id == m.Pending {
			h = max(h, pendingToks)
		}
		return h
	}
	costs := make([]float64, len(ok))
	best := math.Inf(1)
	for i, c := range ok {
		newToks := max(0, prompt-hit(c.Agent.ID))
		// Prompt tokens waiting on the agent: what this router sent and the
		// agent has not started, or what the agent reports from every
		// router, whichever is larger.
		queued := max(s.queue.get(c.Agent.ID), c.Agent.ReportedPrefill())
		costs[i] = float64(queued+newToks)/s.PrefillRate + decodePressure(c)
		best = min(best, costs[i])
	}
	eps := max(s.TieEpsilon*best, 0.005)
	var chosen *domain.Agent
	var bestScore uint64
	for i, c := range ok {
		if costs[i] > best+eps {
			continue
		}
		// Rendezvous hashing on the first anchor: every router breaks the tie
		// the same way, so new text arriving on two routers lands together.
		if score := hrw(pr.Anchors[0], c.Agent.ID); chosen == nil || score > bestScore {
			chosen, bestScore = c.Agent, score
		}
	}

	h := hit(chosen.ID)
	pr.match, pr.newToks, pr.selected = m, max(0, prompt-h), true
	switch {
	case m.Depth[chosen.ID] > 0:
		s.observe(req.Model, PrefixOutcomeHit)
	case chosen.ID == m.Pending && pendingToks > 0:
		s.observe(req.Model, PrefixOutcomePending)
	default:
		s.observe(req.Model, PrefixOutcomeMiss)
	}
	return chosen
}

// Commit implements Committer: the chosen agent now has (or soon will have)
// the matched blocks, and any new text of the request is on its way there.
func (s prefixAwareStrategy) Commit(req SelectRequest, chosen *domain.Agent) {
	pr := req.Prefix
	if pr == nil || !pr.selected || s.index == nil || len(pr.Anchors) == 0 {
		return
	}
	s.index.Commit(req.Model, chosen.ID, pr.Anchors, pr.match, s.PredictedTTL, s.PendingTTL)
	pr.release() // a retry: the previous agent no longer prefills this request
	pr.reserved, pr.reservedToks, pr.queue = chosen.ID, pr.newToks, s.queue
	s.queue.add(chosen.ID, pr.newToks)
}

func (s prefixAwareStrategy) busy(c ScoredCandidate, mean float64) bool {
	snap := c.Snapshot
	if snap.KVCacheUtilization != nil && *snap.KVCacheUtilization >= s.KVThreshold {
		return true
	}
	if snap.WaitingRequests != nil && *snap.WaitingRequests > float64(s.MaxWaiting) {
		return true
	}
	load := float64(c.Agent.InFlight())
	return load > busyFloor && load > s.BusyFactor*mean
}

// decodePressure estimates the seconds an agent's running requests add to a
// new request's first token, from what its engine reports. An engine that
// reports nothing adds nothing: the router's own load count still holds
// requests that just finished (until the agent's next report), and using it
// would push a conversation away from the agent that holds it; the busy
// filter guards against hot spots instead.
func decodePressure(c ScoredCandidate) float64 {
	if c.Snapshot.RunningRequests == nil {
		return 0
	}
	running := *c.Snapshot.RunningRequests
	itl := defaultITLSeconds
	if c.Snapshot.AvgITLSeconds != nil && *c.Snapshot.AvgITLSeconds > 0 {
		itl = *c.Snapshot.AvgITLSeconds
	}
	return running * itl
}

func hrw(anchor uint64, id peer.ID) uint64 {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], anchor)
	d := xxhash.New()
	d.Write(b[:])             //nolint:errcheck
	d.WriteString(string(id)) //nolint:errcheck
	return d.Sum64()
}

func sum(v []int) int {
	t := 0
	for _, x := range v {
		t += x
	}
	return t
}

func (s prefixAwareStrategy) observe(model, outcome string) {
	if s.metrics != nil {
		s.metrics.PrefixRouted(model, outcome)
	}
}
