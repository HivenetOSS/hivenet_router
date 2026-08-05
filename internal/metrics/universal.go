// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// agentCounterState holds all in-process mutable state for one agent's universal counters.
// Atomic fields are used for the hot-path request counters to avoid lock contention.
// RTT fields (SRTT, RTTVAR) are protected by mu — updated per request but rarely contended
// because each goroutine only updates the state for the agent it is currently forwarding to.
type agentCounterState struct {
	// Hot-path counters (atomic)
	successfulRequests atomic.Int64
	failedRequests     atomic.Int64
	inputTokens        atomic.Int64
	outputTokens       atomic.Int64
	rejectedRequests   atomic.Int64
	disconnections     atomic.Int64 // session disconnections (router-lifetime)

	// Failure counters — incremented on transition events, persisted to diskDB
	agentFailures   atomic.Int64
	backendFailures atomic.Int64

	// Circuit breaker — session-scoped consecutive failure streak, not persisted.
	// Reset to 0 on any RecordSuccess; incremented on every RecordFailure.
	consecutiveFails atomic.Int64

	// Baselines loaded from diskDB on Bootstrap — added to atomic values when flushing
	baseSuccessful      int64
	baseFailed          int64
	baseInputTokens     int64
	baseOutputTokens    int64
	baseDisconn         int64
	baseAgentFailures   int64
	baseBackendFailures int64

	// RFC 6298 RTT state — protected by mu
	mu         sync.Mutex
	srtt       float64
	rttvar     float64
	srttInited bool

	// Timing — set on Bootstrap/RecordReconnect
	onlineSince time.Time

	// Prometheus label cache
	model        string
	engine       string
	organization string
	machine      string
}

// UniversalCounterStore manages per-agent engine-agnostic counters.
// It keeps all mutable state in-process (atomic + mutex) and writes to BadgerDB
// only at flush points (agent disconnect) to keep the hot path lock-free.
type UniversalCounterStore struct {
	states  sync.Map // peer.ID → *agentCounterState
	storage storage.RoutingStorage
	m       *RouterMetrics
	flushWg sync.WaitGroup // tracks the periodic-flush goroutine for clean shutdown
	// flushMu serialises FlushAll against ResetAll so a concurrent flush cannot
	// write a pre-reset (non-zero) history back to diskDB after it was wiped. The
	// hot path (RecordSuccess/RecordFailure) does NOT take this lock.
	flushMu sync.Mutex
}

// NewUniversalCounterStore creates a counter store backed by the given storage and metrics.
func NewUniversalCounterStore(s storage.RoutingStorage, m *RouterMetrics) *UniversalCounterStore {
	return &UniversalCounterStore{storage: s, m: m}
}

// Bootstrap initialises (or re-initialises) the in-process state for peerID.
// It reads universalHistory from diskDB to warm-start SRTT/RTTVAR and set counter
// baselines, so routing decisions are informed immediately after a router restart.
// Must be called from handleAgentRegister before any requests are forwarded.
func (u *UniversalCounterStore) Bootstrap(peerID peer.ID, model, engine, organization, machine string) {
	s := &agentCounterState{
		onlineSince:  time.Now(),
		model:        model,
		engine:       engine,
		organization: organization,
		machine:      machine,
	}

	// Load history from diskDB to warm-start RTT and counter baselines.
	if h, err := u.storage.GetUniversalHistory(peerID); err == nil && h != nil {
		s.baseSuccessful = h.SuccessfulRequests
		s.baseFailed = h.FailedRequests
		s.baseInputTokens = h.InputTokens
		s.baseOutputTokens = h.OutputTokens
		s.baseDisconn = h.TotalDisconnections
		s.baseAgentFailures = h.AgentFailures
		s.baseBackendFailures = h.BackendFailures

		if h.SRTT > 0 {
			s.srtt = h.SRTT
			s.rttvar = h.RTTVAR
			s.srttInited = true
		}
	} else if err != nil {
		log.Warnf("UniversalCounterStore.Bootstrap: diskDB read failed for %s: %v", peerID, err)
	} else {
		log.Debugf("UniversalCounterStore.Bootstrap: no history found for %s (new agent)", peerID)
	}

	// Check if there was a previous session for this peer (reconnect within same router run).
	// If so, carry over accumulated session counters before replacing the state.
	// firstRegistration is true when the router just started (no in-memory state) — this
	// is the only case where we need to seed Prometheus counters from diskDB baselines.
	// On reconnect within the same router run, Prometheus already has the accumulated values.
	var firstRegistration bool
	if prev, loaded := u.states.Load(peerID); loaded {
		old := prev.(*agentCounterState)
		old.mu.Lock()
		// Carry forward RTT if not bootstrapped from disk.
		if !s.srttInited && old.srttInited {
			s.srtt = old.srtt
			s.rttvar = old.rttvar
			s.srttInited = true
		}
		old.mu.Unlock()
		s.disconnections.Store(old.disconnections.Load())
	} else {
		firstRegistration = true
	}

	u.states.Store(peerID, s)

	// Write a fresh punctual entry to memDB.
	_ = u.storage.SetUniversalPunctual(peerID, u.buildPunctual(s))

	// Push restored values to Prometheus so Grafana shows the correct state
	// immediately after a router restart, before the first new request arrives.
	// AgentRegistered() is called right after Bootstrap() in handleAgentRegister
	// and must NOT call Set(0) on these gauges to avoid overwriting the values.
	//
	// Counter seeding (baseSuccessful, baseFailed, etc.) is done only on first
	// registration within this router's process lifetime. On reconnect, the Prometheus
	// counters already hold the right accumulated values — re-adding the baselines
	// would double-count.
	var srtt, rttvar, rate float64
	if s.srttInited {
		srtt = s.srtt
		rttvar = s.rttvar
	}
	total := s.baseSuccessful + s.baseFailed
	if total > 0 {
		rate = float64(s.baseSuccessful) / float64(total)
	}

	var successSeed, failSeed, inputSeed, outputSeed, disconnSeed int64
	if firstRegistration {
		successSeed = s.baseSuccessful
		failSeed = s.baseFailed
		inputSeed = s.baseInputTokens
		outputSeed = s.baseOutputTokens
		disconnSeed = s.baseDisconn
		u.m.SeedAgentFailureCounters(peerID.String(), model, engine, organization, machine, s.baseAgentFailures, s.baseBackendFailures)
	}
	u.m.AgentUniversalUpdated(peerID.String(), model, engine, organization, machine,
		successSeed, failSeed,
		rate, 0,
		inputSeed, outputSeed,
		0, disconnSeed,
		srtt, rttvar,
	)
}

// RecordSuccess is called by the processor on HTTP 200 from an agent.
// rttMs is the router-observed round-trip time for this request.
func (u *UniversalCounterStore) RecordSuccess(agent *domain.Agent, inputTokens, outputTokens int, rttMs float64) {
	s := u.getOrInit(agent)

	s.successfulRequests.Add(1)
	s.inputTokens.Add(int64(inputTokens))
	s.outputTokens.Add(int64(outputTokens))

	s.mu.Lock()
	s.consecutiveFails.Store(0) // reset streak atomically with RTT update
	u.updateRTT(s, rttMs)
	srtt := s.srtt
	rttvar := s.rttvar
	s.mu.Unlock()

	capUtil := capacityUtil(agent)
	successRate := u.successRate(s)
	p := u.buildPunctual(s)
	p.CapacityUtilization = capUtil

	_ = u.storage.SetUniversalPunctual(agent.ID, p)

	u.m.AgentUniversalUpdated(
		agent.ID.String(), s.model, s.engine, s.organization, s.machine,
		1, 0,
		successRate, capUtil,
		int64(inputTokens), int64(outputTokens),
		0, 0,
		srtt, rttvar,
	)
}

// RecordFailure is called by the processor on any non-200 or forwarding error.
// rttMs should be 0 when the request never reached the agent (e.g. marshal error).
func (u *UniversalCounterStore) RecordFailure(agent *domain.Agent, rttMs float64) {
	s := u.getOrInit(agent)

	s.failedRequests.Add(1)

	s.mu.Lock()
	s.consecutiveFails.Add(1) // increment streak atomically with RTT update
	if rttMs > 0 {
		u.updateRTT(s, rttMs)
	}
	now := time.Now()
	srtt := s.srtt
	rttvar := s.rttvar
	s.mu.Unlock()

	capUtil := capacityUtil(agent)
	successRate := u.successRate(s)
	p := u.buildPunctual(s)
	p.CapacityUtilization = capUtil
	p.LastFailureAt = now

	_ = u.storage.SetUniversalPunctual(agent.ID, p)

	u.m.AgentUniversalUpdated(
		agent.ID.String(), s.model, s.engine, s.organization, s.machine,
		0, 1,
		successRate, capUtil,
		0, 0,
		0, 0,
		srtt, rttvar,
	)
}

// RecordAgentFailure is called on the healthy→unhealthy transition for missed heartbeats.
// It increments the in-process counter (persisted on next flush) and fires the Prometheus metric.
func (u *UniversalCounterStore) RecordAgentFailure(peerID peer.ID) {
	raw, ok := u.states.Load(peerID)
	if !ok {
		return
	}
	s := raw.(*agentCounterState)
	s.agentFailures.Add(1)
	u.m.AgentFailure(peerID.String(), s.model, s.engine, s.organization, s.machine)
}

// RecordBackendFailure is called when an agent reports its backend as unhealthy (transition only).
// It increments the in-process counter (persisted on next flush) and fires the Prometheus metric.
func (u *UniversalCounterStore) RecordBackendFailure(peerID peer.ID) {
	raw, ok := u.states.Load(peerID)
	if !ok {
		return
	}
	s := raw.(*agentCounterState)
	s.backendFailures.Add(1)
	u.m.ModelBackendFailure(peerID.String(), s.model, s.engine, s.organization, s.machine)
}

// RecordDisconnect is called by the health monitor when an agent is removed.
// It increments the disconnection counter and flushes accumulated counters to
// universalHistory on diskDB.
func (u *UniversalCounterStore) RecordDisconnect(peerID peer.ID) {
	raw, ok := u.states.Load(peerID)
	if !ok {
		return
	}
	s := raw.(*agentCounterState)
	s.disconnections.Add(1)

	s.mu.Lock()
	srtt := s.srtt
	rttvar := s.rttvar
	s.mu.Unlock()

	h := u.buildHistory(s)
	if err := u.storage.SetUniversalHistory(peerID, h); err != nil {
		log.Warnf("UniversalCounterStore.RecordDisconnect: diskDB flush failed for %s: %v", peerID, err)
	}

	u.m.AgentUniversalUpdated(
		peerID.String(), s.model, s.engine, s.organization, s.machine,
		0, 0,
		u.successRate(s), 0,
		0, 0,
		0, 1,
		srtt, rttvar,
	)
}

// StartPeriodicFlush runs a background goroutine that flushes all in-process
// universalHistory counters to diskDB at the given interval.
// This bounds the data loss window to at most one interval on a router crash.
// The on-disconnect flush in RecordDisconnect remains as an additional safety net
// for clean shutdowns (Option D: periodic + on-disconnect).
// Call WaitFlush() after cancelling ctx to ensure the goroutine has exited before
// closing storage — this prevents "DB closed" panics on graceful shutdown.
// The goroutine exits when ctx is cancelled.
func (u *UniversalCounterStore) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	u.flushWg.Add(1)
	go func() {
		defer u.flushWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				u.FlushAll()
			}
		}
	}()
}

// WaitFlush blocks until the periodic-flush goroutine has exited.
// Call this during shutdown after cancelling the context passed to StartPeriodicFlush
// and before closing storage.
func (u *UniversalCounterStore) WaitFlush() {
	u.flushWg.Wait()
}

// FlushAll iterates all known agents and writes their current history to diskDB.
// It is safe to call concurrently and is exported so the router can trigger a
// final flush during graceful shutdown before closing storage.
func (u *UniversalCounterStore) FlushAll() {
	u.flushMu.Lock()
	defer u.flushMu.Unlock()
	u.states.Range(func(key, value any) bool {
		peerID := key.(peer.ID)
		s := value.(*agentCounterState)
		h := u.buildHistory(s)
		if err := u.storage.SetUniversalHistory(peerID, h); err != nil {
			log.Warnf("UniversalCounterStore.FlushAll: diskDB flush failed for %s: %v", peerID, err)
		}
		return true
	})
}

// ResetAll clears every per-agent lifetime counter — in memory, on disk, and in
// Prometheus — so dashboards reflect behaviour since the reset rather than historical
// totals. Intended for the admin metrics-reset endpoint, typically run right after a
// deploy to observe a change against a clean baseline.
//
// For each currently-known agent it swaps the in-memory state for a fresh zeroed one
// (preserving identity labels and onlineSince). Replacing the pointer is race-free:
// any in-flight reader keeps the old struct, new reads get the zeroed one. It then
// resets the per-agent Prometheus series and wipes the persisted universalHistory so
// a later Bootstrap (on reconnect or restart) loads zero baselines. flushMu is held so
// a concurrent FlushAll cannot re-persist stale totals after the wipe.
//
// Billing/quota counters, agent metadata, and liveness gauges (agent info/health) are
// left untouched.
func (u *UniversalCounterStore) ResetAll() error {
	u.flushMu.Lock()
	defer u.flushMu.Unlock()

	u.states.Range(func(key, value any) bool {
		old := value.(*agentCounterState)
		old.mu.Lock()
		onlineSince := old.onlineSince
		old.mu.Unlock()
		u.states.Store(key, &agentCounterState{
			onlineSince:  onlineSince,
			model:        old.model,
			engine:       old.engine,
			organization: old.organization,
			machine:      old.machine,
		})
		return true
	})

	if u.m != nil {
		u.m.ResetAgentSeries()
	}

	return u.storage.ResetUniversalHistory()
}

// DeleteState removes all in-process state for peerID after the agent has been
// fully unregistered. Should be called after RecordDisconnect.
func (u *UniversalCounterStore) DeleteState(peerID peer.ID) {
	u.states.Delete(peerID)
	_ = u.storage.DeleteUniversalPunctual(peerID)
}

// AgentCounters is a snapshot of all per-agent universal counter state,
// exposed to the routing table API without leaking agentCounterState internals.
// Field names mirror the corresponding Prometheus metric suffixes for consistency.
type AgentCounters struct {
	// Latency — RFC 6298
	SRTTMs     float64
	RTTVARMs   float64
	SRTTInited bool

	// Derived rates
	SuccessRate float64

	// Lifetime counters (base from diskDB + session increments)
	SuccessfulRequests int64
	FailedRequests     int64
	InputTokens        int64
	OutputTokens       int64
	Disconnections     int64
	AgentFailures      int64
	BackendFailures    int64

	// Session counters (reset on router restart)
	RejectedRequests int64
}

// GetAgentStats returns a full counters snapshot for peerID.
// ok is false if the agent is not present in the store (not yet bootstrapped
// or already removed).
func (u *UniversalCounterStore) GetAgentStats(peerID peer.ID) (AgentCounters, bool) {
	raw, ok := u.states.Load(peerID)
	if !ok {
		return AgentCounters{}, false
	}
	s := raw.(*agentCounterState)

	s.mu.Lock()
	srtt := s.srtt
	rttvar := s.rttvar
	inited := s.srttInited
	s.mu.Unlock()

	return AgentCounters{
		SRTTMs:             srtt,
		RTTVARMs:           rttvar,
		SRTTInited:         inited,
		SuccessRate:        u.successRate(s),
		SuccessfulRequests: s.baseSuccessful + s.successfulRequests.Load(),
		FailedRequests:     s.baseFailed + s.failedRequests.Load(),
		InputTokens:        s.baseInputTokens + s.inputTokens.Load(),
		OutputTokens:       s.baseOutputTokens + s.outputTokens.Load(),
		Disconnections:     s.baseDisconn + s.disconnections.Load(),
		AgentFailures:      s.baseAgentFailures + s.agentFailures.Load(),
		BackendFailures:    s.baseBackendFailures + s.backendFailures.Load(),
		RejectedRequests:   s.rejectedRequests.Load(),
	}, true
}

// --- helpers ---

// getOrInit returns the existing state for agent or creates a minimal one if missing.
// The minimal path only happens when a request arrives before Bootstrap completes —
// in normal operation Bootstrap is always called first on registration.
func (u *UniversalCounterStore) getOrInit(agent *domain.Agent) *agentCounterState {
	if raw, ok := u.states.Load(agent.ID); ok {
		return raw.(*agentCounterState)
	}
	s := &agentCounterState{
		onlineSince:  time.Now(),
		model:        agent.Metadata.Model,
		engine:       agent.Metadata.Engine,
		organization: agent.Metadata.Organization,
		machine:      agent.Metadata.Machine,
	}
	actual, _ := u.states.LoadOrStore(agent.ID, s)
	return actual.(*agentCounterState)
}

// updateRTT updates SRTT and RTTVAR using an asymmetric EWMA.
//
// First measurement uses RFC 6298 §2.2 initialisation.
//
// Subsequent measurements use an asymmetric α to address the cold-start
// problem in LLM inference routing:
//
//   - When the new sample is BETTER than SRTT (R' < SRTT), use α=0.5 so
//     the estimate converges quickly toward the true steady-state latency.
//     Without this, a single cold-start request (model loading) would keep
//     SRTT inflated for ~40 subsequent requests and bias routing decisions
//     against the agent for minutes.
//
//   - When the new sample is WORSE than SRTT (R' > SRTT), use α=0.125
//     (RFC 6298 standard) to filter one-off spikes and avoid
//     over-reacting to momentary slowdowns.
//
// RTTVAR always uses β=0.25 (RFC 6298 §2.3) and must be updated before
// SRTT (RFC 6298 §2.3 ordering requirement is preserved).
//
// Must be called with s.mu held.
func (u *UniversalCounterStore) updateRTT(s *agentCounterState, rttMs float64) {
	if !s.srttInited {
		// First measurement: RFC 6298 §2.2 initialisation.
		s.srtt = rttMs
		s.rttvar = rttMs / 2
		s.srttInited = true
		return
	}
	// RTTVAR must be updated before SRTT (RFC 6298 §2.3 ordering requirement).
	s.rttvar = 0.75*s.rttvar + 0.25*math.Abs(s.srtt-rttMs)

	// Asymmetric α: converge fast when improving, filter spikes when worsening.
	if rttMs < s.srtt {
		s.srtt = 0.5*s.srtt + 0.5*rttMs // α=0.5 — fast downward adaptation
	} else {
		s.srtt = 0.875*s.srtt + 0.125*rttMs // α=0.125 — RFC 6298, spike-resistant
	}
}

// successRate computes lifetime SuccessfulRequests/(Successful+Failed), or 0.0 if no
// requests have ever been recorded. It includes diskDB baselines so that the gauge
// stays continuous across reconnects — session counters alone would reset to 0/0
// on every reconnect and cause a misleading step-down in Grafana graphs.
func (u *UniversalCounterStore) successRate(s *agentCounterState) float64 {
	ok := s.baseSuccessful + s.successfulRequests.Load()
	fail := s.baseFailed + s.failedRequests.Load()
	total := ok + fail
	if total == 0 {
		return 0
	}
	return float64(ok) / float64(total)
}

// LiveSnapshot returns the current in-memory success rate and SRTT for peerID.
// Both values come directly from the in-process agentCounterState — no storage read,
// no flush delay. Returns ok=false if the agent has no in-process state (not yet
// bootstrapped or already removed).
//
// This is the correct source for routing decisions: GetUniversalHistory reads diskDB
// which is only flushed every UniversalFlushInterval (default 30s), making it up to
// 30 seconds stale for an actively serving agent.
func (u *UniversalCounterStore) LiveSnapshot(peerID peer.ID) (successRate, srtt float64, consecutiveFails int64, ok bool) {
	raw, loaded := u.states.Load(peerID)
	if !loaded {
		return 0, 0, 0, false
	}
	s := raw.(*agentCounterState)

	// Acquire mu so that SRTT and consecutiveFails are read in a self-consistent way
	// with respect to their own update protocol. consecutiveFails is updated under mu
	// in both RecordSuccess (Store(0)) and RecordFailure (Add(1)), so reading it here
	// under the same lock ensures we see a valid, ordered streak relative to the last
	// completed operation visible to this goroutine.
	//
	// Note: successfulRequests and failedRequests are incremented via atomics outside
	// this lock, so the success-rate and consecutiveFails/srtt snapshot are only
	// approximately aligned: they can differ by up to one in-flight request. That
	// bounded inconsistency is intentional — locking all counter increments would add
	// contention on every request on the hot path, with no practical routing benefit.
	s.mu.Lock()
	consecutiveFails = s.consecutiveFails.Load()
	total := s.baseSuccessful + s.successfulRequests.Load() +
		s.baseFailed + s.failedRequests.Load()
	succeeded := s.baseSuccessful + s.successfulRequests.Load()
	srtt = s.srtt
	s.mu.Unlock()

	if total == 0 {
		// No request history yet — success rate is meaningless.
		// Return ok=false so the evaluator leaves SuccessRate nil and
		// exclude_if gates are skipped for this agent (nil-passes-gates contract).
		return 0, srtt, consecutiveFails, false
	}

	return float64(succeeded) / float64(total), srtt, consecutiveFails, true
}

// capacityUtil computes ActiveRequests/Capacity for the agent.
func capacityUtil(agent *domain.Agent) float64 {
	cap := agent.Metadata.Capacity
	if cap <= 0 {
		return 0
	}
	return float64(agent.GetLoad()) / float64(cap)
}

// buildPunctual snapshots the current in-process state into an AgentUniversalPunctual.
func (u *UniversalCounterStore) buildPunctual(s *agentCounterState) *domain.AgentUniversalPunctual {
	return &domain.AgentUniversalPunctual{
		OnlineSince:        s.onlineSince,
		DisconnectionCount: s.disconnections.Load(),
		RejectedRequests:   s.rejectedRequests.Load(),
	}
}

// buildHistory accumulates in-process counters on top of diskDB baselines.
func (u *UniversalCounterStore) buildHistory(s *agentCounterState) *domain.AgentUniversalHistory {
	ok := s.baseSuccessful + s.successfulRequests.Load()
	fail := s.baseFailed + s.failedRequests.Load()
	total := ok + fail
	var rate float64
	if total > 0 {
		rate = float64(ok) / float64(total)
	}

	s.mu.Lock()
	srtt := s.srtt
	rttvar := s.rttvar
	s.mu.Unlock()

	return &domain.AgentUniversalHistory{
		SuccessfulRequests:  ok,
		FailedRequests:      fail,
		SuccessRate:         rate,
		InputTokens:         s.baseInputTokens + s.inputTokens.Load(),
		OutputTokens:        s.baseOutputTokens + s.outputTokens.Load(),
		TotalDisconnections: s.baseDisconn + s.disconnections.Load(),
		SRTT:                srtt,
		RTTVAR:              rttvar,
		AgentFailures:       s.baseAgentFailures + s.agentFailures.Load(),
		BackendFailures:     s.baseBackendFailures + s.backendFailures.Load(),
	}
}
