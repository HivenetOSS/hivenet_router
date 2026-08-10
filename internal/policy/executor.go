// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hivenet_router/internal/domain"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// QueueMetrics is an optional observer the Executor calls when the per-model
// wait queue depth changes or a wait duration is recorded.
// Implemented by *metrics.RouterMetrics; may be nil (e.g. in unit tests).
type QueueMetrics interface {
	QueueDepthUpdated(model string, depth int)
	QueueWaitObserved(model string, durationSeconds float64)
}

var log = logging.Logger("policy")

// ErrAllStepsExhausted is returned when the primary policy step and every fallback
// step have been tried and none produced an eligible agent.
var ErrAllStepsExhausted = errors.New("all policy steps exhausted: no eligible agents")

// ErrModelNotFound is returned when no agents are registered for the requested model.
var ErrModelNotFound = errors.New("no agents registered for model")

// ErrNoAgentsAvailable is returned when agents are registered for the model but all
// are currently offline or unhealthy.
var ErrNoAgentsAvailable = errors.New("agents exist but all are offline or unhealthy")

// ErrNoCapacity is returned when agents are healthy but all slots are full.
var ErrNoCapacity = errors.New("all agents are at maximum capacity")

// AgentLister is the minimal interface the Executor requires from the agent registry.
// Using an interface avoids an import cycle between the policy and router packages.
type AgentLister interface {
	ListByModel(model string) []*domain.Agent
}

// policyState is an immutable snapshot of the global policy, the named policy
// documents, and the model-keyed routing index derived from them.
// All three are stored together under a single atomic.Pointer so that
// NewSession always observes a consistent view — no torn read between a global
// update and a named-policy update during a concurrent SIGHUP or API call.
//
//	named         — keyed by policy document name (filename stem or API URL segment).
//	                Each document declares which models it applies to via its Models field.
//	modelPolicies — derived from named; keyed by model name for O(1) routing lookup.
//	                Rebuilt by buildModelPolicies on every write to named.
type policyState struct {
	global        *Policy
	named         map[string]*Policy // keyed by document name
	modelPolicies map[string]*Policy // keyed by model name (derived)
}

// buildModelPolicies expands each named policy's Models field into a flat
// model→policy routing map. Conflicts are prevented upstream (LoadDirSnapshot
// and SetNamedPolicy both reject conflicting documents before they reach here),
// so the last write simply wins — this is a mechanical expansion, not arbitration.
func buildModelPolicies(named map[string]*Policy) map[string]*Policy {
	result := make(map[string]*Policy, len(named))
	for _, p := range named {
		for _, model := range p.Models {
			result[model] = p
		}
	}
	return result
}

// Executor applies the active routing policy to every incoming request.
//
// Both the global policy and per-model overrides are stored as a single
// atomic.Pointer[policyState] so that NewSession snapshots them in one load.
// All writes (SetPolicy, SetNamedPolicy, …) hold stateMu and use copy-on-write,
// so the read path (NewSession) is fully lock-free.
type Executor struct {
	agents            AgentLister
	evaluator         *Evaluator
	state             atomic.Pointer[policyState]
	stateMu           sync.Mutex // serialises all write-path operations
	globalMaxTries    int
	defaultQueueDepth int

	queueMu sync.RWMutex
	queues  map[string]*WaitQueue // keyed by model name

	queueMetrics QueueMetrics // optional; nil in tests
}

// NewExecutor creates an Executor with an initial policy.
// globalMaxTries is used for any policy step whose MaxTries is 0 (not set).
// defaultQueueDepth caps the number of requests that may wait per model when
// all agents are at capacity; 0 disables the wait queue entirely.
func NewExecutor(agents AgentLister, evaluator *Evaluator, initial *Policy, globalMaxTries int, defaultQueueDepth int) *Executor {
	e := &Executor{
		agents:            agents,
		evaluator:         evaluator,
		globalMaxTries:    globalMaxTries,
		defaultQueueDepth: defaultQueueDepth,
		queues:            make(map[string]*WaitQueue),
	}
	e.state.Store(&policyState{
		global:        initial,
		named:         make(map[string]*Policy),
		modelPolicies: make(map[string]*Policy),
	})
	return e
}

// SetQueueMetrics wires an observer that receives queue depth and wait-time events.
// Must be called before any requests are processed (typically once at startup).
func (e *Executor) SetQueueMetrics(qm QueueMetrics) {
	e.queueMetrics = qm
}

// getOrCreateQueue returns the WaitQueue for model, creating it lazily on first use.
func (e *Executor) getOrCreateQueue(model string) *WaitQueue {
	// Fast path: queue already exists.
	e.queueMu.RLock()
	if q, ok := e.queues[model]; ok {
		e.queueMu.RUnlock()
		return q
	}
	e.queueMu.RUnlock()

	// Slow path: allocate under write lock.
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	if q, ok := e.queues[model]; ok { // double-check
		return q
	}
	q := newWaitQueue(e.defaultQueueDepth)
	// Wire depth-gauge callbacks so the metric updates exactly when the waiter
	// list changes — on enqueue and on every exit path — rather than only on wakeup.
	q.onEnqueue = func() {
		if e.queueMetrics != nil {
			e.queueMetrics.QueueDepthUpdated(model, q.Depth())
		}
	}
	q.onDequeue = func() {
		if e.queueMetrics != nil {
			e.queueMetrics.QueueDepthUpdated(model, q.Depth())
		}
	}
	e.queues[model] = q
	return q
}

// SignalCapacity wakes one goroutine waiting for capacity on model, if any.
// Call this whenever a concurrency slot is freed (agent.DecrementLoad) or
// new capacity arrives (new agent registered for model).
func (e *Executor) SignalCapacity(model string) {
	if e.defaultQueueDepth == 0 {
		return // queue disabled — no lock needed
	}
	e.queueMu.RLock()
	q, ok := e.queues[model]
	e.queueMu.RUnlock()
	if !ok || q.Depth() == 0 {
		return // no queue or no waiters — fast path
	}
	q.Signal()
	// Depth update is handled by the onDequeue callback in Wait() — no call needed here.
}

// SetPolicy atomically replaces the global policy, preserving all named policies.
// In-flight sessions are unaffected — they hold a policyState snapshot from creation.
func (e *Executor) SetPolicy(p *Policy) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	cur := e.state.Load()
	e.state.Store(&policyState{global: p, named: cur.named, modelPolicies: cur.modelPolicies})
}

// GetPolicy returns the currently active global policy.
func (e *Executor) GetPolicy() *Policy {
	return e.state.Load().global
}

// EffectivePolicy returns the policy that governs a given model: the named
// per-model policy when one claims the model, otherwise the global policy.
// The global and per-model maps come from a single atomic load, so the result
// is always internally consistent. Mirrors the resolution NewSession uses for
// routing, exposed for admission gates that need a model's limits up front.
func (e *Executor) EffectivePolicy(model string) *Policy {
	s := e.state.Load()
	if mp, ok := s.modelPolicies[model]; ok {
		return mp
	}
	return s.global
}

// SetNamedPolicy atomically adds or replaces a named policy document and
// rebuilds the model-keyed routing index from all named policies.
// Returns an error if any model in p.Models is already claimed by a different
// document — the caller must resolve the conflict before the policy is stored.
// The global policy is preserved. Uses copy-on-write so concurrent readers are unaffected.
func (e *Executor) SetNamedPolicy(name string, p *Policy) error {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	cur := e.state.Load()
	// Check for ownership conflicts against all OTHER named documents.
	for _, model := range p.Models {
		for otherName, other := range cur.named {
			if otherName == name {
				continue // updating the same document is always allowed
			}
			for _, m := range other.Models {
				if m == model {
					return fmt.Errorf("model %q is already claimed by policy %q", model, otherName)
				}
			}
		}
	}
	newNamed := make(map[string]*Policy, len(cur.named)+1)
	for k, v := range cur.named {
		newNamed[k] = v
	}
	newNamed[name] = p
	e.state.Store(&policyState{global: cur.global, named: newNamed, modelPolicies: buildModelPolicies(newNamed)})
	return nil
}

// DeleteNamedPolicy atomically removes a named policy document and rebuilds
// the model-keyed routing index. Models that were served by this document
// revert to the global policy. If no document with this name exists this is a no-op.
func (e *Executor) DeleteNamedPolicy(name string) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	cur := e.state.Load()
	newNamed := make(map[string]*Policy, len(cur.named))
	for k, v := range cur.named {
		if k != name {
			newNamed[k] = v
		}
	}
	e.state.Store(&policyState{global: cur.global, named: newNamed, modelPolicies: buildModelPolicies(newNamed)})
}

// GetNamedPolicies returns a defensive copy of the current named policy map.
// Keyed by policy document name (filename stem or API URL segment).
// Callers may read the returned map freely; mutations do not affect the live state.
func (e *Executor) GetNamedPolicies() map[string]*Policy {
	m := e.state.Load().named
	result := make(map[string]*Policy, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// GetNamedPolicy returns the named policy document for name, or (nil, false) if absent.
func (e *Executor) GetNamedPolicy(name string) (*Policy, bool) {
	p, ok := e.state.Load().named[name]
	return p, ok
}

// SetNamedPoliciesFromSnapshot atomically replaces the entire named policy map
// and rebuilds the model-keyed routing index. Used by the SIGHUP reload to
// apply a freshly-loaded DirSnapshot in one store. The provided map is copied.
func (e *Executor) SetNamedPoliciesFromSnapshot(named map[string]*Policy) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	cur := e.state.Load()
	newNamed := make(map[string]*Policy, len(named))
	for k, v := range named {
		newNamed[k] = v
	}
	e.state.Store(&policyState{global: cur.global, named: newNamed, modelPolicies: buildModelPolicies(newNamed)})
}

// NewSession creates a per-request routing session. The policy is snapshotted
// atomically so that a concurrent SetPolicy call cannot affect this session.
// If a per-model policy exists for model it takes full precedence over the
// global policy; otherwise the global policy is used.
//
// capability filters agent selection to only agents whose Metadata.Capability
// matches. Pass domain.CapabilityLLM ("llm") for chat completions.
func (e *Executor) NewSession(model, capability string) *Session {
	s := e.state.Load() // single atomic load — global and per-model are always consistent
	pol := s.global
	if mp, ok := s.modelPolicies[model]; ok {
		pol = mp
	}

	// Flatten primary step + fallback chain into a single linear slice.
	steps := make([]resolvedStep, 0, 1+len(pol.FallbackChain))
	steps = append(steps, e.resolve("routing_policy", pol.RoutingPolicy))
	for i, fb := range pol.FallbackChain {
		name := fb.Name
		if name == "" {
			name = fmt.Sprintf("fallback_chain[%d]", i)
		}
		steps = append(steps, e.resolve(name, fb.PolicyStep))
	}

	return &Session{
		model:            model,
		capability:       capability,
		steps:            steps,
		stepFailed:       make(map[peer.ID]struct{}),
		executor:         e,
		fallbackProvider: pol.FallbackProvider,
		lastReason:       ErrAllStepsExhausted,
	}
}

// resolve binds a PolicyStep to its concrete Strategy and computes its effective maxTries.
func (e *Executor) resolve(name string, step PolicyStep) resolvedStep {
	maxTries := step.MaxTries
	if maxTries <= 0 {
		maxTries = e.globalMaxTries
	}
	return resolvedStep{
		name:       name,
		PolicyStep: step,
		strategy:   Get(step.Strategy),
		maxTries:   maxTries,
	}
}

// resolvedStep is a PolicyStep with its Strategy pre-looked up and maxTries resolved.
type resolvedStep struct {
	name       string
	PolicyStep          // embedded: Match, ExcludeIf, Strategy name, MaxTries
	strategy   Strategy // concrete strategy instance
	maxTries   int      // effective max forward attempts for this step
}

// gateDiag records the outcome of one gate in the policy funnel for a single step.
// Gate numbers match the Policy Funnel diagram (docs/online/images/Policy.png).
type gateDiag struct {
	num     int    // 1–7
	name    string // "model_filter" | "health" | "capability" | "match" | "prev_failures" | "capacity" | "exclude_if"
	poolIn  int    // agents entering this gate; –1 for gate 1 (no preceding gate)
	poolOut int    // agents surviving; 0 means pool drained here (FAIL)
	reason  string // non-empty whenever poolOut < poolIn
}

// stepExhaustDiag holds the per-gate funnel trace for one exhausted policy step.
type stepExhaustDiag struct {
	stepNum int // 1-based position in the chain
	name    string
	trigger string     // "no_candidates" or "max_tries"
	gates   []gateDiag // populated only for "no_candidates"
}

// Session is a stateful per-request cursor over the policy steps.
// It is NOT safe for concurrent use — each request must have its own Session.
type Session struct {
	model            string
	capability       string // "llm", "embedding", or "reranker" — filters agent selection
	steps            []resolvedStep
	stepIdx          int
	stepTries        int
	stepFailed       map[peer.ID]struct{} // agents that failed a forward in the current step
	executor         *Executor
	fallbackProvider *FallbackProvider // snapshotted from policy at session creation
	// lastReason records the most recent explanation for why buildCandidates returned
	// zero candidates. It is set at routing time (not post-hoc) so the error code
	// reflects the actual state seen during selection — avoiding the stale-snapshot
	// problem that diagnoseEmptyPool() has when agent health changes between selection
	// and exhaustion.
	lastReason error
	// exhaustDiags accumulates one entry per step that advanced without selecting an agent.
	// Emitted as a batch only when the full chain is exhausted — never on successful routing.
	exhaustDiags []stepExhaustDiag
	// lastGates holds the gate funnel from the most recent zero-candidate buildCandidates call.
	// Consumed by advanceStep("no_candidates") to attach the funnel to the step trace.
	lastGates []gateDiag
}

// Select picks the next eligible agent, advancing through fallback steps as needed.
// Returns ErrAllStepsExhausted when the entire chain is consumed.
//
// ctx governs how long the session waits when all agents are at capacity
// (ErrNoCapacity). The goroutine parks in a per-model wait queue and resumes
// when a slot is freed (DecrementLoad) or new capacity arrives (agent registration).
// When ctx expires the session escalates to the next fallback step as normal.
//
// A TryAcquireSlot race (slot filled between scoring and acquisition) does NOT
// count as a forward failure — selection retries immediately within the same step.
func (s *Session) Select(ctx context.Context) (*domain.Agent, error) {
	for s.stepIdx < len(s.steps) {
		step := &s.steps[s.stepIdx]

		candidates := s.buildCandidates(step)
		if len(candidates) == 0 {
			// When all healthy agents are momentarily at capacity, park in the
			// per-model wait queue instead of immediately escalating to a fallback
			// step. The goroutine wakes when a slot is freed or new capacity joins.
			if s.lastReason == ErrNoCapacity && s.executor.defaultQueueDepth > 0 {
				queue := s.executor.getOrCreateQueue(s.model)
				_, queueSpan := otel.Tracer("hivenet-router").Start(ctx, "queue_wait",
					trace.WithAttributes(attribute.String("model", s.model)),
				)
				start := time.Now()
				waitErr := queue.Wait(ctx)
				elapsed := time.Since(start)
				// Depth gauge is updated by onDequeue callback in Wait() — no call needed here.
				// Only record wait duration on successful dispatch (waitErr == nil): the histogram
				// measures how long requests waited before being forwarded, not timeout durations.
				// Recording ctx-expired waits would skew P95/P99 toward RequestTimeout and make
				// the metric useless for observing actual dispatch latency.
				if waitErr == nil {
					if s.executor.queueMetrics != nil {
						s.executor.queueMetrics.QueueWaitObserved(s.model, elapsed.Seconds())
					}
					queueSpan.SetAttributes(attribute.Float64("queue.wait_ms", float64(elapsed.Milliseconds())))
				} else {
					queueSpan.SetStatus(codes.Error, waitErr.Error())
				}
				queueSpan.End()
				if waitErr == nil {
					// A slot may be available — retry selection within the same step.
					continue
				}
				// ErrQueueFull or ctx expired — escalate to the fallback chain.
				log.Debugf("policy: step %q — capacity wait ended (%v), escalating for model %q",
					step.name, waitErr, s.model)
			}
			s.advanceStep("no_candidates")
			continue
		}

		chosen := step.strategy.Select(candidates)

		// Atomically claim a slot. If lost to a race, retry selection without counting
		// a forward try — the agent is not "failed", just momentarily at capacity.
		//
		// This is a transient retry within the same step — the agent is momentarily at
		// capacity. A dedicated slot-contention metric can be added as future work.
		if !chosen.TryAcquireSlot() {
			continue
		}

		return chosen, nil
	}
	// Use the reason captured at routing time rather than re-querying agent state,
	// which may have changed (e.g. agents going offline between selection and exhaustion).
	return nil, s.lastReason
}

// RecordFailure marks agentID as failed in the current step and increments the
// try counter. When the budget is exhausted the next Select call advances the step.
// Returns true if the try budget was exhausted and the session advanced to the next step.
func (s *Session) RecordFailure(id peer.ID) bool {
	s.stepFailed[id] = struct{}{}
	s.stepTries++
	if s.stepTries >= s.steps[s.stepIdx].maxTries {
		s.advanceStep("max_tries")
		return true
	}
	return false
}

// advanceStep moves to the next fallback step and resets all per-step state.
// trigger is "no_candidates" or "max_tries".
func (s *Session) advanceStep(trigger string) {
	fromName := s.steps[s.stepIdx].name
	fromIdx := s.stepIdx
	s.stepIdx++
	s.stepTries = 0
	s.stepFailed = make(map[peer.ID]struct{})

	switch trigger {
	case "no_candidates":
		s.exhaustDiags = append(s.exhaustDiags, stepExhaustDiag{
			stepNum: fromIdx + 1,
			name:    fromName,
			trigger: "no_candidates",
			gates:   s.lastGates,
		})
		s.lastGates = nil
		to := "(none remaining)"
		if s.stepIdx < len(s.steps) {
			to = s.steps[s.stepIdx].name
		}
		log.Debugf("policy: step %q → advancing to %q", fromName, to)
	case "max_tries":
		s.exhaustDiags = append(s.exhaustDiags, stepExhaustDiag{
			stepNum: fromIdx + 1,
			name:    fromName,
			trigger: "max_tries",
		})
	}
}

// EmitExhaustionLogs writes a single WARN line summarising why the full policy
// chain was exhausted. Only called when all steps are consumed — never on
// successful routing. Every line carries req/model/capability/tenant so a
// single grep on the request ID returns the complete diagnosis.
func (s *Session) EmitExhaustionLogs(requestID, tenantID string) {
	totalSteps := len(s.steps)
	reqCtx := fmt.Sprintf("req=%s model=%q capability=%q tenant=%s", requestID, s.model, s.capability, tenantID)

	chain := make([]string, len(s.steps))
	for i, step := range s.steps {
		chain[i] = step.name
	}
	log.Warnf("%s  policy_exhausted  chain=[%s]  tried=%d/%d  reason=%q",
		reqCtx, strings.Join(chain, "→"), len(s.exhaustDiags), totalSteps, exhaustionReason(s.exhaustDiags))
}

// exhaustionReason builds a human-readable reason string from the accumulated
// step diagnostics. For each step it names the exact gate that drained the pool
// and the specific exclusion detail, so the summary line is self-contained.
//
//	"routing_policy: gate[4] match [engine(got="",want="vllm")=2]"
//	"routing_policy: gate[4] match [engine(got="",want="vllm")=2] | relaxed: gate[7] exclude_if [kv_cache_utilization=2]"
func exhaustionReason(diags []stepExhaustDiag) string {
	parts := make([]string, 0, len(diags))
	for _, sd := range diags {
		if sd.trigger == "max_tries" {
			parts = append(parts, sd.name+": max_tries exhausted")
			continue
		}
		if len(sd.gates) == 0 {
			parts = append(parts, sd.name+": no candidates")
			continue
		}
		// The last gate in the slice is always the FAIL gate — we stop
		// appending once a gate drains the pool to zero.
		g := sd.gates[len(sd.gates)-1]
		if g.reason != "" {
			parts = append(parts, fmt.Sprintf("%s: gate[%d] %s [%s]", sd.name, g.num, g.name, g.reason))
		} else {
			parts = append(parts, fmt.Sprintf("%s: gate[%d] %s", sd.name, g.num, g.name))
		}
	}
	return strings.Join(parts, " | ")
}

// StepName returns the name of the current policy step, or "" when all steps are exhausted.
func (s *Session) StepName() string {
	if s.stepIdx < len(s.steps) {
		return s.steps[s.stepIdx].name
	}
	return ""
}

// StepStrategy returns the strategy name of the current policy step.
func (s *Session) StepStrategy() string {
	if s.stepIdx < len(s.steps) {
		return s.steps[s.stepIdx].Strategy
	}
	return ""
}

// StepTries returns the number of forward failures recorded so far in the current step.
func (s *Session) StepTries() int { return s.stepTries }

// StepMaxTries returns the effective try budget for the current step.
func (s *Session) StepMaxTries() int {
	if s.stepIdx < len(s.steps) {
		return s.steps[s.stepIdx].maxTries
	}
	return 0
}

// FallbackProvider returns the policy's last-resort provider configuration,
// or nil if none was declared. It is snapshotted at session creation time.
func (s *Session) FallbackProvider() *FallbackProvider {
	return s.fallbackProvider
}

// buildCandidates applies the three filter layers for the given step and
// returns the surviving agents paired with their live snapshots.
// When the result is empty, logs a per-attribute breakdown so the exact reason
// is visible: which filter attribute caused misses, which gate was violated.
func (s *Session) buildCandidates(step *resolvedStep) []ScoredCandidate {
	all := s.executor.agents.ListByModel(s.model)
	candidates := make([]ScoredCandidate, 0, len(all))

	if len(all) == 0 {
		s.lastReason = ErrModelNotFound
		s.lastGates = []gateDiag{{num: 1, name: "model_filter", poolIn: -1, poolOut: 0, reason: "model_not_registered"}}
		return candidates
	}

	var nUnhealthy, nAtCapacity, nPrevFailed int
	filterMiss := make(map[string]int) // e.g. "region" -> 2
	gateMiss := make(map[string]int)   // e.g. "kv_cache_utilization" -> 1

	for _, agent := range all {
		// Hard constraints — always enforced regardless of policy configuration.
		if !agent.IsHealthy() || !agent.IsBackendHealthy() {
			nUnhealthy++
			continue
		}
		// Capability filter: only route to agents that serve the requested type.
		if s.capability != "" && agent.Metadata.Capability != s.capability {
			filterMiss["capability"]++
			continue
		}
		// Layer 1: static match filter.
		if !MatchesFilter(agent, step.Match) {
			filterMiss[filterMissReason(agent, step.Match)]++
			continue
		}
		// Exclude agents that already failed a forward in this step.
		if _, failed := s.stepFailed[agent.ID]; failed {
			nPrevFailed++
			continue
		}
		// Capacity hard gate — agent is full.
		if agent.GetLoad() >= agent.Metadata.Capacity {
			nAtCapacity++
			continue
		}
		// Layer 2: dynamic metric gates.
		snap := s.executor.evaluator.Snapshot(agent.ID, agent)
		if !PassesGates(snap, step.ExcludeIf) {
			gateMiss[gateFailReason(snap, step.ExcludeIf)]++
			continue
		}
		candidates = append(candidates, ScoredCandidate{Agent: agent, Snapshot: snap})
	}

	if len(candidates) == 0 {
		// Capture the reason at routing time so Select() returns an accurate error
		// without re-querying potentially stale agent state after exhaustion.
		switch {
		case nUnhealthy == len(all):
			s.lastReason = ErrNoAgentsAvailable
		case len(filterMiss) == 0 && nAtCapacity > 0 && nUnhealthy+nAtCapacity+nPrevFailed == len(all):
			// All healthy, filter-passing agents are either at capacity or were already tried
			// and failed in this step. Waiting for a slot to free is worthwhile.
			s.lastReason = ErrNoCapacity
			// default: mixed reasons (filter exclusions, gate misses) —
			// leave s.lastReason unchanged so a more specific reason from a prior step survives.
		}

		// Build the per-gate funnel snapshot. Stored in s.lastGates and consumed by
		// advanceStep("no_candidates"); emitted only if the full chain is exhausted.
		capMisses := filterMiss["capability"]

		matchMisses := 0
		var matchParts []string
		for k, n := range filterMiss {
			if k != "capability" {
				matchMisses += n
				matchParts = append(matchParts, fmt.Sprintf("%s=%d", k, n))
			}
		}
		sort.Strings(matchParts)

		var gateParts []string
		for k, n := range gateMiss {
			gateParts = append(gateParts, fmt.Sprintf("%s=%d", k, n))
		}
		sort.Strings(gateParts)

		var gates []gateDiag
		rem := len(all)

		// Gate 1: model_filter — always PASS (ListByModel pre-filtered by model name).
		gates = append(gates, gateDiag{num: 1, name: "model_filter", poolIn: -1, poolOut: rem})

		// Gate 2: health
		if rem > 0 {
			out := rem - nUnhealthy
			reason := ""
			if nUnhealthy > 0 {
				reason = fmt.Sprintf("unhealthy=%d", nUnhealthy)
			}
			gates = append(gates, gateDiag{num: 2, name: "health", poolIn: rem, poolOut: out, reason: reason})
			rem = out
		}
		// Gate 3: capability
		if rem > 0 {
			out := rem - capMisses
			reason := ""
			if capMisses > 0 {
				reason = fmt.Sprintf("capability_mismatch=%d", capMisses)
			}
			gates = append(gates, gateDiag{num: 3, name: "capability", poolIn: rem, poolOut: out, reason: reason})
			rem = out
		}
		// Gate 4: match (Layer 1) — region, engine, tags, organization, machine, gpu_model
		if rem > 0 {
			out := rem - matchMisses
			gates = append(gates, gateDiag{num: 4, name: "match", poolIn: rem, poolOut: out, reason: strings.Join(matchParts, ", ")})
			rem = out
		}
		// Gate 5: prev_failures — agents that already failed a forward in this step
		if rem > 0 {
			out := rem - nPrevFailed
			reason := ""
			if nPrevFailed > 0 {
				reason = fmt.Sprintf("prev_failed=%d", nPrevFailed)
			}
			gates = append(gates, gateDiag{num: 5, name: "prev_failures", poolIn: rem, poolOut: out, reason: reason})
			rem = out
		}
		// Gate 6: capacity — active_requests < capacity
		if rem > 0 {
			out := rem - nAtCapacity
			reason := ""
			if nAtCapacity > 0 {
				reason = fmt.Sprintf("at_capacity=%d", nAtCapacity)
			}
			gates = append(gates, gateDiag{num: 6, name: "capacity", poolIn: rem, poolOut: out, reason: reason})
			rem = out
		}
		// Gate 7: exclude_if (Layer 2) — dynamic metric thresholds
		if rem > 0 {
			gates = append(gates, gateDiag{num: 7, name: "exclude_if", poolIn: rem, poolOut: 0, reason: strings.Join(gateParts, ", ")})
		}

		s.lastGates = gates
	}
	return candidates
}
