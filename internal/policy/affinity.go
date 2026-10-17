// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/pin"
)

// SessionIDHeader is the client session header litellm already forwards to the
// router today (forward_client_headers_to_llm_api) and that pi and the PWA
// gateway already send. It is the second key source for agent affinity: when no
// X-Hivenet-Task-ID is present, a conversation keyed by it stays pinned for as
// long as the client keeps the header stable.
const SessionIDHeader = "x-litellm-session-id"

// Affinity defaults applied when the agent_affinity block leaves a field unset.
const (
	defaultAgentAffinityTTL    = 30 * time.Minute
	defaultAgentAffinityMargin = 0.25
)

// AffinityContext carries the per-request identity agent affinity keys on. It
// is built once per request (see NewAffinityContext) and passed to NewSession;
// a nil context, or a policy step without agent_affinity, disables affinity for
// the request at zero cost — no body parsing, no store lookup.
type AffinityContext struct {
	// KeyID is the caller's API key identity (owner of any pin this request
	// reads or writes, so per-key caps stay per-key).
	KeyID string
	// TaskHeader is the client's explicit conversation identity, in precedence
	// order: X-Hivenet-Task-ID (≤ pin.MaxTaskIDLen bytes; a longer value is
	// ignored on the plain path — the alias path rejects it with 400 upstream),
	// else x-litellm-session-id, else "".
	TaskHeader string
	// Fingerprint lazily yields (endUser, system, firstUserText) from the raw
	// request body. It is called at most once per session and never at all when
	// TaskHeader is set, so header-keyed conversations pay no parse cost. Nil
	// means "no body fingerprint available" (headerless + nil = no affinity).
	Fingerprint func() (endUser, system, firstUserText string)
}

// NewAffinityContext builds the affinity context for one pending request, or
// nil when the request must not be pinned:
//
//   - non-LLM capabilities (embedding, reranker) never pin: a shared content
//     key would strand all of a key's traffic of that kind on one agent;
//   - /v1/messages/count_tokens never pins: it is a stateless tokenizer lookup;
//   - a request with neither a task/session header nor a body to fingerprint
//     has nothing to key on.
//
// Header extraction and the >128-byte rule live here (not in the router) so the
// precedence is unit-tested next to the engine it feeds.
func NewAffinityContext(pending *domain.PendingRequest) *AffinityContext {
	if pending == nil || pending.Request == nil {
		return nil
	}
	capability := pending.Capability
	if capability == "" {
		capability = domain.CapabilityLLM
	}
	if capability != domain.CapabilityLLM || pending.Path == "/v1/messages/count_tokens" {
		return nil
	}
	headers := pending.Request.HttpHeaders
	// Precedence: explicit task id, then litellm's session id. An over-long task
	// id is ignored here (deterministic per client, bounded key) rather than
	// rejected: the alias middleware already 400s it on the alias path.
	task := headers.Get(pin.TaskIDHeader)
	if len(task) > pin.MaxTaskIDLen {
		task = ""
	}
	if task == "" {
		task = headers.Get(SessionIDHeader)
	}
	raw, path := pending.Request.RawBytes, pending.Path
	return &AffinityContext{
		KeyID:      pending.KeyID,
		TaskHeader: task,
		Fingerprint: func() (string, string, string) {
			return domain.AffinityFingerprint(raw, path)
		},
	}
}

// AffinityResult is the affinity decision taken for one request. It feeds the
// X-Hivenet-Agent-Source response header and hivenet_router_affinity_total.
type AffinityResult int

const (
	// AffinityNone — affinity is off for this request/step, was never attempted
	// (no candidate pool), or could not be keyed. No header, no metric.
	AffinityNone AffinityResult = iota
	// AffinityHit — the pinned agent was eligible, within the load margin and
	// served the request. The pin's sliding TTL was refreshed.
	AffinityHit
	// AffinityMiss — no live pin existed; the agent that got the request was
	// pinned (max age restarts).
	AffinityMiss
	// AffinityMargin — a pin existed but its agent was more than load_margin
	// above the strategy's best; best served and became the new pin.
	AffinityMargin
	// AffinityIneligible — a pin existed but its agent could not serve (unhealthy,
	// at capacity, previous failure, or a slot race), so a fallback served the
	// request. The pin is kept in both sub-cases (see affResolve): a stale pin
	// costs one worse routing decision per turn and expires on its own.
	AffinityIneligible
)

// String returns the wire/metric spelling of the result.
func (r AffinityResult) String() string {
	switch r {
	case AffinityHit:
		return "hit"
	case AffinityMiss:
		return "miss"
	case AffinityMargin:
		return "margin"
	case AffinityIneligible:
		return "ineligible"
	default:
		return "none"
	}
}

// affinityConfig is a step's agent_affinity block with defaults applied and
// durations parsed. Built once per session by resolve(); nil block → nil config
// → the step never touches affinity.
type affinityConfig struct {
	ttl           time.Duration // sliding, refreshed on every pin hit
	loadMargin    float64       // pinned may be this much (of capacity) above best
	maxAge        time.Duration // absolute cap on a pin's life; 0 = none
	maxPins       int           // store caps: first-use-wins, see AffinityPins
	maxPinsPerKey int
}

// affinityConfigOf normalises a step's block. An absent block disables the
// feature; an unparseable duration also disables it (defensive — the loader
// rejects such policies at load time, only code-built policies can reach here).
func affinityConfigOf(a *AgentAffinity) *affinityConfig {
	if a == nil {
		return nil
	}
	cfg := &affinityConfig{
		ttl:           defaultAgentAffinityTTL,
		loadMargin:    defaultAgentAffinityMargin,
		maxPins:       pin.DefaultPinMax,
		maxPinsPerKey: pin.DefaultPinMaxPerKey,
	}
	if a.TTL != "" {
		d, err := time.ParseDuration(a.TTL)
		if err != nil || d <= 0 {
			log.Warnf("policy: agent_affinity.ttl %q is not a positive duration — affinity disabled for this step", a.TTL)
			return nil
		}
		cfg.ttl = d
	}
	if a.LoadMargin != nil {
		cfg.loadMargin = *a.LoadMargin
	}
	if a.MaxAge != "" {
		d, err := time.ParseDuration(a.MaxAge)
		if err != nil || d < 0 {
			log.Warnf("policy: agent_affinity.max_age %q is not a valid duration — affinity disabled for this step", a.MaxAge)
			return nil
		}
		cfg.maxAge = d // 0 means no absolute cap
	}
	if a.MaxPins != nil {
		cfg.maxPins = *a.MaxPins
	}
	if a.MaxPinsPerKey != nil {
		cfg.maxPinsPerKey = *a.MaxPinsPerKey
	}
	return cfg
}

// AffinityPins exposes the executor's agent-affinity pin store (tests and
// diagnostics). nil means no agent_affinity step has run yet: the store is
// created lazily by affinityStore so policies without the flag allocate
// nothing. Store caps are first-use-wins — whichever affinity step runs first
// sizes the store for the process lifetime.
func (e *Executor) AffinityPins() *pin.PinStore {
	e.affMu.Lock()
	defer e.affMu.Unlock()
	return e.agentPins
}

// affinityStore returns the lazily-created agent pin store, sizing it from the
// first config that asks for it.
func (e *Executor) affinityStore(cfg *affinityConfig) *pin.PinStore {
	e.affMu.Lock()
	defer e.affMu.Unlock()
	if e.agentPins == nil {
		e.agentPins = pin.NewPinStore(cfg.maxPins, cfg.maxPinsPerKey)
	}
	return e.agentPins
}

// affKeyOrCompute returns this session's pin key, computing it once. The key is
// pin.TaskKey over (model, keyID, task/session header) when a header is
// present, else over the (end user, system, first human turn) fingerprint —
// the header wins, so a churning system prompt (pi re-stamps
// "Snapshot session_start …" every turn) cannot break the pin.
func (s *Session) affKeyOrCompute() string {
	if s.affKey != "" {
		return s.affKey
	}
	if s.affCtx == nil {
		return ""
	}
	in := pin.TaskKeyInput{Alias: s.model, KeyID: s.affCtx.KeyID, HeaderValue: s.affCtx.TaskHeader}
	if in.HeaderValue == "" {
		if s.affCtx.Fingerprint == nil {
			return "" // nothing identifies this conversation
		}
		in.EndUser, in.System, in.FirstUserText = s.affCtx.Fingerprint()
	}
	s.affKey = pin.TaskKey(in)
	return s.affKey
}

// affResolve runs the one-shot affinity attempt for one policy step. It returns
// the agent to serve when the pin won an acquired slot (hit), else nil plus the
// decision to record. best is the strategy's own pick — the margin test and the
// fallback both need it.
//
//	miss       — no live pin (expired pins were removed by Get): whoever gets
//	             this request is pinned (affPendingPin → pinIfPending).
//	ineligible — pin hit but its agent is not in the candidate pool (unhealthy,
//	             at capacity, previous failure, a gate): KEEP the pin and serve
//	             the fallback — the mirror of semantic's keepsEntry.
//	margin     — pinned agent is more than load_margin above best: re-pin the
//	             agent that actually serves.
//	hit        — pinned agent is eligible within the margin: acquire its slot and
//	             serve it. A slot race falls through to best WITHOUT dropping the
//	             pin: pendingPin moves the pin to the agent that actually serves
//	             (its copy of the prefix is the one that will be warm next turn).
func (s *Session) affResolve(cfg *affinityConfig, candidates []ScoredCandidate, best *domain.Agent) (*domain.Agent, AffinityResult) {
	key := s.affKeyOrCompute()
	if key == "" {
		return nil, AffinityNone
	}
	store := s.executor.affinityStore(cfg)
	_, pinnedID, ok := store.Get(key) // route slot holds the agent id; refreshes the sliding TTL on a hit
	if !ok {
		s.affPendingPin = true
		return nil, AffinityMiss
	}
	var pinned *domain.Agent
	for i := range candidates {
		if candidates[i].Agent.ID.String() == pinnedID {
			pinned = candidates[i].Agent
			break
		}
	}
	if pinned == nil {
		return nil, AffinityIneligible // transient miss: keep the pin, serve best
	}
	if loadRatio(pinned) > loadRatio(best)+cfg.loadMargin {
		s.affPendingPin = true
		return nil, AffinityMargin
	}
	if pinned.TryAcquireSlot() {
		return pinned, AffinityHit
	}
	// Slot race: serve best, keep a pin (on whoever serves), record transiently.
	s.affPendingPin = true
	return nil, AffinityIneligible
}

// pinIfPending stores the pin for the agent that actually won a slot. Called
// exactly once per Select return path (and only after an acquisition), so a
// request that exhausts pins nothing and a request that re-selects after a
// forward failure does not re-pin mid-request: affPendingPin is consumed on the
// first successful acquisition.
func (s *Session) pinIfPending(a *domain.Agent) {
	if !s.affPendingPin || s.affCfg == nil || s.affCtx == nil {
		return
	}
	s.affPendingPin = false
	key := s.affKeyOrCompute()
	if key == "" {
		return
	}
	// Max age restarts here (PinStore.Put), not on hits — a long-lived
	// conversation that keeps re-pinning lives until it goes idle.
	s.executor.affinityStore(s.affCfg).Put(key, s.affCtx.KeyID, s.model, a.ID.String(), s.affCfg.ttl, s.affCfg.maxAge)
}

// AffinityResult reports the affinity decision taken by the most recent Select.
// AffinityNone means affinity is off for this request, was never attempted (no
// candidate pool was ever built), or could not be keyed — callers emit no
// header and record no metric for it.
func (s *Session) AffinityResult() AffinityResult { return s.affResult }
