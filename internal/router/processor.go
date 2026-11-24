// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/prefixhash"
	"hivenet_router/internal/provider"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	p2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// RequestProcessor is responsible for processing requests that arrive in the queue.
// It consumes PendingRequest objects and routes them to available agents using
// libp2p HTTP (NamespacedClient) for request forwarding.
//
// Selection follows the active routing policy via policy.Executor: the three-layer
// match → exclude_if → strategy pipeline is applied per request, with automatic
// fallback chain advancement on forward failures.
//
// When all policy steps are exhausted and the policy declares a fallback_provider,
// the request is forwarded to the configured closed-source provider via HTTPS.

// PeerCloser drops all libp2p connections to a peer so the next outbound request
// re-dials a fresh one. Implemented by network.Network (host.Network()); a fake is
// injected in tests via WithPeerCloser.
type PeerCloser interface {
	ClosePeer(peer.ID) error
}

// noopPeerCloser is the default PeerCloser used when no libp2p host is provided and
// no closer is injected (tests / misconfiguration). ClosePeer is a no-op so the
// eviction path can never panic on a nil closer. Production always uses the real
// host's Network() via httpHost.
type noopPeerCloser struct{}

func (noopPeerCloser) ClosePeer(peer.ID) error { return nil }

// ForwardFunc performs a single forward attempt to an agent and returns the response,
// the measured RTT in milliseconds, and an error. Defaults to forwardToAgent; tests
// override it via WithForwardFunc to drive dispatch without real networking.
type ForwardFunc func(ctx context.Context, agent *domain.Agent, pending *domain.PendingRequest) (*domain.ChatResponse, float64, error)

// ProcessorOption customises a RequestProcessor at construction. Used to inject test
// doubles for the connection closer and the forward function.
type ProcessorOption func(*RequestProcessor)

// defaultEvictCooldown throttles per-agent connection eviction so a burst of
// concurrent forward failures cannot repeatedly tear down a freshly re-dialed
// connection (the in-flight failures were riding the old, now-closed one).
const defaultEvictCooldown = 2 * time.Second

// maxReconnectsPerAgent bounds the free, budget-exempt re-dial granted on a
// connection-level failure. After this many within a single request, further
// failures fall through to normal policy escalation, so a genuinely unreachable
// agent still exhausts its tries instead of looping.
const maxReconnectsPerAgent = 1

type RequestProcessor struct {
	requestQueue chan *domain.PendingRequest
	executor     *policy.Executor
	httpHost     *p2phttp.Host // libp2p HTTP host used to make outbound requests to agents
	metrics      *metrics.RouterMetrics
	counters     *metrics.UniversalCounterStore
	sem          chan struct{} // semaphore that caps concurrent in-flight forwards
	// providers maps provider engine name ("openai", "anthropic") to its implementation.
	// Populated at startup from config; only configured providers are present (missing key means not configured).
	providers map[string]provider.Provider

	// providerBaseURLs overrides provider endpoints by name (see
	// WithProviderBaseURLs); nil = the providers' public APIs.
	providerBaseURLs map[string]string
	// limiter enforces the per-key daily token budget on the output side.
	// AllowOutputTokens is called after the agent returns the completion token count.
	limiter auth.RateLimiter

	// salter, when set, salts each forwarded body per tenant (WithCacheSalter).
	salter *cachesalt.Salter
	// peers drops dead libp2p connections to an agent on a connection-level forward
	// failure so the next attempt re-dials. Defaults to httpHost.StreamHost.Network().
	peers PeerCloser
	// evictCooldown / evictMu / lastEvict implement per-agent eviction throttling.
	evictCooldown time.Duration
	evictMu       sync.Mutex
	lastEvict     map[peer.ID]time.Time
	// forwardFn performs the actual forward; defaults to forwardToAgent.
	forwardFn ForwardFunc

	// prefixKeys key the prefix anchors (prefix-aware routing); prefixScope
	// returns the scope of a tenant's anchors (nil: one scope for everyone).
	prefixKeys  prefixhash.Keys
	prefixScope func(tenantID string) string
	// prefixCheck receives, after an answer, the blocks the agent's engine
	// confirmed holding at the routing decision and the cached prompt tokens
	// the engine reported.
	prefixCheck func(agent peer.ID, model string, blocks []uint64, at int64, actual int)
}

// NewRequestProcessor creates a new request processor.
// maxConcurrent limits how many agent-forward goroutines may run at once.
// providers may be nil or empty when no closed-source providers are configured.
// limiter may be nil when auth is disabled.
func NewRequestProcessor(
	requestQueue chan *domain.PendingRequest,
	executor *policy.Executor,
	httpHost *p2phttp.Host,
	m *metrics.RouterMetrics,
	counters *metrics.UniversalCounterStore,
	maxConcurrent int,
	providers map[string]provider.Provider,
	limiter auth.RateLimiter,
	opts ...ProcessorOption,
) *RequestProcessor {
	p := &RequestProcessor{
		requestQueue:  requestQueue,
		executor:      executor,
		httpHost:      httpHost,
		metrics:       m,
		counters:      counters,
		sem:           make(chan struct{}, maxConcurrent),
		providers:     providers,
		limiter:       limiter,
		evictCooldown: defaultEvictCooldown,
		lastEvict:     make(map[peer.ID]time.Time),
	}
	// Default the connection closer to the live libp2p host. httpHost may be nil in
	// unit tests, which inject a fake via WithPeerCloser instead.
	if httpHost != nil {
		p.peers = httpHost.StreamHost.Network()
	}
	p.forwardFn = p.forwardToAgent
	for _, opt := range opts {
		opt(p)
	}
	// Guarantee a non-nil closer: with a nil httpHost and no WithPeerCloser override
	// (tests / misconfiguration), fall back to a no-op so eviction never panics.
	if p.peers == nil {
		p.peers = noopPeerCloser{}
	}
	return p
}

// WithPeerCloser overrides the component used to drop dead agent connections.
// Intended for tests.
func WithPeerCloser(pc PeerCloser) ProcessorOption {
	return func(p *RequestProcessor) { p.peers = pc }
}

// WithCacheSalter makes every forward attempt salt the body for the tenant
// (see cachesalt.Salter.Apply). nil (the default) forwards bodies unchanged.
func WithCacheSalter(s *cachesalt.Salter) ProcessorOption {
	return func(p *RequestProcessor) { p.salter = s }
}

// WithPrefixKeys sets the keys of the prefix anchors computed for requests
// whose policy routes on prefixes. Without them no anchor is computed.
func WithPrefixKeys(k prefixhash.Keys) ProcessorOption {
	return func(p *RequestProcessor) { p.prefixKeys = k }
}

// WithPrefixScope sets the scope of a tenant's prefix anchors. The anchors
// must follow the engine's prefix cache: when the cache is separated per
// tenant (cache salt), scope returns the tenant so tenants never share an
// anchor; when the cache is shared, leave it unset.
func WithPrefixScope(scope func(tenantID string) string) ProcessorOption {
	return func(p *RequestProcessor) { p.prefixScope = scope }
}

// WithPrefixCheck sets the function that checks what an agent's engine
// confirmed holding (blocks, as of at) against the cached tokens it reported
// (agent trust).
func WithPrefixCheck(f func(agent peer.ID, model string, blocks []uint64, at int64, actual int)) ProcessorOption {
	return func(p *RequestProcessor) { p.prefixCheck = f }
}

// checkPrefix reports a prediction check when the engine reported its cached
// tokens and a prediction was made.
func (p *RequestProcessor) checkPrefix(agent *domain.Agent, pending *domain.PendingRequest, actual int, known bool) {
	if p.prefixCheck == nil || !known || len(pending.PredictedBlocks) == 0 {
		return
	}
	p.prefixCheck(agent.ID, agent.Metadata.Model, pending.PredictedBlocks, pending.PredictedAt, actual)
}

// countTokensPath is the Anthropic token-counting endpoint: it holds no KV
// cache, so it gets no prefix anchors.
const countTokensPath = "/v1/messages/count_tokens"

// setPrefix computes the request's prefix anchors when its policy routes on
// them, hands them to the session and keeps them for the agent.
func (p *RequestProcessor) setPrefix(session *policy.Session, pending *domain.PendingRequest, capability string) {
	if !p.prefixKeys.Valid() || capability != domain.CapabilityLLM || pending.Path == countTokensPath || !session.WantsPrefix() {
		return
	}
	scope := ""
	if p.prefixScope != nil {
		scope = p.prefixScope(pending.TenantID)
	}
	anchors, sizes := p.prefixKeys.Anchors(pending.Request.Model, scope, pending.Request)
	if len(anchors) == 0 {
		return
	}
	session.SetPrefix(&policy.PrefixRequest{Anchors: anchors, Sizes: sizes})
	pending.PrefixAnchors = prefixhash.EncodeAnchors(anchors)
}

// WithForwardFunc overrides the per-attempt forward function. Intended for tests.
func WithForwardFunc(fn ForwardFunc) ProcessorOption {
	return func(p *RequestProcessor) { p.forwardFn = fn }
}

// shouldEvict reports whether a stale-connection eviction for id is allowed right
// now, rate-limited to once per evictCooldown per agent. This dedupes the burst of
// concurrent forward failures that occur when one connection dies, and protects the
// connection re-dialed by the first failure from being closed by the rest.
func (p *RequestProcessor) shouldEvict(id peer.ID) bool {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()
	now := time.Now()
	if last, ok := p.lastEvict[id]; ok && now.Sub(last) < p.evictCooldown {
		return false
	}
	p.lastEvict[id] = now
	return true
}

// isConnectionLevelError reports whether err means the libp2p path to the agent is
// broken (a stale/dead connection), as opposed to an application-level response from
// a reachable agent. Only ErrCodeAgentDisconnected qualifies: if the agent produced
// any HTTP response (even an error), the connection is alive. Deadlines are excluded
// so a slow-but-healthy agent is never mistaken for a dead connection.
func isConnectionLevelError(err error) bool {
	var re *domain.RouterError
	if errors.As(err, &re) {
		return re.Code == domain.ErrCodeAgentDisconnected
	}
	return false
}

// isResourceLimit reports whether a forward failed because a libp2p resource
// limit (streams or memory) was reached. The connection is healthy, the agent
// is just full: the request goes to another agent and the connection is not
// dropped, which would cut every other stream on it.
func isResourceLimit(err error) bool {
	return errors.Is(err, network.ErrResourceLimitExceeded) ||
		strings.Contains(err.Error(), network.ErrResourceLimitExceeded.Error())
}

// requestDeadlinePassed reports whether the request's own deadline has expired,
// i.e. whether a forward error means "the agent was too slow for this
// request's budget" rather than "the agent's connection is broken". It checks
// the request context, not the error: the transport surfaces a deadline in
// several shapes (context.DeadlineExceeded, the multiplexer's own stream
// timeout error), and libp2phttp opens the stream under a separate, shorter
// timeout whose expiry looks the same. Only an expired request deadline maps
// to a request timeout; anything else stays on the connection-failure path,
// so a stalled stream open still evicts and fails over to another agent.
// Misclassifying a request deadline as agent_disconnected would trigger
// ClosePeer: a GOAWAY that makes a healthy agent drop and re-register,
// truncating every other in-flight stream on its connection.
func requestDeadlinePassed(ctx context.Context) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	// The stream's read deadline is armed from the same instant as the context
	// timer and can fire a moment before ctx.Err() is set.
	d, ok := ctx.Deadline()
	return ok && !time.Now().Before(d)
}

// Start begins processing requests from the queue.
// It returns when ctx is cancelled or the requestQueue channel is closed.
func (p *RequestProcessor) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case pending, ok := <-p.requestQueue:
			if !ok {
				return
			}
			if pending.IsExpired() {
				pending.Error <- domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request expired in queue", domain.SourceRouter)
				continue
			}

			// Spawn the goroutine immediately so the loop is never blocked.
			// The semaphore is acquired inside the goroutine: if all slots are
			// full the goroutine waits, but the loop keeps draining the queue
			// and can still honour ctx.Done() and request expiry.
			go func() {
				p.sem <- struct{}{}
				defer func() { <-p.sem }()
				p.dispatchWithPolicy(pending)
			}()
		}
	}
}

// dispatchWithPolicy runs the policy-aware select→forward→retry loop for one request.
// It creates a Session from the active policy and iterates until a forward succeeds
// or all policy steps (primary + fallback chain) are exhausted.
func (p *RequestProcessor) dispatchWithPolicy(pending *domain.PendingRequest) {
	capability := pending.Capability
	if capability == "" {
		capability = domain.CapabilityLLM
	}
	session := p.executor.NewSessionWithStrategy(pending.Request.Model, pending.RoutingStrategy, capability)
	if pending.RoutingStrategy != "" {
		p.metrics.RoutingStrategy(pending.RoutingStrategy, pending.RoutingStrategySource)
	}
	session.SetScope(pending.Scope)
	session.SetReservation(pending.Pool)
	p.setPrefix(session, pending, capability)
	defer session.ReleasePrefill()

	// Inherit the trace context from the inbound HTTP request so spans
	// created here become children of the Gin middleware root span.
	parentCtx := pending.Ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	tracer := otel.Tracer("hivenet-router")
	spanCtx, span := tracer.Start(parentCtx, "dispatch",
		trace.WithAttributes(
			attribute.String("request.id", pending.ID),
			attribute.String("model", pending.Request.Model),
			attribute.String("tenant.id", pending.TenantID),
		),
	)
	defer span.End()

	// Use the request deadline as the selection context so that capacity waits
	// in the per-model queue are automatically bounded by the client's timeout.
	ctx, cancel := context.WithDeadline(spanCtx, pending.Deadline)
	defer cancel()

	// reconnects tracks free, budget-exempt re-dials granted per agent within this
	// request (see maxReconnectsPerAgent).
	reconnects := make(map[peer.ID]int)

	for {
		// Check deadline before each attempt so expired requests are never forwarded.
		if pending.IsExpired() {
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			pending.Error <- domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request timed out waiting for an available agent", domain.SourceRouter)
			return
		}

		agent, err := session.Select(ctx)
		if err != nil {
			// All local policy steps exhausted — try the external fallback: the
			// key's own provider account (BYOK) first, else the policy's
			// fallback_provider (deprecated).
			target, streamRefused := p.externalTarget(pending, session)
			if target != nil {
				p.serveExternally(pending, target)
				return
			}
			session.EmitExhaustionLogs(pending.ID, pending.TenantID)
			p.metrics.RequestFailed("", "", pending.Request.Model, pending.TenantID)
			p.metrics.PolicyExhausted(pending.Request.Model)
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			selErr := mapSelectorError(err)
			if streamRefused {
				selErr.Message += " (the external fallback answers only non-streaming requests)"
			}
			pending.Error <- selErr
			return
		}

		// Select may have blocked in the capacity wait queue while the request
		// deadline expired (e.g. queue woke on slot free, then advanced to a
		// fallback step that had capacity). The slot is already acquired; release
		// it cleanly before returning so the next waiter can use it immediately.
		if pending.IsExpired() {
			p.slotRelease(agent)()
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			pending.Error <- domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request timed out waiting for an available agent", domain.SourceRouter)
			return
		}

		// Stamp the deployment from the selected agent so all downstream metric calls
		// use the real deployment ID rather than the request-time "unset" default.
		pending.DeploymentID = agent.Metadata.DeploymentID
		p.metrics.PrimeTenantCounters(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)

		// Propagate key attributes to the parent HTTP span (POST /v1/chat/completions)
		// so Tempo span-metrics carry them as dimensions on the root span. Without
		// this, dashboards filtering on the root span_name can't filter by model or
		// peer_id because those attributes only exist on child spans.
		peerIDAttr := attribute.String("peer_id", agent.ID.String())
		span.SetAttributes(peerIDAttr)
		rootSpan := trace.SpanFromContext(pending.Ctx)
		rootSpan.SetAttributes(
			peerIDAttr,
			attribute.String("model", pending.Request.Model),
		)

		stepName := session.StepName()
		log.Debugf("Policy step %q selected agent %s (strategy: %s, try %d/%d)",
			stepName, shortID(agent.ID), session.StepStrategy(),
			session.StepTries()+1, session.StepMaxTries())

		// forwardToAgent releases the slot and wakes one waiter when the slot is
		// actually free: on return for errors and non-streaming responses, at
		// the end of the stream for streaming ones.
		pending.PrefillTokens = session.PrefillTokens()
		pending.PredictedBlocks, pending.PredictedAt = session.PredictedClaim()
		resp, rttMs, fwdErr := p.forwardFn(ctx, agent, pending)
		// The agent has answered (the first byte, for a stream) or failed:
		// its prefill no longer waits on this request.
		session.ReleasePrefill()

		if fwdErr == nil {
			if stepName == "routing_policy" {
				p.metrics.PolicyPrimaryRouted(pending.Request.Model)
			} else {
				p.metrics.PolicyFallbackRouted(pending.Request.Model)
			}
			pending.Response <- resp
			return
		}

		// Nothing reached the agent: the router could not prepare the request
		// (e.g. a body it cannot salt). That says nothing about the agent, so
		// it fails the request without touching any agent counter: neither a
		// failure, nor a success with the near-zero RTT of a request that was
		// never sent, which would make the agent look fast.
		var unsent *unsentError
		if errors.As(fwdErr, &unsent) {
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			pending.Error <- unsent.err
			return
		}

		// Short-circuit on non-retryable client errors (e.g. context_length_exceeded,
		// invalid_parameter). These are request-level failures — retrying against other
		// agents would produce the same error and incorrectly penalise healthy agents.
		var re *domain.RouterError
		if errors.As(fwdErr, &re) && isNonRetryable(re.Code) {
			log.Debugf("Non-retryable error from agent %s (%s) — short-circuiting (request: %s)",
				shortID(agent.ID), re.Code, pending.ID)
			// Record the tenant failure here (not in forwardToAgent) so it fires exactly
			// once per client-visible failure regardless of internal retry attempts.
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			// Every non-retryable code is a verdict on the request, not the agent: a
			// token-limit rejection is a router-side quota decision on a completed
			// request, and context_length_exceeded / invalid_parameter mean the backend
			// answered correctly that the request is invalid. Recording a failure would
			// degrade the agent's success-rate and consecutiveFails streak, so a client
			// repeatedly sending over-long prompts could get a healthy agent excluded by
			// policy. Use RecordSuccess with zero token counts so SRTT and success-rate
			// stay accurate.
			p.counters.RecordSuccess(agent, 0, 0, rttMs)
			pending.Error <- re
			return
		}

		// Request deadline expired while waiting on the agent: the request's own
		// budget ran out before the agent produced a response. The connection is
		// fine, so do NOT evict it — ClosePeer would GOAWAY the agent, making it
		// drop and re-register and truncating every other in-flight stream on the
		// connection. Do NOT record a failure either: queueing behind long
		// generations is not an agent defect, and a multi-minute RTT would poison
		// its SRTT / consecutive-failure streak. Only the dedicated timeout streak
		// grows (reset by any success), so an agent whose backend is hung — every
		// request times out, none succeed — is still caught by a
		// consecutive_timeouts gate. All remaining attempts share this same,
		// now-expired request deadline, so fail the request now instead of
		// burning policy try budget on doomed re-dispatches.
		if errors.As(fwdErr, &re) && re.Code == domain.ErrCodeRequestTimeout && re.Source == domain.SourceRouter {
			log.Warnf("dispatch: request %s exceeded its deadline on agent %s — failing request, keeping the connection (error: %v)",
				pending.ID, shortID(agent.ID), fwdErr)
			p.counters.RecordTimeout(agent)
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			pending.Error <- fwdErr
			return
		}

		// Connection-level failure: the libp2p path to this agent is dead (a stale
		// connection), not an agent-application error. Drop the connection so the
		// agent's next heartbeat fails fast and it reconnects + re-registers (the
		// router never dials agents), and retry WITHOUT charging the policy try
		// budget — max_tries is reserved for genuine agent failures (backend errors,
		// overload). Capped per agent so a truly unreachable agent still falls
		// through to normal escalation.
		if isConnectionLevelError(fwdErr) && reconnects[agent.ID] < maxReconnectsPerAgent {
			reconnects[agent.ID]++
			if p.shouldEvict(agent.ID) {
				if cerr := p.peers.ClosePeer(agent.ID); cerr != nil {
					log.Debugf("dispatch: ClosePeer(%s) failed: %v", shortID(agent.ID), cerr)
				} else {
					log.Infof("dispatch: dropped stale connection to agent %s (%v) — agent will reconnect (request: %s)",
						shortID(agent.ID), fwdErr, pending.ID)
				}
				p.metrics.AgentConnectionReset(agent.Metadata.Model, "forward_failure")
			}
			// The transport genuinely failed, so keep SRTT/health honest. The immediate
			// re-dial success will RecordSuccess and reset the consecutive-failure streak.
			p.counters.RecordFailure(agent, rttMs)
			continue
		}

		// Forward failed. forwardToAgent already called DecrementLoad via its defer —
		// do NOT call it again here. Pass the measured RTT (0 for pre-send failures,
		// real value for post-send failures) so SRTT stays accurate on failure paths.
		// An agent that is only full (a libp2p resource limit) is not unhealthy:
		// the request moves on without marking the agent down.
		var busy *domain.RouterError
		if !errors.As(fwdErr, &busy) || busy.Code != domain.ErrCodeNoCapacity {
			p.counters.RecordFailure(agent, rttMs)
		}

		advanced := session.RecordFailure(agent.ID)
		if advanced {
			next := session.StepName()
			if next == "" {
				next = "(none remaining)"
			}
			log.Debugf("Policy step %q exhausted try budget, escalating to %q (request: %s, error: %v)",
				stepName, next, pending.ID, fwdErr)
		} else {
			log.Debugf("Forward to agent %s failed (%v), retrying within step %q (request: %s)",
				shortID(agent.ID), fwdErr, stepName, pending.ID)
		}
	}
}

// slotRelease returns the function that frees agent's in-flight slot. It runs
// at most once, and it wakes one request waiting for capacity on the agent's
// model, so a waiter is only woken when the slot is really free.
func (p *RequestProcessor) slotRelease(agent *domain.Agent) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			agent.DecrementLoad()
			if p.executor != nil {
				p.executor.SignalCapacity(domain.PoolKey(agent.Metadata.Model, agent.Metadata.ReservedFor))
			}
		})
	}
}

// forwardToAgent forwards a request to an agent via libp2p HTTP and returns the response
// along with the measured RTT in milliseconds.
//
// RTT is 0 for pre-send failures (marshal, client creation) where no network round-trip
// occurred. For post-send failures (non-200, decode error, network error after dial) the
// real RTT is returned so the caller can pass it to RecordFailure and keep SRTT accurate.
//
// Slot ownership: DecrementLoad is called exactly once per forward on ALL paths —
// via a sync.Once: by the deferred release on error paths, explicitly before
// RecordSuccess on the non-streaming success path, and by drainStream when a
// streamed response fully drains (the slot must cover the whole generation so
// least-loaded sees the agent's real concurrent load). The same release wakes
// one request waiting for capacity on the model, so a waiter is only woken
// once the slot is really free. The caller must NEVER call DecrementLoad.
//
// NamespacedClient fetches the agent's /.well-known/libp2p document, resolves the path
// prefix for inferenceProtocolID, and prepends it to every outbound request URL — so
// the agent's handler receives the path stripped of the protocol prefix.
func (p *RequestProcessor) forwardToAgent(parentCtx context.Context, agent *domain.Agent, pending *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
	// Release the slot exactly once. Early error paths that return before the
	// response completes rely on the deferred release. Non-streaming success
	// returns only after the body is fully read, so the deferred release lands
	// on completion. Streaming success sets releaseOnReturn = false and hands
	// the release to the drainStream goroutine (see below), so the slot stays
	// held for the whole generation instead of just the TTFT window.
	releaseOnReturn := true
	decrement := p.slotRelease(agent)
	defer func() {
		if releaseOnReturn {
			decrement()
		}
	}()

	region := agent.Metadata.Region
	engine := agent.Metadata.Engine
	model := agent.Metadata.Model

	agentID := shortID(agent.ID)
	log.Debugf("Sending request %s to agent %s", pending.ID, agentID)

	// Record the full peer ID before the network call so the handler can log
	// which agent was in-flight if the handler-side deadline fires first.
	pending.DispatchedAgentID.Store(agent.ID.String())

	if pending.Request == nil {
		return nil, 0, NotSent(fmt.Errorf("missing request"))
	}

	// The path this attempt forwards to: embeddings and rerank by capability,
	// otherwise the handler's path (e.g. "/v1/messages" for the Anthropic
	// passthrough), defaulting to chat completions.
	forwardPath := pending.Path
	switch {
	case pending.Capability == domain.CapabilityEmbedding:
		forwardPath = "/v1/embeddings"
	case pending.Capability == domain.CapabilityReranker:
		forwardPath = "/v1/rerank"
	case forwardPath == "":
		forwardPath = "/v1/chat/completions"
	}

	// Salt per attempt, for the engine this attempt lands on: a retry or a
	// fallback step may reach a different engine. RawBytes is shared by every
	// attempt and is never modified.
	body := pending.Request.RawBytes
	if p.salter != nil {
		salted, err := p.salter.Apply(body, engine, forwardPath, pending.TenantID)
		if err != nil {
			return nil, 0, NotSent(domain.NewRouterError(domain.ErrCodeRequestInvalid, "Invalid request body: "+err.Error(), domain.SourceRouter))
		}
		body = salted
	}

	// AddrInfo carries the peer ID only — no dial addresses. The stream is opened
	// over the connection the agent itself established at registration (libp2p's
	// Connect returns early when Connectedness == Connected, and heartbeats keep
	// that connection alive). If the connection is gone, this fails immediately
	// with a connection-level error and dispatch escalates; recovery is always
	// agent-initiated reconnection, never a router-side dial.
	client, err := p.httpHost.NamespacedClient(
		inferenceProtocolID,
		peer.AddrInfo{ID: agent.ID},
	)
	if err != nil {
		p.metrics.RequestFailed(region, engine, model, pending.TenantID)
		// NamespacedClient fetches the agent's /.well-known/libp2p over the connection,
		// so a dead/stale connection fails here. Return a structured agent_disconnected
		// error so dispatchWithPolicy treats it as connection-level (re-dial) and the
		// audit log shows the real cause instead of a generic backend error.
		return nil, 0, domain.NewRouterError(domain.ErrCodeAgentDisconnected,
			fmt.Sprintf("failed to establish client for agent: %v", err), domain.SourceRouter)
	}

	tracer := otel.Tracer("hivenet-router")
	spanCtx, forwardSpan := tracer.Start(parentCtx, "forward_to_agent",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("peer_id", agent.ID.String()),
			attribute.String("agent.id", agent.ID.String()),
			attribute.String("agent.model", model),
			attribute.String("agent.engine", engine),
			attribute.String("agent.region", region),
		),
	)
	defer forwardSpan.End()

	ctx, cancel := context.WithDeadline(spanCtx, pending.Deadline)
	// For non-streaming requests, cancel is deferred normally.
	// For streaming, the goroutine that drains resp.Body takes ownership of cancel.
	deferCancel := true
	defer func() {
		if deferCancel {
			cancel()
		}
	}()

	// Embedding and reranking: simple synchronous forward, no streaming. The
	// input tokens come from the backend's usage after the response.
	if pending.Capability == domain.CapabilityEmbedding || pending.Capability == domain.CapabilityReranker {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, forwardPath, bytes.NewReader(body))
		if err != nil {
			return nil, 0, NotSent(fmt.Errorf("failed to build HTTP request: %w", err))
		}
		// Forward the headers the handler set (only X-Request-ID: client headers
		// are not forwarded on this path), then enforce Content-Type and inject
		// W3C trace context, same as the LLM path. A copy, so one attempt's
		// headers never leak into the next or into the pending request.
		if pending.Request.HttpHeaders != nil {
			req.Header = pending.Request.HttpHeaders.Clone()
		}
		req.Header.Set("X-Request-ID", pending.ForwardID())
		req.Header.Set(domain.HeaderTimeout, domain.TimeoutHeader(pending.Deadline))
		req.Header.Set("Content-Type", "application/json")
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

		start := time.Now()
		resp, err := client.Do(req)
		rttMs := float64(time.Since(start).Milliseconds())
		forwardSpan.SetAttributes(attribute.Float64("rtt_ms", rttMs))
		if err != nil {
			forwardSpan.RecordError(err)
			forwardSpan.SetStatus(codes.Error, err.Error())
			p.metrics.RequestFailed(region, engine, model, pending.TenantID)
			if requestDeadlinePassed(ctx) {
				return nil, rttMs, domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request deadline exceeded while contacting agent", domain.SourceRouter)
			}
			if isResourceLimit(err) {
				return nil, rttMs, domain.NewRouterError(domain.ErrCodeNoCapacity, fmt.Sprintf("Agent busy: %v", err), domain.SourceRouter)
			}
			return nil, rttMs, domain.NewRouterError(domain.ErrCodeAgentDisconnected, fmt.Sprintf("Agent unreachable: %v", err), domain.SourceRouter)
		}
		defer resp.Body.Close()
		forwardSpan.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

		rawBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			p.metrics.RequestFailed(region, engine, model, pending.TenantID)
			return nil, rttMs, fmt.Errorf("failed to read agent response: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			p.metrics.RequestFailed(region, engine, model, pending.TenantID)
			return nil, rttMs, domain.DecodeAgentError(resp.StatusCode, rawBytes)
		}

		tokens, estimated := poolingTokens(engine, rawBytes, pending)
		p.chargePoolingTokens(pending, tokens)

		p.metrics.RequestRouted(region, engine, model, pending.TenantID)
		p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, model, tokens, 0)
		decrement()
		p.counters.RecordSuccess(agent, tokens, 0, rttMs)
		headers := resp.Header.Clone()
		headers.Del("Content-Length")
		return &domain.ChatResponse{
			RawBytes:       rawBytes,
			HttpHeaders:    headers,
			ProcessedBy:    agent.ID.String(),
			Usage:          domain.Usage{PromptTokens: tokens, TotalTokens: tokens},
			UsageEstimated: estimated,
		}, rttMs, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		forwardPath, bytes.NewReader(body))
	if err != nil {
		return nil, 0, NotSent(fmt.Errorf("failed to build HTTP request: %w", err))
	}
	// apply original request headers (including any custom headers added by the openAI client library) so they are forwarded to the backend engine.
	// A copy, so one attempt's headers never leak into the next or into the pending request.
	if pending.Request.HttpHeaders != nil {
		req.Header = pending.Request.HttpHeaders.Clone()
	}
	// Each attempt its own backend request ID: see ForwardID.
	req.Header.Set("X-Request-ID", pending.ForwardID())
	req.Header.Set(domain.HeaderTimeout, domain.TimeoutHeader(pending.Deadline))
	if pending.PrefixAnchors != "" {
		req.Header.Set(domain.HeaderPrefixAnchors, pending.PrefixAnchors)
	} else {
		req.Header.Del(domain.HeaderPrefixAnchors) // never trust a client's copy
	}
	if pending.PrefillTokens > 0 {
		req.Header.Set(domain.HeaderPrefillTokens, strconv.Itoa(pending.PrefillTokens))
	} else {
		req.Header.Del(domain.HeaderPrefillTokens)
	}

	// Inject W3C trace context headers so the agent can create child spans.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	start := time.Now()
	resp, err := client.Do(req)
	rttMs := float64(time.Since(start).Milliseconds())
	forwardSpan.SetAttributes(attribute.Float64("rtt_ms", rttMs))
	if err != nil {
		forwardSpan.RecordError(err)
		forwardSpan.SetStatus(codes.Error, err.Error())
		p.metrics.RequestFailed(region, engine, model, pending.TenantID)
		if requestDeadlinePassed(ctx) {
			return nil, rttMs, domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request deadline exceeded while contacting agent", domain.SourceRouter)
		}
		if isResourceLimit(err) {
			return nil, rttMs, domain.NewRouterError(domain.ErrCodeNoCapacity, fmt.Sprintf("Agent busy: %v", err), domain.SourceRouter)
		}
		return nil, rttMs, domain.NewRouterError(domain.ErrCodeAgentDisconnected, fmt.Sprintf("Agent unreachable: %v", err), domain.SourceRouter)
	}
	// For non-streaming, close the body when this function returns.
	// For streaming, the goroutine takes ownership; deferClose prevents double-close.
	deferClose := true
	defer func() {
		if deferClose {
			resp.Body.Close()
		}
	}()

	forwardSpan.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

	if resp.StatusCode != http.StatusOK {
		p.metrics.RequestFailed(region, engine, model, pending.TenantID)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		// The agent's envelope when it is one the router can trust, else the
		// body classified as the backend's (DecodeAgentError).
		return nil, rttMs, domain.DecodeAgentError(resp.StatusCode, body)
	}

	var chatResp domain.ChatResponse
	chatResp.HttpHeaders = resp.Header.Clone()
	chatResp.HttpHeaders.Del("Content-Length")

	if domain.IsStreamingResponse(resp.Header) {
		// Streaming: hand the response body to a goroutine that proxies bytes via
		// an io.Pipe. The goroutine owns resp.Body and cancel so neither is closed
		// prematurely by the defers above.
		deferClose = false
		deferCancel = false
		pr, pw := io.Pipe()
		meter := NewSSETokenMeter()
		// Grow the occupancy reservation as undeclared output streams so the
		// budget reflects live footprint (a no-op for a declared request, which
		// reserved its max_tokens up front). Runs before pw.Close(), so all
		// growth lands before the handler releases the reservation.
		if pending.Reservation != nil {
			meter.SetContentObserver(NewGrowthObserver(pending.Reservation))
		}
		// RTT is time-to-first-byte (headers already received above). Token-bearing
		// metrics are recorded by drainStream once the stream completes; here we
		// only mark the request as routed. The in-flight slot stays held until the
		// stream drains (drainStream releases it) — not until it starts — so
		// least-loaded sees the agent's real concurrent load during generation
		// instead of treating every streaming request as instantly idle.
		releaseOnReturn = false
		p.metrics.RequestRouted(region, engine, model, pending.TenantID)
		go p.drainStream(agent, pending, pw, resp, cancel, meter, region, engine, model, rttMs, decrement)
		chatResp.Body = pr
		chatResp.ProcessedBy = agent.ID.String()
		log.Debugf("Streaming response started for %s from agent %s", pending.ID, agentID)
		return &chatResp, rttMs, nil
	}

	// Non-streaming: buffer and parse the full response.
	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		p.metrics.RequestFailed(region, engine, model, pending.TenantID)
		return nil, rttMs, fmt.Errorf("failed to read agent response body: %w", err)
	}
	chatResp.RawBytes = rawBytes
	if err := json.Unmarshal(chatResp.RawBytes, &chatResp); err != nil {
		p.metrics.RequestFailed(region, engine, model, pending.TenantID)
		return nil, rttMs, fmt.Errorf("invalid response format: %w", err)
	}

	// Daily budget: settle the hold set at admission with the tokens the
	// backend reports (its prompt and completion; the held estimate for a
	// prompt it did not report), after the agent processed the request. If
	// they do not fit, the budgets take what they have left and the response
	// is discarded with 429.
	if settled, allowed, remaining, limErr := p.settleDaily(pending, chatResp.Usage.PromptTokens, chatResp.Usage.CompletionTokens); settled {
		if limErr != nil {
			// Redis error — fail-open: treat as allowed and proceed.
			log.Warnf("Output token check error for request %s: %v (fail-open)", pending.ID, limErr)
		} else if !allowed {
			p.metrics.TenantTokenLimited(pending.TenantID, pending.KeyID, pending.DeploymentID, "output")
			p.metrics.RequestFailed(region, engine, model, pending.TenantID)
			// Do NOT call TenantRequestFailed here. dispatchWithPolicy treats
			// ErrCodeTokenLimitExceeded as non-retryable and calls TenantRequestFailed
			// exactly once at the short-circuit path (line ~198). Calling it here too
			// would double-count every output-token rejection.
			//
			// Attach actual usage and agent ID so the audit log records what the agent
			// produced, not zeros. Inference completed — the budget decision is ours.
			// Copy Usage so only the token counts are retained — &chatResp.Usage would
			// keep the entire ChatResponse (including completion content) alive until GC.
			usageCopy := chatResp.Usage
			re := domain.NewRouterError(domain.ErrCodeTokenLimitExceeded,
				// An output charge is judged against the budgets alone, never
				// this router's share, and it already filled the budget it
				// overflowed: name that one.
				auth.ExplainSpent(p.limiter, dailyBudget(pending)).Message("daily token budget exceeded"),
				domain.SourceRouter)
			re.Usage = &usageCopy
			re.AgentID = agent.ID.String()
			return nil, rttMs, re
		} else {
			pending.RemainingTokens = remaining
		}
	}

	p.metrics.RequestRouted(region, engine, model, pending.TenantID)
	p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, model, chatResp.Usage.PromptTokens, chatResp.Usage.CompletionTokens)
	p.checkPrefix(agent, pending, chatResp.Usage.CachedTokens, chatResp.Usage.CachedKnown)
	decrement()
	p.counters.RecordSuccess(agent, chatResp.Usage.PromptTokens, chatResp.Usage.CompletionTokens, rttMs)
	chatResp.ProcessedBy = agent.ID.String()
	log.Debugf("Received response for %s from agent %s", pending.ID, agentID)
	return &chatResp, rttMs, nil
}

// drainStream proxies an agent's streaming response body into the client pipe
// byte-exact, performs post-hoc token accounting once the stream ends, and
// releases the in-flight slot exactly when the generation is done. releaseSlot is
// the forwardToAgent slot-release closure (sync.Once-backed, so a redundant call
// is a no-op); it is invoked after io.Copy returns — normal stream end or client
// disconnect — so least-loaded observes the agent's real concurrent load for the
// whole generation, not just the TTFT window.
//
// Truncation handling: if the upstream stream dies mid-generation (agent
// connection reset, request deadline) or closes without the terminal marker
// (finish_reason / [DONE]), the client would otherwise see a partial stream end
// silently and treat it as a complete answer. drainStream therefore injects a
// terminal SSE error frame before closing the pipe and records
// hivenet_stream_truncated_total. A client-side failure (the handler closed the
// pipe because the client went away) fails the frame write and is not counted.
//
// The goroutine owns resp.Body and cancel: both stay open until the stream has
// fully drained, which is what makes the late slot release safe.
func (p *RequestProcessor) drainStream(
	agent *domain.Agent,
	pending *domain.PendingRequest,
	pw *io.PipeWriter,
	resp *http.Response,
	cancel context.CancelFunc,
	meter *SSETokenMeter,
	region, engine, model string,
	rttMs float64,
	releaseSlot func(),
) {
	defer cancel()
	defer pw.Close()
	defer resp.Body.Close()
	// Forward bytes to the client byte-exact while a copy feeds the token
	// meter (TeeReader never alters the forwarded stream). The capturing
	// reader records an upstream (agent) read failure separately so it can be
	// told apart from a client-side write failure after io.Copy returns. The
	// line writer forwards whole lines only, so a stream cut mid-line never
	// leaves a half event in front of the terminal error frame.
	var srcErr error
	out := &lineWriter{w: pw}
	_, copyErr := io.Copy(out, &errCaptureReader{r: io.TeeReader(resp.Body, meter), e: &srcErr})
	meter.Flush()

	// A write failure (copyErr set, no upstream read error) means the client
	// went away mid-stream: the client chose to stop, so this is not a system
	// truncation — do not count it and do not inject a frame (the write would
	// be a no-op anyway).
	clientGone := copyErr != nil && srcErr == nil
	if clientGone {
		log.Debugf("Stream for request %s ended because the client disconnected — not counted as truncation", pending.ID)
	}

	// Detect a truncated stream before the pipe closes: (1) the agent stream
	// errored mid-generation (connection reset, deadline), or (2) the stream
	// closed cleanly but never carried the terminal marker — the agent's own
	// write side failed and the handler returned early, which the router sees
	// as a clean EOF with no finish_reason / [DONE] (OpenAI) or message_stop
	// (Anthropic). A body with no recognised events is never misjudged.
	var truncReason string
	switch {
	case srcErr != nil:
		truncReason = "upstream_error"
	case !clientGone && meter.SawEvents() && !meter.SawTerminal():
		truncReason = "missing_terminal"
	}
	if truncReason == "" {
		if !clientGone {
			_ = out.flush()
		}
	} else {
		p.metrics.StreamTruncated(model, truncReason)
		extra := ""
		if srcErr != nil {
			extra = ": " + srcErr.Error()
		}
		log.Warnf("Stream for request %s truncated (%s%s) — injecting terminal error frame",
			pending.ID, truncReason, extra)
		// Terminal SSE error frame in the request's dialect: clients that
		// understand it surface a retryable error instead of a silent partial
		// answer. A held partial line is dropped (it cannot be completed), and
		// an open event is closed first so the frame is parsed on its own. If
		// the client already went away the write fails — a no-op, the metric
		// above still records the event.
		_, _ = pw.Write([]byte(out.eventBoundary() + truncationFrame(pending.Path)))
	}

	// Stream finished — the token counts are now known. Accounting is
	// post-hoc: the response is already delivered, so this deducts the
	// actual completion tokens from the daily budget (best-effort, fail-open;
	// it cannot reject a response that has already been streamed) and records
	// real token metrics in place of the previous zero placeholders.
	prompt, completion := meter.Tokens(pending.Request)
	// Publish the totals to the handler so the audit log carries them.
	// Stored BEFORE pw.Close() fires (deferred), so the handler's io.Copy
	// is still blocked when we set these — guaranteeing visibility on the
	// handler side as soon as it unblocks.
	pending.StreamedPromptTokens.Store(int64(prompt))
	pending.StreamedCompletionTokens.Store(int64(completion))
	pending.StreamedPromptExact.Store(meter.HaveUsage())
	exactPrompt := 0
	if meter.HaveUsage() {
		exactPrompt = prompt
	}
	if settled, allowed, _, err := p.settleDaily(pending, exactPrompt, completion); settled {
		if err != nil {
			log.Warnf("Streaming output token accounting for %s: %v (fail-open)", pending.ID, err)
		} else if !allowed {
			// The stream is already delivered and cannot be rejected mid-flight,
			// but record the limit event so quota/abuse metrics stay accurate.
			p.metrics.TenantTokenLimited(pending.TenantID, pending.KeyID, pending.DeploymentID, "output")
		}
	}
	p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, model, prompt, completion)
	if cached, known := meter.Cached(); known {
		p.checkPrefix(agent, pending, cached, true)
	}
	// Release the in-flight slot before recording the success, mirroring the
	// non-streaming path (decrement before RecordSuccess) so the
	// CapacityUtilization published by RecordSuccess reflects post-completion
	// load. RecordSuccess does not depend on the slot being held, and routing
	// reads GetLoad() directly, so this is metric-accuracy only.
	releaseSlot()
	p.counters.RecordSuccess(agent, prompt, completion, rttMs)
}

// unsentError marks an error the router raised before it sent anything to the
// agent (preparing the body, building the request). dispatchWithPolicy fails
// the request on it without recording anything on the agent.
type unsentError struct{ err error }

func (e *unsentError) Error() string { return e.err.Error() }
func (e *unsentError) Unwrap() error { return e.err }

// NotSent wraps an error a ForwardFunc raised before it sent anything to the
// agent, such as a body it could not prepare. The request fails with err, and
// nothing is recorded on the agent: the error says nothing about it.
func NotSent(err error) error { return &unsentError{err: err} }

// dailyBudget names the daily token budgets a request's output is charged
// to: the same buckets its prompt estimate was charged to at admission. A
// request with neither budget is still charged, so it counts in its tenant's
// usage today.
// settleDaily charges the daily budgets the tokens a request used: through
// its hold when the handler set one, else (no prompt estimate was held) the
// completion alone, as an output charge. settled is false when there was
// nothing to charge.
func (p *RequestProcessor) settleDaily(pending *domain.PendingRequest, prompt, completion int) (settled, allowed bool, remaining int, err error) {
	if ok, allowed, remaining, err := pending.SettleDaily(prompt, completion); ok {
		return true, allowed, remaining, err
	}
	if p.limiter == nil || completion <= 0 {
		return false, true, -1, nil
	}
	allowed, remaining, err = p.limiter.AllowOutputTokens(dailyBudget(pending), completion)
	return true, allowed, remaining, err
}

func dailyBudget(p *domain.PendingRequest) auth.DailyBudget {
	return auth.DailyBudget{TenantID: p.TenantID, KeyID: p.KeyID, Model: p.QuotaModel,
		KeyTokensPerDay: p.TokensPerDay, TenantTokensPerDay: p.TenantTokensPerDay}
}

// engineUsageInCharacters lists the engines whose embedding and rerank usage
// counts characters, not tokens: Infinity reports the characters it read in
// usage.prompt_tokens.
var engineUsageInCharacters = map[string]bool{"infinity": true}

// poolingTokens is the input tokens of an embedding or rerank response: the
// backend's usage when it reports one in tokens, else the estimate the handler
// charged at admission (estimated true).
func poolingTokens(engine string, body []byte, pending *domain.PendingRequest) (tokens int, estimated bool) {
	if !engineUsageInCharacters[engine] {
		if n, ok := domain.ParsePoolingUsage(body); ok {
			return n, false
		}
	}
	return pending.EstimatedInputTokens, true
}

// chargePoolingTokens charges the daily budgets the input tokens of an
// embedding or rerank response. Admission only checked the estimate against
// them, so the budgets count exactly what the backend read. The answer is
// computed, so it is returned even when this charge runs a budget out (the
// charge takes what is left, and the next request is refused at admission);
// RemainingTokens then reads 0.
func (p *RequestProcessor) chargePoolingTokens(pending *domain.PendingRequest, tokens int) {
	if p.limiter == nil || tokens <= 0 {
		return
	}
	allowed, remaining, err := p.limiter.AllowOutputTokens(dailyBudget(pending), tokens)
	switch {
	case err != nil:
		log.Warnf("Input token charge error for request %s: %v (fail-open)", pending.ID, err)
	case !allowed:
		pending.RemainingTokens = 0
	default:
		pending.RemainingTokens = remaining
	}
}

// truncationFrame returns the terminal SSE error frame drainStream injects into
// a truncated stream, shaped for the request's dialect: an Anthropic error
// event for /v1/messages, the OpenAI error chunk convention otherwise.
func truncationFrame(path string) string {
	const msg = "upstream stream ended before completion; the response may be partial"
	if strings.HasPrefix(path, "/v1/messages") {
		return "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"stream_truncated: " + msg + "\"}}\n\n"
	}
	return "data: {\"error\":{\"code\":\"stream_truncated\",\"message\":\"" + msg + "\",\"type\":\"server_error\"}}\n\n"
}

// lineWriter forwards only newline-terminated lines and holds back a partial
// trailing line until its newline arrives. SSE consumers cannot act on a
// partial line anyway, so holding it adds no visible latency, and it lets
// drainStream drop the half line of a stream cut mid-event instead of gluing
// the terminal error frame onto it. A line longer than maxHeldLine is
// forwarded as-is: holding is best-effort and must neither grow without bound
// nor stall a stream that never sends a newline.
type lineWriter struct {
	w    io.Writer
	held []byte
	// tail is the last two bytes forwarded, used to tell whether the client
	// is between events (after a blank line) or inside one.
	tail [2]byte
	n    int
}

// maxHeldLine bounds the partial line lineWriter holds back.
const maxHeldLine = 64 << 10

func (lw *lineWriter) Write(p []byte) (int, error) {
	i := bytes.LastIndexByte(p, '\n')
	if i < 0 {
		lw.held = append(lw.held, p...)
		if len(lw.held) > maxHeldLine {
			if err := lw.flush(); err != nil {
				return 0, err
			}
		}
		return len(p), nil
	}
	if err := lw.forward(append(lw.held, p[:i+1]...)); err != nil {
		return 0, err
	}
	lw.held = append(lw.held[:0], p[i+1:]...)
	return len(p), nil
}

// flush forwards the held partial line, if any. Used when the stream ended
// complete, so an unterminated final line still reaches the client.
func (lw *lineWriter) flush() error {
	if len(lw.held) == 0 {
		return nil
	}
	err := lw.forward(lw.held)
	lw.held = nil
	return err
}

func (lw *lineWriter) forward(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	if _, err := lw.w.Write(b); err != nil {
		return err
	}
	if len(b) >= 2 {
		lw.tail = [2]byte{b[len(b)-2], b[len(b)-1]}
	} else {
		lw.tail = [2]byte{lw.tail[1], b[0]}
	}
	lw.n += len(b)
	return nil
}

// eventBoundary returns what must be written before a new SSE event so it is
// not merged into the event the client is currently reading: nothing at the
// start of the stream or after a blank line, a newline after a complete line.
func (lw *lineWriter) eventBoundary() string {
	if lw.n == 0 || lw.tail == [2]byte{'\n', '\n'} {
		return ""
	}
	return "\n"
}

// errCaptureReader wraps a reader and records the first non-EOF read error
// into e. drainStream uses it to tell an upstream (agent) stream failure apart
// from a client-side write failure after io.Copy returns: a read error means
// the agent's stream died and the client deserves a terminal error frame, a
// write error means the client went away and the frame would only be noise.
type errCaptureReader struct {
	r io.Reader
	e *error
}

func (rc *errCaptureReader) Read(p []byte) (int, error) {
	n, err := rc.r.Read(p)
	if err != nil && err != io.EOF && rc.e != nil && *rc.e == nil {
		*rc.e = err
	}
	return n, err
}

// externalTarget is where a request goes when no local agent can serve it.
type externalTarget struct {
	kind     string // "byok" (the key's own account) or "cloud" (the operator's, deprecated)
	provider string
	model    string
	call     func() (*domain.ChatResponse, error)
}

// externalTarget returns the external fallback for a request no local agent
// could serve, or nil: the key's own provider account (BYOK) first, else the
// policy's fallback_provider. Only a non-streaming chat completion qualifies,
// the one shape the providers' clients send and decode: an Anthropic
// /v1/messages body, a token count or an embedding would be answered as a
// chat completion. And never a request bound to a scope: the provider runs
// outside the operator's sites. streamRefused reports a streaming request
// that a fallback would otherwise have taken.
func (p *RequestProcessor) externalTarget(pending *domain.PendingRequest, session *policy.Session) (t *externalTarget, streamRefused bool) {
	if pending.Capability != domain.CapabilityLLM || (pending.Path != "" && pending.Path != chatCompletionsPath) || !externalAllowed(pending) {
		return nil, false
	}
	switch fb, fp := pending.ExternalFallback, session.FallbackProvider(); {
	case fb != nil:
		t = &externalTarget{kind: "byok", provider: fb.Provider, model: fb.Model,
			call: func() (*domain.ChatResponse, error) { return p.tryBYOK(pending, fb) }}
	case fp != nil:
		t = &externalTarget{kind: "cloud", provider: fp.Engine, model: fp.Model,
			call: func() (*domain.ChatResponse, error) { return p.tryProviderFallback(pending, fp.Engine, fp.Model) }}
	default:
		return nil, false
	}
	if pending.Request != nil && pending.Request.Stream {
		return nil, true
	}
	return t, false
}

// chatCompletionsPath is the only path the external fallback serves.
const chatCompletionsPath = "/v1/chat/completions"

// serveExternally answers pending from t, the external fallback. The
// provider serves it off-box, so the occupancy reservation is released
// first: no local KV is held, and the fallback fires exactly when the local
// pool is degraded and admission headroom matters most (Release is
// idempotent; the handler's deferred release stays safe). A provider's error
// reaches the client with the provider's own explanation (a bad key, an
// unknown model, an exhausted quota are the client's to fix with BYOK); logs
// get only its status.
func (p *RequestProcessor) serveExternally(pending *domain.PendingRequest, t *externalTarget) {
	if pending.IsExpired() {
		p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
		pending.Error <- domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request timed out before the external fallback could be attempted", domain.SourceRouter)
		return
	}
	if pending.Reservation != nil {
		pending.Reservation.Release()
	}
	resp, err := t.call()
	if err == nil {
		p.metrics.RequestRouted(t.kind, t.provider, t.model, pending.TenantID)
		if t.kind == "cloud" {
			// The operator's fallback only: BYOK is counted under kind "byok".
			p.metrics.PolicyProviderFallbackRouted(pending.Request.Model)
		}
		p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
		// The daily budgets count an externally served request at its held
		// prompt estimate, as before holds: the provider's usage is not
		// local GPU work.
		_, _, _, _ = pending.SettleDaily(0, 0)
		pending.Response <- resp
		return
	}
	// err never holds the provider's body (HTTPError.Error) nor the
	// credential (sent only as a header).
	log.Warnf("External fallback (%s %s/%s) failed for request %s: %v", t.kind, t.provider, t.model, pending.ID, err)
	p.metrics.RequestFailed(t.kind, t.provider, t.model, pending.TenantID)
	p.metrics.PolicyExhausted(pending.Request.Model)
	p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
	msg := err.Error()
	var he *provider.HTTPError
	if errors.As(err, &he) {
		msg = fmt.Sprintf("provider %s: %s", t.provider, he.ClientMessage())
	}
	pending.Error <- domain.NewRouterError(domain.ErrCodeBackendError, msg, domain.SourceBackend)
}

// externalAllowed reports whether a request may be sent to an external
// provider: not when it is bound to a scope narrower than "all".
func externalAllowed(pending *domain.PendingRequest) bool {
	return pending.Scope == "" || pending.Scope == domain.ScopeAll
}

// WithProviderBaseURLs overrides the providers' API endpoints by provider
// name ("openai", "anthropic"), for the operator's providers and the clients'
// own (BYOK) alike: an OpenAI-compatible gateway, a regional endpoint.
func WithProviderBaseURLs(urls map[string]string) ProcessorOption {
	return func(p *RequestProcessor) { p.providerBaseURLs = urls }
}

// byokClient is shared by every BYOK call: each call builds a provider for
// its key's credential, but keeps the connections (and TLS sessions) to the
// provider, which a new client per request would set up again at the moment
// the local pool is already exhausted.
var byokClient = &http.Client{Timeout: 120 * time.Second}

// tryBYOK sends the request to the key's own provider account. The client's
// credential is revealed only into the provider's auth header.
func (p *RequestProcessor) tryBYOK(pending *domain.PendingRequest, fb *domain.ExternalFallback) (*domain.ChatResponse, error) {
	prov, err := provider.New(provider.Config{
		Name: fb.Provider, APIKey: fb.APIKey.Reveal(), BaseURL: p.providerBaseURLs[fb.Provider], Metrics: p.metrics,
		HTTPClient: byokClient, // one connection pool for every key's calls
	})
	if err != nil {
		return nil, fmt.Errorf("external fallback: %w", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), pending.Deadline)
	defer cancel()
	log.Infof("Forwarding request %s to the key's own provider account %s (model: %s)", pending.ID, fb.Provider, fb.Model)
	resp, err := prov.Complete(ctx, pending.Request, fb.Model)
	if err != nil {
		return nil, fmt.Errorf("provider %s: %w", fb.Provider, err)
	}
	// Billing and the audit log tell it apart from the operator's provider:
	// the client pays the provider directly.
	resp.ProcessedBy = domain.BYOKProcessedByPrefix + fb.Provider
	if err := withRawBody(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// withRawBody gives a provider's decoded answer the OpenAI JSON body the
// handlers write (they write RawBytes; a provider decodes the answer into
// fields). Who served it stays out of the body: it is in the audit log.
func withRawBody(resp *domain.ChatResponse) error {
	if resp.Body != nil || len(resp.RawBytes) > 0 {
		return nil
	}
	out := *resp
	out.ProcessedBy = ""
	raw, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("provider response: encode: %w", err)
	}
	resp.RawBytes = raw
	return nil
}

// tryProviderFallback forwards pending to the named closed-source provider using the
// specified model. It is called only after all local policy steps are exhausted.
// Returns the ChatResponse on success, or an error if the provider is not configured
// or the HTTPS call fails.
func (p *RequestProcessor) tryProviderFallback(pending *domain.PendingRequest, engine, model string) (*domain.ChatResponse, error) {
	prov, ok := p.providers[engine]
	if !ok {
		return nil, fmt.Errorf("provider %q is not configured (missing API key)", engine)
	}

	ctx, cancel := context.WithDeadline(context.Background(), pending.Deadline)
	defer cancel()

	log.Infof("Forwarding request %s to provider fallback %s (model: %s)", pending.ID, engine, model)
	start := time.Now()
	resp, err := prov.Complete(ctx, pending.Request, model)
	if err != nil {
		return nil, fmt.Errorf("provider %s: %w", engine, err)
	}
	log.Infof("Provider fallback %s succeeded for request %s (%.0fms)", engine, pending.ID, float64(time.Since(start).Milliseconds()))
	if err := withRawBody(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// isNonRetryable reports whether an ErrorCode represents a client-side failure
// that will produce the same result on any agent. These errors must not be
// retried — doing so wastes capacity and incorrectly inflates agent failure counters.
func isNonRetryable(code domain.ErrorCode) bool {
	switch code {
	case domain.ErrCodeContextLengthExceeded, domain.ErrCodeInvalidParameter, domain.ErrCodeRequestInvalid,
		domain.ErrCodeTokenLimitExceeded:
		return true
	}
	return false
}

// mapSelectorError converts a policy selection error to a structured RouterError
// with the most specific error code available.
func mapSelectorError(err error) *domain.RouterError {
	switch {
	case errors.Is(err, policy.ErrModelNotFound):
		return domain.NewRouterError(domain.ErrCodeModelNotFound, "No agents registered for this model", domain.SourceRouter)
	case errors.Is(err, policy.ErrNoAgentsAvailable):
		return domain.NewRouterError(domain.ErrCodeNoAgentsAvailable, "All agents for this model are currently offline or unhealthy", domain.SourceRouter)
	case errors.Is(err, policy.ErrNoCapacity):
		return domain.NewRouterError(domain.ErrCodeNoCapacity, "All agents for this model are at maximum capacity", domain.SourceRouter)
	default:
		return domain.NewRouterError(domain.ErrCodeNoAgentsAvailable, err.Error(), domain.SourceRouter)
	}
}

// shortID returns the first 8 characters of a peer.ID string for log brevity.
func shortID(id peer.ID) string {
	s := id.String()
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
