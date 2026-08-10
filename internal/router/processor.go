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
	"sync"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/provider"

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
	// limiter enforces the per-tenant daily token budget on the output side.
	// AllowOutputTokens is called after the agent returns the completion token count.
	limiter auth.RateLimiter
	// peers drops dead libp2p connections to an agent on a connection-level forward
	// failure so the next attempt re-dials. Defaults to httpHost.StreamHost.Network().
	peers PeerCloser
	// evictCooldown / evictMu / lastEvict implement per-agent eviction throttling.
	evictCooldown time.Duration
	evictMu       sync.Mutex
	lastEvict     map[peer.ID]time.Time
	// forwardFn performs the actual forward; defaults to forwardToAgent.
	forwardFn ForwardFunc
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
	session := p.executor.NewSession(pending.Request.Model, capability)

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
			// All local policy steps exhausted — try the provider fallback if configured.
			// Provider fallback calls ForwardChat and only makes sense for LLM requests.
			if fp := session.FallbackProvider(); fp != nil && pending.Capability == domain.CapabilityLLM {
				if pending.IsExpired() {
					p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
					pending.Error <- domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request timed out before provider fallback could be attempted", domain.SourceRouter)
					return
				}
				if resp, provErr := p.tryProviderFallback(pending, fp.Engine, fp.Model); provErr == nil {
					p.metrics.RequestRouted("cloud", fp.Engine, fp.Model, pending.TenantID)
					p.metrics.PolicyProviderFallbackRouted(pending.Request.Model)
					p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
					pending.Response <- resp
					return
				} else {
					log.Warnf("Provider fallback (%s/%s) failed for request %s: %v", fp.Engine, fp.Model, pending.ID, provErr)
					// Record failure under the provider's own labels so the empty-label
					// time series is not polluted and provider failure rates are observable.
					p.metrics.RequestFailed("cloud", fp.Engine, fp.Model, pending.TenantID)
					p.metrics.PolicyExhausted(pending.Request.Model)
					p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
					pending.Error <- domain.NewRouterError(domain.ErrCodeBackendError, provErr.Error(), domain.SourceBackend)
					return
				}
			}
			session.EmitExhaustionLogs(pending.ID, pending.TenantID)
			p.metrics.RequestFailed("", "", pending.Request.Model, pending.TenantID)
			p.metrics.PolicyExhausted(pending.Request.Model)
			p.metrics.TenantRequestFailed(pending.TenantID, pending.KeyID, pending.DeploymentID, pending.Request.Model)
			pending.Error <- mapSelectorError(err)
			return
		}

		// Select may have blocked in the capacity wait queue while the request
		// deadline expired (e.g. queue woke on slot free, then advanced to a
		// fallback step that had capacity). The slot is already acquired; release
		// it cleanly before returning so the next waiter can use it immediately.
		if pending.IsExpired() {
			agent.DecrementLoad()
			p.executor.SignalCapacity(agent.Metadata.Model)
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

		resp, rttMs, fwdErr := p.forwardFn(ctx, agent, pending)
		// DecrementLoad was called inside forwardToAgent on all paths — wake one waiter.
		p.executor.SignalCapacity(agent.Metadata.Model)

		if fwdErr == nil {
			if stepName == "routing_policy" {
				p.metrics.PolicyPrimaryRouted(pending.Request.Model)
			} else {
				p.metrics.PolicyFallbackRouted(pending.Request.Model)
			}
			pending.Response <- resp
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
			// Token-limit rejections are purely router-side quota decisions: the agent
			// completed the request successfully (HTTP 200). Recording a failure would
			// degrade the agent's success-rate and consecutiveFails streak, potentially
			// triggering policy exclusion for a healthy agent. Use RecordSuccess with
			// zero token counts so SRTT and success-rate stay accurate.
			// All other non-retryable codes (context_length_exceeded, invalid_parameter,
			// etc.) are genuine agent-side failures and use RecordFailure as normal.
			if re.Code == domain.ErrCodeTokenLimitExceeded {
				p.counters.RecordSuccess(agent, 0, 0, rttMs)
			} else {
				p.counters.RecordFailure(agent, rttMs)
			}
			pending.Error <- re
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
		p.counters.RecordFailure(agent, rttMs)

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

// forwardToAgent forwards a request to an agent via libp2p HTTP and returns the response
// along with the measured RTT in milliseconds.
//
// RTT is 0 for pre-send failures (marshal, client creation) where no network round-trip
// occurred. For post-send failures (non-200, decode error, network error after dial) the
// real RTT is returned so the caller can pass it to RecordFailure and keep SRTT accurate.
//
// Slot ownership: DecrementLoad is called exactly once by this function on ALL paths —
// via a sync.Once defer on error paths, and explicitly before RecordSuccess on the
// success path. The caller must NEVER call DecrementLoad.
//
// NamespacedClient fetches the agent's /.well-known/libp2p document, resolves the path
// prefix for inferenceProtocolID, and prepends it to every outbound request URL — so
// the agent's handler receives the path stripped of the protocol prefix.
func (p *RequestProcessor) forwardToAgent(parentCtx context.Context, agent *domain.Agent, pending *domain.PendingRequest) (*domain.ChatResponse, float64, error) {
	// Release the slot exactly once. For early error paths that return before
	// explicit decrement, the defer handles it. For the success path, decrement
	// is called explicitly before recording metrics so GetLoad() reflects
	// the post-completion state — capacityUtil reports 0 when the agent goes idle.
	var once sync.Once
	decrement := func() { once.Do(agent.DecrementLoad) }
	defer decrement()

	region := agent.Metadata.Region
	engine := agent.Metadata.Engine
	model := agent.Metadata.Model

	agentID := shortID(agent.ID)
	log.Debugf("Sending request %s to agent %s", pending.ID, agentID)

	// Record the full peer ID before the network call so the handler can log
	// which agent was in-flight if the handler-side deadline fires first.
	pending.DispatchedAgentID.Store(agent.ID.String())

	if pending.Request == nil {
		return nil, 0, fmt.Errorf("missing request")
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

	// Embedding and reranking: simple synchronous forward — no streaming, no token accounting.
	if pending.Capability == domain.CapabilityEmbedding || pending.Capability == domain.CapabilityReranker {
		path := "/v1/embeddings"
		if pending.Capability == domain.CapabilityReranker {
			path = "/v1/rerank"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewReader(pending.Request.RawBytes))
		if err != nil {
			return nil, 0, fmt.Errorf("failed to build HTTP request: %w", err)
		}
		// Forward original client headers (e.g. Authorization, custom headers) then
		// enforce Content-Type and inject W3C trace context, same as the LLM path.
		if pending.Request.HttpHeaders != nil {
			req.Header = pending.Request.HttpHeaders
		}
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
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, rttMs, domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request deadline exceeded while contacting agent", domain.SourceRouter)
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
			var errResp domain.RouterErrorResponse
			if jsonErr := json.Unmarshal(rawBytes, &errResp); jsonErr == nil && errResp.Error != nil {
				return nil, rttMs, errResp.Error
			}
			// Fall back to the raw body (e.g. FastAPI {"detail":"..."}) so the
			// caller sees the actual backend error rather than a generic status code.
			msg := string(rawBytes)
			if msg == "" {
				msg = fmt.Sprintf("agent returned HTTP %d", resp.StatusCode)
			}
			return nil, rttMs, domain.NewRouterError(domain.ErrCodeBackendError, msg, domain.SourceBackend)
		}

		p.metrics.RequestRouted(region, engine, model, pending.TenantID)
		p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, model, 0, 0)
		decrement()
		p.counters.RecordSuccess(agent, 0, 0, rttMs)
		headers := resp.Header.Clone()
		headers.Del("Content-Length")
		return &domain.ChatResponse{
			RawBytes:    rawBytes,
			HttpHeaders: headers,
			ProcessedBy: agent.ID.String(),
		}, rttMs, nil
	}

	// Forward to the path the handler requested (e.g. "/v1/messages" for the
	// Anthropic passthrough); default to chat/completions for native LLM requests.
	forwardPath := pending.Path
	if forwardPath == "" {
		forwardPath = "/v1/chat/completions"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		forwardPath, bytes.NewReader(pending.Request.RawBytes))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build HTTP request: %w", err)
	}
	// apply original request headers (including any custom headers added by the openAI client library) so they are forwarded to the backend engine.
	if pending.Request.HttpHeaders != nil {
		req.Header = pending.Request.HttpHeaders
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
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, rttMs, domain.NewRouterError(domain.ErrCodeRequestTimeout, "Request deadline exceeded while contacting agent", domain.SourceRouter)
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
		// Parse only the first 4 KB for JSON: structured error bodies are always short.
		// Using the full (potentially truncated) slice would break JSON parsing silently
		// and degrade a structured context_length_exceeded into a retryable backend_error.
		jsonSlice := body
		if len(jsonSlice) > 4*1024 {
			jsonSlice = jsonSlice[:4*1024]
		}
		var errResp domain.RouterErrorResponse
		if jsonErr := json.Unmarshal(jsonSlice, &errResp); jsonErr == nil && errResp.Error != nil {
			return nil, rttMs, errResp.Error
		}
		// Fallback: agent returned a non-structured error body.
		msg := string(body)
		if msg == "" {
			msg = fmt.Sprintf("agent returned HTTP %d", resp.StatusCode)
		}
		return nil, rttMs, domain.NewRouterError(domain.ErrCodeBackendError, msg, domain.SourceBackend)
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
		go func() {
			defer cancel()
			defer pw.Close()
			defer resp.Body.Close()
			// Forward bytes to the client byte-exact while a copy feeds the token
			// meter (TeeReader never alters the forwarded stream).
			io.Copy(pw, io.TeeReader(resp.Body, meter)) //nolint:errcheck

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
			if p.limiter != nil && pending.TokensPerDay > 0 && completion > 0 {
				allowed, _, err := p.limiter.AllowOutputTokens(pending.TenantID, pending.QuotaModel, pending.TokensPerDay, completion)
				if err != nil {
					log.Warnf("Streaming output token accounting for %s: %v (fail-open)", pending.ID, err)
				} else if !allowed {
					// The stream is already delivered and cannot be rejected mid-flight,
					// but record the limit event so quota/abuse metrics stay accurate.
					p.metrics.TenantTokenLimited(pending.TenantID, pending.KeyID, pending.DeploymentID, "output")
				}
			}
			p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, model, prompt, completion)
			p.counters.RecordSuccess(agent, prompt, completion, rttMs)
		}()
		// RTT is time-to-first-byte (headers already received above). The
		// token-bearing metrics are recorded in the goroutine once the stream
		// completes; here we only mark the request as routed and release the slot.
		p.metrics.RequestRouted(region, engine, model, pending.TenantID)
		decrement()
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

	// Output token enforcement: deduct actual completion tokens from the daily budget.
	// This check happens after the agent has already processed the request.
	// If denied: discard the response and return 429.
	// Note: the estimated prompt tokens were already deducted by AllowInputTokens in
	// the handler (before the request was queued) and are NOT rolled back here.
	// Only the completion tokens (n) are withheld — AllowOutputTokens does not add n
	// to b.used when it returns allowed=false, but the prompt deduction stands.
	if p.limiter != nil && pending.TokensPerDay > 0 && chatResp.Usage.CompletionTokens > 0 {
		allowed, remaining, limErr := p.limiter.AllowOutputTokens(
			pending.TenantID, pending.QuotaModel, pending.TokensPerDay, chatResp.Usage.CompletionTokens)
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
			re := domain.NewRouterError(
				domain.ErrCodeTokenLimitExceeded, "daily token budget exceeded", domain.SourceRouter)
			re.Usage = &usageCopy
			re.AgentID = agent.ID.String()
			return nil, rttMs, re
		} else {
			pending.RemainingTokens = remaining
		}
	}

	p.metrics.RequestRouted(region, engine, model, pending.TenantID)
	p.metrics.TenantRequestSucceeded(pending.TenantID, pending.KeyID, pending.DeploymentID, model, chatResp.Usage.PromptTokens, chatResp.Usage.CompletionTokens)
	decrement()
	p.counters.RecordSuccess(agent, chatResp.Usage.PromptTokens, chatResp.Usage.CompletionTokens, rttMs)
	chatResp.ProcessedBy = agent.ID.String()
	log.Debugf("Received response for %s from agent %s", pending.ID, agentID)
	return &chatResp, rttMs, nil
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
