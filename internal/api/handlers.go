// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/storage"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// headerRateLimitRemainingTokens is the response header carrying the tenant's
// remaining daily token budget (TPD quota).
const headerRateLimitRemainingTokens = "X-RateLimit-Remaining-Tokens"

// headerRateLimitRemainingRequests is the response header carrying the
// tenant's remaining per-minute request budget (RPM quota).
const headerRateLimitRemainingRequests = "X-RateLimit-Remaining-Requests"

// AgentMetadataView holds immutable identity fields for an agent.
type AgentMetadataView struct {
	Model         string   `json:"model"`
	Engine        string   `json:"engine"`
	Region        string   `json:"region,omitempty"`
	Organization  string   `json:"organization,omitempty"`
	Machine       string   `json:"machine,omitempty"`
	Capacity      int      `json:"capacity"`
	Tags          []string `json:"tags,omitempty"`
	HideLLM       bool     `json:"hide_llm,omitempty"`
	LLMPrettyName string   `json:"llm_pretty_name,omitempty"`
	LLMInfo       string   `json:"llm_info,omitempty"`
	Capability    string   `json:"capability,omitempty"`
	GPUModel      string   `json:"gpu_model,omitempty"`
	// Opaque correlation IDs an agent may report about itself, echoed back so
	// an external orchestrator can join a routing-table row to whatever it
	// scheduled. Both are empty unless the agent was given them.
	DeploymentID string `json:"deployment_id,omitempty"`
	ReplicaID    string `json:"replica_id,omitempty"`
}

// AgentStatusView holds live connection and health state for an agent.
type AgentStatusView struct {
	Healthy             bool      `json:"healthy"`
	BackendHealthy      bool      `json:"backend_healthy"`
	ActiveRequests      int       `json:"active_requests"`
	CapacityUtilization float64   `json:"capacity_utilization"`
	LastSeen            time.Time `json:"last_seen"`
}

// AgentUniversalView holds engine-agnostic lifetime counters and latency
// (matching hivenet_router_agent_* Prometheus metrics).
type AgentUniversalView struct {
	SuccessfulRequestsTotal int64    `json:"successful_requests_total"`
	FailedRequestsTotal     int64    `json:"failed_requests_total"`
	SuccessRate             float64  `json:"success_rate"`
	InputTokensTotal        int64    `json:"input_tokens_total"`
	OutputTokensTotal       int64    `json:"output_tokens_total"`
	RejectedRequestsTotal   int64    `json:"rejected_requests_total"`
	DisconnectionsTotal     int64    `json:"disconnections_total"`
	AgentFailuresTotal      int64    `json:"agent_failures_total"`
	BackendFailuresTotal    int64    `json:"backend_failures_total"`
	SRTTMs                  *float64 `json:"srtt_ms,omitempty"`
	RTTVARMs                *float64 `json:"rttvar_ms,omitempty"`
	LatencyState            string   `json:"latency_state"` // "KNOWN" | "UNKNOWN"
}

// AgentEngineView holds real-time engine backend metrics
// (matching hivenet_router_agent_engine_* Prometheus metrics).
// Nil when no engine snapshot has been received yet (e.g. Ollama/custom engines).
type AgentEngineView struct {
	KVCacheUtilization *float64 `json:"kv_cache_utilization,omitempty"`
	RunningRequests    *float64 `json:"running_requests,omitempty"`
	WaitingRequests    *float64 `json:"waiting_requests,omitempty"`
	PreemptionsTotal   *float64 `json:"preemptions_total,omitempty"`
	AvgTTFTSeconds     *float64 `json:"avg_ttft_seconds,omitempty"`
	P90TTFTSeconds     *float64 `json:"p90_ttft_seconds,omitempty"`
	AvgITLSeconds      *float64 `json:"avg_itl_seconds,omitempty"`
	P90ITLSeconds      *float64 `json:"p90_itl_seconds,omitempty"`
}

// AgentHardwareView mirrors domain.HardwareSnapshot but uses time.Time for the
// timestamp so the API response is human-readable (RFC3339) instead of a raw
// Unix epoch integer.
type AgentHardwareView struct {
	GPUs      []domain.GPUMetric  `json:"gpu,omitempty"`
	CPU       domain.CPUMetric    `json:"cpu"`
	Memory    domain.MemoryMetric `json:"memory"`
	Timestamp time.Time           `json:"timestamp"`
}

// AgentView is the per-agent snapshot returned by GET /v1/agents.
// Fields are grouped into sections that mirror the Prometheus metric namespaces.
type AgentView struct {
	PeerID    string             `json:"peer_id"`
	Metadata  AgentMetadataView  `json:"metadata"`
	Status    AgentStatusView    `json:"status"`
	Universal AgentUniversalView `json:"universal"`
	// Engine is omitted entirely for agents that don't expose engine metrics.
	Engine *AgentEngineView `json:"engine,omitempty"`
	// Hardware is omitted until the first snapshot arrives (agent startup window).
	Hardware *AgentHardwareView `json:"hardware,omitempty"`
}

// MetricContext carries the per-request tenant identity labels passed to metric
// callbacks. Using a struct instead of positional strings prevents mis-ordering
// at call sites and lets future label additions extend the struct without
// changing every callback signature.
type MetricContext struct {
	TenantID     string
	KeyID        string
	DeploymentID string
	Model        string
	Phase        string // non-empty only for token-limited callbacks ("input", "output")
}

// RoutingTableProvider is implemented by the router and supplies a live
// snapshot of all registered agents for the GET /v1/agents endpoint.
type RoutingTableProvider interface {
	GetRoutingTable() []AgentView
}

// RegistrationFeed is implemented by the router and exposes a live SSE-friendly
// stream of agent-registration deltas. Each call to SubscribeRegistration
// returns a fresh channel + a cancel function the caller MUST invoke
// (typically as a deferred call once the HTTP request ends).
//
// The channel is buffered; events that overflow on a slow consumer are dropped
// rather than blocking the registry path. A dropped event is a temporary view
// gap, not a lost state: consumers are expected to reconcile against the
// routing table periodically, which repairs it.
type RegistrationFeed interface {
	SubscribeRegistration() (<-chan domain.RegistrationEvent, func())
}

// Handlers contains all dependencies required by HTTP handlers.
// This struct is injected into the API server and reused across requests.
type Handlers struct {

	// storage provides access to routing and agent state.
	// It is an interface, allowing different implementations (in-memory, DB, etc.).
	storage storage.RoutingStorage

	// executor holds the active routing policy and exposes GetPolicy/SetPolicy.
	// Used by the /admin/policy endpoints.
	executor *policy.Executor

	// routingTable provides a live snapshot of the routing table for GET /v1/agents.
	routingTable RoutingTableProvider

	// requestQueue is a buffered channel used to enqueue incoming requests.
	// Worker goroutines consume from this channel and process requests.
	requestQueue chan *domain.PendingRequest

	// timeout defines how long a request may run before failing.
	timeout time.Duration

	// providerValidator validates that any fallback_provider declared in a policy
	// has a configured API key. Called by PutPolicy before activating a new policy.
	// May be nil when no providers are configured.
	providerValidator func(*policy.Policy) error

	// policyReloadObserver is called after each policy reload attempt.
	// trigger is "api" or "sighup"; result is "success" or "error".
	// May be nil when metrics are not configured.
	policyReloadObserver func(trigger, result string)

	// limiter enforces per-tenant token budgets. The input check (AllowInputTokens)
	// runs here after the request body is parsed; the output check runs in the processor.
	// May be nil when auth is disabled.
	limiter auth.RateLimiter

	// onTokenLimited is called when AllowInputTokens denies a request.
	// Used to increment the Prometheus counters (token_limited_total and requests_failed_total)
	// without importing the metrics package directly.
	// May be nil.
	onTokenLimited func(MetricContext)

	// onRequestReceived is called at handler entry (after auth) with the tenant, key, and deployment IDs.
	// Used to update the last-request timestamp metric. May be nil.
	onRequestReceived func(MetricContext)

	// onRequestComplete is called when a successful agent response is returned.
	// Receives tenant context and end-to-end duration in seconds. May be nil.
	onRequestComplete func(MetricContext, float64)

	// onRequestDuration is called when a request completes (success path) with
	// tenant, agent, model, status code, and duration. May be nil.
	onRequestDuration func(tenantID, peerID, model, statusCode string, durationSeconds float64)

	// keyRegistry is the mutable in-memory API key registry (DynamicKeyProvider).
	// Nil when running in static-key or no-auth mode.
	keyRegistry *auth.DynamicKeyProvider

	// resetMetrics clears all persisted lifetime per-agent counters (disk, memory,
	// and Prometheus series) for POST /admin/metrics/reset. Wired to
	// UniversalCounterStore.ResetAll via a callback to avoid importing the metrics
	// package here. May be nil when no counter store is configured.
	resetMetrics func() error

	// healthyAgentCount reports the number of currently healthy agents serving
	// the given model. Used by per-model quota admission to compute the
	// effective per-minute ceiling as requests_per_minute_per_replica × replicas
	// and to detect the "no live backends" case (skip quota, let routing surface
	// the real error). Set by the router; nil-safe for tests that exercise the
	// legacy flat path.
	healthyAgentCount func(model string) int

	// registrationFeed is the live agent-registration change feed the SSE
	// handler on /admin/registration-stream consumes. Nil-safe: the handler
	// returns 501 when no feed is wired (e.g. tests).
	registrationFeed RegistrationFeed

	// admission enforces the per-model KV-occupancy admit budget before a request
	// is queued. Nil disables the gate (e.g. tests, or when no policy declares a
	// budget). Reservations it hands out are released on every request exit path.
	admission *admission.Controller

	// enginePressure returns the aggregate live engine KV-cache utilization and
	// waiting-request count for a model (each nil when no healthy agent reports
	// it), driving the front-door shed gate. Nil disables the gate (e.g. tests).
	enginePressure func(model string) (kvUtil, waiting *float64)

	// keyAdmission enforces the serverless per-key occupancy share — a per-key
	// token-weighted in-flight budget parallel to the global one. Nil disables it.
	keyAdmission *admission.Controller

	// minuteLimiter enforces the serverless per-key tokens-per-minute caps
	// (ITPM/OTPM). Nil disables them.
	minuteLimiter *auth.MinuteRateLimiter

	// onAdmissionReject is called when an admission gate rejects a request, with
	// the gate/reason (b1, b2, b3, b4_occupancy, b4_itpm, b4_otpm) and model.
	// Wired to the admission-rejections counter. Nil-safe.
	onAdmissionReject func(reason, model string)
}

// NewHandlers initializes a Handlers instance with all required dependencies.
// providerValidator, policyReloadObserver, limiter, onTokenLimited, keyRegistry,
// and healthyAgentCount may be nil (the per-model quota path falls back to the
// legacy flat shape when healthyAgentCount is nil).
func NewHandlers(
	storage storage.RoutingStorage,
	routingTable RoutingTableProvider,
	requestQueue chan *domain.PendingRequest,
	timeout time.Duration,
	executor *policy.Executor,
	providerValidator func(*policy.Policy) error,
	policyReloadObserver func(trigger, result string),
	limiter auth.RateLimiter,
	onTokenLimited func(MetricContext),
	onRequestReceived func(MetricContext),
	onRequestComplete func(MetricContext, float64),
	onRequestDuration func(tenantID, peerID, model, statusCode string, durationSeconds float64),
	keyRegistry *auth.DynamicKeyProvider,
	resetMetrics func() error,
	healthyAgentCount func(model string) int,
	registrationFeed RegistrationFeed,
	admissionController *admission.Controller,
	enginePressure func(model string) (kvUtil, waiting *float64),
	keyAdmission *admission.Controller,
	minuteLimiter *auth.MinuteRateLimiter,
	onAdmissionReject func(reason, model string),
) *Handlers {
	return &Handlers{
		storage:              storage,
		routingTable:         routingTable,
		requestQueue:         requestQueue,
		timeout:              timeout,
		executor:             executor,
		providerValidator:    providerValidator,
		policyReloadObserver: policyReloadObserver,
		limiter:              limiter,
		onTokenLimited:       onTokenLimited,
		onRequestReceived:    onRequestReceived,
		onRequestComplete:    onRequestComplete,
		onRequestDuration:    onRequestDuration,
		keyRegistry:          keyRegistry,
		resetMetrics:         resetMetrics,
		healthyAgentCount:    healthyAgentCount,
		registrationFeed:     registrationFeed,
		admission:            admissionController,
		enginePressure:       enginePressure,
		keyAdmission:         keyAdmission,
		minuteLimiter:        minuteLimiter,
		onAdmissionReject:    onAdmissionReject,
	}
}

// fireAdmissionReject reports an admission-gate rejection by reason and model.
func (h *Handlers) fireAdmissionReject(reason, model string) {
	if h.onAdmissionReject != nil {
		h.onAdmissionReject(reason, model)
	}
}

// Liveness is the public health probe — always returns 200 as long as the
// router process is running. Intended for load balancer health checks,
// Kubernetes liveness/readiness probes, and uptime monitors.
// It intentionally exposes no operational data.
func (h *Handlers) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// AdminHealth returns the full operational status of the router:
// agent count, healthy count, queue depth, and per-agent metadata.
// Protected by adminAuth middleware — registered under /admin/health.
func (h *Handlers) AdminHealth(c *gin.Context) {

	// Retrieve all registered agents from storage.
	agents, err := h.storage.ListAgents()
	if err != nil {
		log.Errorf("AdminHealth: failed to list agents: %v", err)
		writeRouterError(c, http.StatusInternalServerError, domain.ErrCodeBackendError, "internal server error", domain.SourceRouter)
		return
	}

	// Count healthy agents and build per-agent metadata list.
	healthyCount := 0
	agentList := make([]map[string]any, 0, len(agents))
	for _, agent := range agents {
		if agent.IsHealthy {
			healthyCount++
		}
		agentList = append(agentList, map[string]any{
			"peer_id":    agent.PeerID,
			"model":      agent.Model,
			"engine":     agent.Metadata.Engine,
			"version":    agent.Metadata.Version,
			"capacity":   agent.Capacity,
			"region":     agent.Region,
			"is_healthy": agent.IsHealthy,
			"last_seen":  agent.LastSeen.Unix(),
		})
	}

	queueLen := len(h.requestQueue)
	status := "healthy"
	if healthyCount == 0 {
		status = "degraded"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":         status,
		"total_agents":   len(agents),
		"healthy_agents": healthyCount,
		"queue_length":   queueLen,
		"timestamp":      time.Now().Unix(),
		"agents":         agentList,
	})
}

// Passthrough is the generic, allowlisted inference handler for the OpenAI
// (/v1/chat/completions) and Anthropic (/v1/messages) dialects. It routes by the
// top-level "model" field and forwards the ORIGINAL request bytes to a healthy
// "llm" agent at the SAME path, so the backend's native endpoint answers without
// schema translation. Token budgeting is exact for OpenAI bodies; for other
// dialects the input estimate is best-effort — it inspects only OpenAI message
// fields, so it may undercount (e.g. it does not see Anthropic's top-level
// "system" or non-text content blocks) — while output-token enforcement and
// per-tenant token metrics still apply via the processor.
func (h *Handlers) Passthrough(c *gin.Context) {
	m := reqMeta{start: time.Now()}

	// Allowlist gate — the public security boundary. Any path not explicitly
	// listed is rejected here and never reaches an agent. See passthroughAllowlist.
	path := c.Request.URL.Path
	if !passthroughAllowlist[path] {
		writeRouterError(c, http.StatusNotFound, domain.ErrCodeRequestInvalid, "unsupported endpoint", domain.SourceRouter)
		return
	}

	var req domain.ChatRequest
	if !h.parsePassthroughRequest(c, &req) {
		return
	}
	if !h.authorizeModel(c, req.Model) {
		return
	}
	if !h.enforceRequestCaps(c, &req) {
		return
	}
	if !h.enforceShedPressure(c, req.Model) {
		return
	}
	// Occupancy admit budget. The reservation is released on every exit path
	// below via this single defer — success, error, timeout, disconnect, and the
	// daily-budget reject that may follow — so a slot is never leaked.
	reservation, admitted := h.enforceOccupancyBudget(c, &req)
	if !admitted {
		return
	}
	defer reservation.Release()

	m.requestID = generateRequestID()
	m.tenantID, m.keyID, m.deploymentID = callerIDs(c)
	m.tpd, m.quotaModel = quotaBudgetFromContext(c)

	// Record last-seen timestamp for the tenant (every authenticated request).
	if h.onRequestReceived != nil {
		h.onRequestReceived(MetricContext{TenantID: m.tenantID, KeyID: m.keyID, DeploymentID: m.deploymentID})
	}

	// Per-key rate caps before the daily-budget charge: reserveInputBudget
	// deducts daily tokens, so charging it only after the cheaper per-minute
	// caps means a rate rejection never spends a tenant's daily budget.
	if !h.enforcePerKeyRates(c, &req, m) {
		return
	}
	if !h.reserveInputBudget(c, &req, m) {
		return
	}
	if !captureRawBody(c, &req, m.requestID) {
		return
	}

	pending := domain.NewPendingRequest(m.requestID, &req, h.timeout)
	pending.Ctx = c.Request.Context()
	pending.TenantID = m.tenantID
	pending.KeyID = m.keyID
	pending.DeploymentID = m.deploymentID
	pending.TokensPerDay = m.tpd
	pending.QuotaModel = m.quotaModel
	pending.Capability = domain.CapabilityLLM
	pending.Path = path // forward to the backend's native endpoint at this path
	if len(reservation) > 0 {
		pending.Reservation = reservation // processor grows it as undeclared output streams
	}

	if !h.enqueueRequest(c, pending, m.requestID, req.Model) {
		return
	}
	h.awaitResponse(c, pending, &req, m)
}

// reqMeta bundles the request-scoped identifiers and budget shared across the
// Passthrough pipeline so the helpers below avoid long parameter lists.
//
// quotaModel selects the limiter bucket: empty for legacy-flat keys (one
// bucket per tenant), non-empty for per-model keys (one bucket per
// (tenant, model)). Stashed by QuotaMiddleware so the handler does not redo
// the resolution.
type reqMeta struct {
	requestID    string
	tenantID     string
	keyID        string
	deploymentID string
	tpd          int
	quotaModel   string
	start        time.Time
}

// parsePassthroughRequest binds the JSON body, validates the model field, and
// records it for the audit log. Returns false (after writing the error) on any
// validation failure.
func (h *Handlers) parsePassthroughRequest(c *gin.Context, req *domain.ChatRequest) bool {
	// ShouldBindBodyWith allows body re-reading if middleware already cached it.
	if err := c.ShouldBindBodyWith(req, binding.JSON); err != nil {
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "Invalid request: "+err.Error(), domain.SourceRouter)
		return false
	}
	// Model must be non-empty and bounded to keep Prometheus label cardinality finite.
	if req.Model == "" {
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "model field is required", domain.SourceRouter)
		return false
	}
	if len(req.Model) > 256 {
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "model name exceeds maximum length of 256 characters", domain.SourceRouter)
		return false
	}
	// Set now so all later early returns carry the model in the audit log.
	c.Set(auditKeyModel, req.Model)
	return true
}

// authorizeModel enforces the per-key model allowlist (empty allowlist = all
// models). Returns false (after writing 403) when the model is not permitted.
func (h *Handlers) authorizeModel(c *gin.Context, model string) bool {
	allowSet, unrestricted := effectiveAllowedModels(c)
	if unrestricted {
		return true
	}
	if _, ok := allowSet[model]; ok {
		return true
	}
	writeRouterError(c, http.StatusForbidden, domain.ErrCodeModelForbidden, "your API key does not have access to model: "+model, domain.SourceRouter)
	return false
}

// enforceRequestCaps applies the per-request hard caps declared by the model's
// policy: the estimated text prompt must fit max_input_tokens and the image
// count must fit images_max. The two caps are complementary — the token cap
// bounds text, the image cap bounds image payloads (an image's token cost is
// invisible to the len/4 estimator, so counting images is how they are bounded;
// a textless image request therefore passes the token cap by design and is held
// only by images_max). Each caps the worst single request and returns a clean
// 400 input_too_long; aggregate load safety is a separate concern handled at
// admission, not here.
//
// The governing policy is resolved per model (named override, else the global
// policy). A cap of zero is "unset" and disables that check, so the gate is a
// no-op when both caps are zero, or when no policy/executor is configured
// (tests). This gate never inspects or clamps max_tokens: the router applies no
// output limit anywhere.
func (h *Handlers) enforceRequestCaps(c *gin.Context, req *domain.ChatRequest) bool {
	if h.executor == nil {
		return true
	}
	pol := h.executor.EffectivePolicy(req.Model)
	if pol == nil {
		return true
	}
	if pol.MaxInputTokens > 0 {
		inputTokens := domain.EstimateTokens(domain.GetMessageSlice(req.Messages))
		if inputTokens > pol.MaxInputTokens {
			h.fireAdmissionReject("b1", req.Model)
			writeRouterError(c, http.StatusBadRequest, domain.ErrCodeInputTooLong,
				fmt.Sprintf("input is %d tokens, over the model limit of %d", inputTokens, pol.MaxInputTokens),
				domain.SourceRouter)
			return false
		}
	}
	if pol.ImagesMax > 0 {
		images := domain.CountImages(req.Messages)
		if images > pol.ImagesMax {
			h.fireAdmissionReject("b1", req.Model)
			writeRouterError(c, http.StatusBadRequest, domain.ErrCodeInputTooLong,
				fmt.Sprintf("request carries %d images, over the model limit of %d", images, pol.ImagesMax),
				domain.SourceRouter)
			return false
		}
	}
	return true
}

// enforceOccupancyBudget applies the KV-occupancy admit budget: the request is
// admitted only while the model's weighted in-flight token sum plus this
// request's footprint stays within admit_fraction × admit_budget_tokens, and the
// in-flight request count stays within max_inflight. The footprint is the input
// estimate plus the declared max_tokens (reserved up front); an undeclared
// request reserves only its input and grows as output streams. Over budget, it
// parks briefly for capacity to free, then returns 429 concurrency_limit_exceeded
// with Retry-After.
//
// It returns the reservations (which the caller MUST release exactly once on
// every exit path) and whether the request was admitted. An empty slice with
// ok=true means the gate is inert for this model — nothing to release, nothing
// rejected. On a serverless replica it holds a second, per-key reservation for
// the key's occupancy share alongside the global budget.
func (h *Handlers) enforceOccupancyBudget(c *gin.Context, req *domain.ChatRequest) (admission.Reservations, bool) {
	var rs admission.Reservations
	if h.executor == nil {
		return rs, true
	}
	pol := h.executor.EffectivePolicy(req.Model)
	if pol == nil {
		return rs, true
	}
	declared := req.MaxCompletionTokens
	if declared == 0 {
		declared = req.MaxTokens
	}
	// Declared output is reserved up front; an undeclared request reserves only
	// its input and grows live (grows=true), matching how the processor meters it.
	footprint := estimatePromptTokens(req) + declared
	grows := declared == 0

	// Global occupancy budget (all replicas).
	if h.admission != nil && (pol.AdmitBudgetTokens > 0 || pol.MaxInflight > 0) {
		res := h.admission.Admit(c.Request.Context(), req.Model, footprint, grows, pol.AdmitBudgetTokens, pol.MaxInflight)
		if res == nil {
			h.fireAdmissionReject("b2", req.Model)
			c.Header("Retry-After", "1")
			writeRouterError(c, http.StatusTooManyRequests, domain.ErrCodeConcurrencyLimit,
				"server at capacity for this model, please retry", domain.SourceRouter)
			return nil, false
		}
		rs = append(rs, res)
	}

	// Per-key occupancy share (serverless replicas only): the key's in-flight
	// footprint must stay within max_occupancy_share × admit_budget_tokens. This
	// is anti-abuse fairness, not box safety (the global budget above is), so the
	// shares are intentionally oversubscribed and no admit fraction is applied.
	if pol.IsServerless() && pol.AdmitBudgetTokens > 0 && h.keyAdmission != nil {
		if share := quotaLimitsFromContext(c).MaxOccupancyShare; share > 0 {
			_, keyID, _ := callerIDs(c)
			budget := int(share * float64(pol.AdmitBudgetTokens))
			res := h.keyAdmission.Admit(c.Request.Context(), keyID+"\x00"+req.Model, footprint, grows, budget, 0)
			if res == nil {
				rs.Release() // hand back the global reservation already taken
				h.fireAdmissionReject("b4_occupancy", req.Model)
				c.Header("Retry-After", "1")
				writeRouterError(c, http.StatusTooManyRequests, domain.ErrCodeRateLimitExceeded,
					"per-key occupancy share exceeded, please retry", domain.SourceRouter)
				return nil, false
			}
			rs = append(rs, res)
		}
	}
	return rs, true
}

// quotaLimitsFromContext returns the calling key's resolved quota limits, or a
// zero value when none were stashed (no auth). The serverless per-key caps live
// on the flat fields; a per-model key leaves them zero, so B4 is inert for it.
func quotaLimitsFromContext(c *gin.Context) auth.QuotaLimits {
	if raw, ok := c.Get("quota_limits"); ok {
		if limits, ok := raw.(auth.QuotaLimits); ok {
			return limits
		}
	}
	return auth.QuotaLimits{}
}

// enforcePerKeyRates applies the serverless per-key token-per-minute caps: it
// charges the request's estimated input tokens against the key's ITPM bucket and
// rejects when the key's OTPM bucket is already drained by recent output. Both
// deny with 429 rate_limit_exceeded + Retry-After. Inert unless the request
// lands on a serverless policy and the key declares the cap. Returns false after
// writing the 429.
func (h *Handlers) enforcePerKeyRates(c *gin.Context, req *domain.ChatRequest, m reqMeta) bool {
	if h.minuteLimiter == nil || h.executor == nil {
		return true
	}
	pol := h.executor.EffectivePolicy(req.Model)
	if pol == nil || !pol.IsServerless() {
		return true
	}
	limits := quotaLimitsFromContext(c)
	// Check the non-deducting OTPM gate before charging the ITPM bucket, so an
	// output-rate rejection never spends input-rate tokens.
	if limits.OutputTokensPerMinute > 0 && h.minuteLimiter.OutputExhausted(m.keyID, req.Model, limits.OutputTokensPerMinute) {
		h.fireAdmissionReject("b4_otpm", req.Model)
		return h.writeRateLimited(c, m, req.Model, "output token rate exceeded, please retry")
	}
	if limits.InputTokensPerMinute > 0 {
		if !h.minuteLimiter.AllowInputTokens(m.keyID, req.Model, limits.InputTokensPerMinute, estimatePromptTokens(req)) {
			h.fireAdmissionReject("b4_itpm", req.Model)
			return h.writeRateLimited(c, m, req.Model, "input token rate exceeded, please retry")
		}
	}
	return true
}

// chargeOutputRate meters completion tokens against the key's OTPM bucket after
// a serverless response, so a burst of output throttles the key's next requests.
// Best-effort and post-hoc: the response is already delivered. No-op unless the
// request is serverless and the key declares an OTPM cap.
func (h *Handlers) chargeOutputRate(c *gin.Context, req *domain.ChatRequest, m reqMeta, completionTokens int) {
	if h.minuteLimiter == nil || h.executor == nil || completionTokens <= 0 {
		return
	}
	pol := h.executor.EffectivePolicy(req.Model)
	if pol == nil || !pol.IsServerless() {
		return
	}
	if otpm := quotaLimitsFromContext(c).OutputTokensPerMinute; otpm > 0 {
		h.minuteLimiter.ChargeOutputTokens(m.keyID, req.Model, otpm, completionTokens)
	}
}

// writeRateLimited emits a 429 rate_limit_exceeded with Retry-After for a per-key
// cap breach, firing the token-limited metric callback. Always returns false.
func (h *Handlers) writeRateLimited(c *gin.Context, m reqMeta, model, msg string) bool {
	h.fireTokenLimited(m, model)
	c.Header("Retry-After", "1")
	writeRouterError(c, http.StatusTooManyRequests, domain.ErrCodeRateLimitExceeded, msg, domain.SourceRouter)
	return false
}

// enforceShedPressure is the front-door KV-pressure shed: when the live engine
// pressure for the model breaches the policy's shed_if thresholds, new requests
// are rejected with 429 + Retry-After at the door rather than queued into an
// already-saturated pool. It reads the aggregate engine signal (mean KV-cache
// utilization and waiting requests across healthy replicas) and evaluates it
// with the same threshold logic and nil-passes contract routing uses for its
// per-agent exclude_if gates — a missing signal (non-vLLM engine, no snapshot
// yet) passes, so the gate fails open. This is additive: the per-agent exclude_if
// routing gates are unchanged; this adds the clean front-door reject on top.
//
// Returns false (after writing 429) when the pool is shedding.
func (h *Handlers) enforceShedPressure(c *gin.Context, model string) bool {
	if h.enginePressure == nil || h.executor == nil {
		return true
	}
	pol := h.executor.EffectivePolicy(model)
	if pol == nil || len(pol.ShedIf) == 0 {
		return true // no shed thresholds declared — gate inert
	}
	kvUtil, waiting := h.enginePressure(model)
	if kvUtil == nil && waiting == nil {
		return true // no live engine signal — fail open
	}
	snap := policy.AgentSnapshot{KVCacheUtilization: kvUtil, WaitingRequests: waiting}
	if policy.PassesGates(snap, pol.ShedIf) {
		return true
	}
	h.fireAdmissionReject("b3", model)
	c.Header("Retry-After", "1")
	writeRouterError(c, http.StatusTooManyRequests, domain.ErrCodeConcurrencyLimit,
		"model is under high load, please retry", domain.SourceRouter)
	return false
}

// estimatePromptTokens returns the input-token estimate used by both admission
// gates: the text estimate, floored at a per-message overhead so a multimodal or
// tool-only message (no estimable text) is still counted rather than measured as
// zero.
func estimatePromptTokens(req *domain.ChatRequest) int {
	promptTokens := domain.EstimateTokens(domain.GetMessageSlice(req.Messages))
	if floor := len(req.Messages) * perMessageTokenOverhead; promptTokens < floor {
		promptTokens = floor
	}
	return promptTokens
}

// effectiveAllowedModels returns the set of models the calling API key may use.
// The set is derived in this precedence order:
//
//  1. quota.per_model (strict per-model quotas): allow-set = the map's keys.
//     The per_model declaration IS the allow-list — a caller that has no quota
//     entry for a model cannot call it (admission would 429 anyway), so the
//     discovery view must hide it too. This keeps a single source of truth.
//
//  2. allowed_models (legacy explicit allowlist on the auth.yaml key entry):
//     allow-set = that list, when non-empty.
//
//  3. Neither set → unrestricted=true. Legacy flat keys with no allowlist see
//     the full catalog, matching the pre-HAI-232 behaviour.
//
// Used by /v1/models and /v1/models/:model to filter the discovery surface so
// callers only see models they can actually invoke (and don't learn the names
// of other tenants' models). Also used by authorizeModel to enforce the same
// rule on inference verbs.
func effectiveAllowedModels(c *gin.Context) (allowSet map[string]struct{}, unrestricted bool) {
	if raw, ok := c.Get("quota_limits"); ok {
		if limits, ok := raw.(auth.QuotaLimits); ok && limits.PerModel != nil {
			allowSet = make(map[string]struct{}, len(limits.PerModel))
			for model := range limits.PerModel {
				allowSet[model] = struct{}{}
			}
			return allowSet, false
		}
	}
	if raw, ok := c.Get("allowed_models"); ok {
		if list, ok := raw.([]string); ok && len(list) > 0 {
			allowSet = make(map[string]struct{}, len(list))
			for _, m := range list {
				allowSet[m] = struct{}{}
			}
			return allowSet, false
		}
	}
	return nil, true
}

// quotaBudgetFromContext returns the daily token budget and the limiter-bucket
// model for this request, both stashed by QuotaMiddleware.
//
//   - Per-model keys: QuotaMiddleware sets effectiveTPDContextKey and
//     quotaModelContextKey to the resolved entry — used here verbatim so the
//     handler charges the same (tenant, model) bucket admission used.
//   - Legacy flat keys: neither context key is set; fall back to
//     QuotaLimits.TokensPerDay and an empty model (legacy tenant-only bucket).
//
// Returns (0, "") when no quota is configured.
func quotaBudgetFromContext(c *gin.Context) (tpd int, model string) {
	if raw, ok := c.Get(effectiveTPDContextKey); ok {
		if v, ok := raw.(int); ok {
			tpd = v
		}
	}
	if raw, ok := c.Get(quotaModelContextKey); ok {
		if v, ok := raw.(string); ok {
			model = v
		}
	}
	if model == "" {
		// Legacy flat shape — read the per-tenant TPD from the umbrella limits.
		if raw, ok := c.Get("quota_limits"); ok {
			if limits, ok := raw.(auth.QuotaLimits); ok && limits.PerModel == nil {
				tpd = limits.TokensPerDay
			}
		}
	}
	return tpd, model
}

// perMessageTokenOverhead is a conservative per-message floor (role + formatting
// tokens) applied to the prompt estimate when a message has no estimable text
// (e.g. image/tool-only multimodal content), so such requests still pass through
// the token-budget admission gate rather than bypassing it with a 0 estimate.
const perMessageTokenOverhead = 4

// reserveInputBudget enforces the daily token budget BEFORE queueing. It is a
// pure admission check: it peeks the remaining budget WITHOUT charging and rejects
// the request up front (clean 429) if its worst case — estimated prompt tokens
// plus the requested max output — cannot fit, so a rejected request consumes no
// budget. Only when the request is admitted is the prompt estimate charged; the
// processor deducts the actual completion tokens after the response (max_tokens is
// never deducted, only used to size the admission check). Returns false (after
// writing 429) when the request cannot be admitted.
func (h *Handlers) reserveInputBudget(c *gin.Context, req *domain.ChatRequest, m reqMeta) bool {
	if h.limiter == nil || m.tpd <= 0 {
		return true
	}
	// EstimateTokens only sees text content; estimatePromptTokens floors a
	// multimodal or tool-only message at a per-message overhead so it is charged
	// (and passes through this gate) rather than bypassing it with a 0 estimate.
	promptTokens := estimatePromptTokens(req)
	if promptTokens == 0 {
		return true // genuinely empty body — nothing to budget
	}
	// Worst-case output. max_completion_tokens is the 2026 field; fall back to
	// max_tokens (also the Anthropic field name).
	reservedOutput := req.MaxCompletionTokens
	if reservedOutput == 0 {
		reservedOutput = req.MaxTokens
	}

	// Admission check — peek, do not charge. If we cannot read the budget, fail open.
	remaining, err := h.limiter.RemainingTokens(m.tenantID, m.quotaModel, m.tpd)
	if err != nil {
		log.Warnf("Remaining-budget check error for request %s: %v (fail-open)", m.requestID, err)
		return true
	}
	if remaining >= 0 && promptTokens+reservedOutput > remaining {
		h.fireTokenLimited(m, req.Model)
		c.Header(headerRateLimitRemainingTokens, strconv.Itoa(remaining))
		msg := "daily token budget exceeded"
		if reservedOutput > 0 {
			msg = "insufficient daily token budget for requested max_tokens"
		}
		writeRouterError(c, http.StatusTooManyRequests, domain.ErrCodeTokenLimitExceeded, msg, domain.SourceRouter)
		return false
	}

	// Admitted — charge the prompt estimate now (the processor charges the actual
	// completion after the response). A concurrent request may have consumed the
	// budget between the peek and here, so honour a late deduction failure.
	allowed, remainingAfter, err := h.limiter.AllowInputTokens(m.tenantID, m.quotaModel, m.tpd, promptTokens)
	if err != nil {
		log.Warnf("Input token check error for request %s: %v (fail-open)", m.requestID, err)
	}
	if !allowed {
		h.fireTokenLimited(m, req.Model)
		c.Header(headerRateLimitRemainingTokens, "0")
		writeRouterError(c, http.StatusTooManyRequests, domain.ErrCodeTokenLimitExceeded, "daily token budget exceeded", domain.SourceRouter)
		return false
	}
	if remainingAfter >= 0 {
		c.Header(headerRateLimitRemainingTokens, strconv.Itoa(remainingAfter))
	}
	return true
}

// fireTokenLimited reports an input-phase token-limit rejection, if a callback is set.
func (h *Handlers) fireTokenLimited(m reqMeta, model string) {
	if h.onTokenLimited != nil {
		h.onTokenLimited(MetricContext{TenantID: m.tenantID, KeyID: m.keyID, DeploymentID: m.deploymentID, Model: model, Phase: "input"})
	}
}

// captureRawBody stores the original headers and cached body bytes on req so the
// processor can forward them verbatim. Returns false (after writing the error)
// when the cached body is missing or malformed.
func captureRawBody(c *gin.Context, req *domain.ChatRequest, requestID string) bool {
	// Preserve any custom headers added by the client library.
	req.HttpHeaders = c.Request.Header.Clone()

	cb, ok := c.Get(gin.BodyBytesKey)
	if !ok {
		log.Errorf("Failed to get cached request body for request %s", requestID)
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeInvalidParameter, "request body is nil", domain.SourceBackend)
		return false
	}
	rawBytes, ok := cb.([]byte)
	if !ok {
		log.Errorf("Unexpected type for cached body in request %s", requestID)
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeInvalidParameter, "request body is nil", domain.SourceBackend)
		return false
	}
	req.RawBytes = rawBytes
	if req.HttpHeaders != nil {
		req.HttpHeaders.Del("Content-Length") // forward path sets the correct length
	}
	return true
}

// enqueueRequest submits pending to the worker queue. Returns false (after
// writing 503) if the queue stays full past the enqueue deadline.
func (h *Handlers) enqueueRequest(c *gin.Context, pending *domain.PendingRequest, requestID, model string) bool {
	select {
	case h.requestQueue <- pending:
		log.Infof("Request queued (ID: %s, Model: %s)", requestID, model)
		return true
	case <-time.After(5 * time.Second):
		writeRouterError(c, http.StatusServiceUnavailable, domain.ErrCodeQueueFull, "Request queue is full, please retry", domain.SourceRouter)
		return false
	}
}

// awaitResponse blocks on the worker result and dispatches to the success,
// error, or timeout writer.
func (h *Handlers) awaitResponse(c *gin.Context, pending *domain.PendingRequest, req *domain.ChatRequest, m reqMeta) {
	select {
	case resp := <-pending.Response:
		h.writeSuccessResponse(c, pending, req, m, resp)
	case err := <-pending.Error:
		writeErrorResponse(c, pending, err)
	case <-time.After(h.timeout):
		// No response — nothing billed. Agent ID is logged when already dispatched.
		c.Set(auditKeyInputTokens, int64(0))
		c.Set(auditKeyOutputTokens, int64(0))
		setDispatchedAgentAudit(c, pending)
		writeRouterError(c, http.StatusGatewayTimeout, domain.ErrCodeRequestTimeout, "Request timeout", domain.SourceRouter)
	}
}

// writeSuccessResponse streams or writes the agent response and records success
// metrics and audit usage.
func (h *Handlers) writeSuccessResponse(c *gin.Context, pending *domain.PendingRequest, req *domain.ChatRequest, m reqMeta, resp *domain.ChatResponse) {
	// Fall back to local estimation only if the engine omitted usage.
	if resp.Usage.TotalTokens == 0 && len(resp.Choices) > 0 && resp.Choices[0].Message != nil {
		promptTokens := domain.EstimateTokens(domain.GetMessageSlice(req.Messages))
		completionTokens := domain.EstimateTokens([]string{resp.Choices[0].Message.Content})
		resp.Usage = domain.Usage{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      promptTokens + completionTokens,
		}
	}
	// RemainingTokens == -1 means the processor enforced no limit — leave the
	// header alone; 0 is valid (budget exactly exhausted).
	if pending.RemainingTokens >= 0 {
		c.Header(headerRateLimitRemainingTokens, strconv.Itoa(pending.RemainingTokens))
	}
	if h.onRequestComplete != nil {
		h.onRequestComplete(MetricContext{TenantID: m.tenantID, KeyID: m.keyID, DeploymentID: pending.DeploymentID, Model: req.Model}, time.Since(m.start).Seconds())
	}
	if h.onRequestDuration != nil {
		h.onRequestDuration(m.tenantID, resp.ProcessedBy, req.Model, strconv.Itoa(http.StatusOK), time.Since(m.start).Seconds())
	}
	c.Set(auditKeyInputTokens, int64(resp.Usage.PromptTokens))
	c.Set(auditKeyOutputTokens, int64(resp.Usage.CompletionTokens))
	c.Set(auditKeyAgentID, resp.ProcessedBy)
	domain.CopyHttpHeaders(c.Writer.Header(), resp.HttpHeaders)
	c.Status(http.StatusOK)

	if resp.Body == nil {
		c.Writer.Write(resp.RawBytes) //nolint:errcheck
		h.chargeOutputRate(c, req, m, resp.Usage.CompletionTokens)
		return
	}
	// Streaming: flush SSE chunks as they arrive so tokens appear progressively.
	defer resp.Body.Close()
	dst := io.Writer(c.Writer)
	if flusher, ok := c.Writer.(http.Flusher); ok {
		dst = &sseFlushingWriter{ResponseWriter: c.Writer, flusher: flusher}
	}
	io.Copy(dst, resp.Body) //nolint:errcheck

	// Streaming-only: overwrite the audit token counts with the real totals the
	// processor's meter goroutine populated. They were 0 above because resp.Usage
	// is zero for streaming (the body had not been read yet at the time of the
	// earlier c.Set). io.Copy has now returned (pw.Close fired), which is also
	// when the goroutine finished writing these — so the values are visible.
	if pt := pending.StreamedPromptTokens.Load(); pt > 0 {
		c.Set(auditKeyInputTokens, pt)
	}
	if ct := pending.StreamedCompletionTokens.Load(); ct > 0 {
		c.Set(auditKeyOutputTokens, ct)
	}
	h.chargeOutputRate(c, req, m, int(pending.StreamedCompletionTokens.Load()))
}

// writeErrorResponse writes a worker error, carrying real usage/agent into the
// audit log when the error reports them (e.g. a post-inference token limit).
func writeErrorResponse(c *gin.Context, pending *domain.PendingRequest, err error) {
	// Defaults: no inference, no agent — overridden below if the error carries usage.
	c.Set(auditKeyInputTokens, int64(0))
	c.Set(auditKeyOutputTokens, int64(0))
	setDispatchedAgentAudit(c, pending)

	var re *domain.RouterError
	if !errors.As(err, &re) {
		c.Set(auditKeyErrorCode, string(domain.ErrCodeBackendError))
		setRootSpanError(c, err.Error())
		writeRouterError(c, http.StatusInternalServerError, domain.ErrCodeBackendError, err.Error(), domain.SourceBackend)
		return
	}
	if re.Usage != nil {
		c.Set(auditKeyInputTokens, int64(re.Usage.PromptTokens))
		c.Set(auditKeyOutputTokens, int64(re.Usage.CompletionTokens))
	}
	if re.AgentID != "" {
		c.Set(auditKeyAgentID, re.AgentID)
	}
	if re.Code == domain.ErrCodeTokenLimitExceeded {
		c.Header(headerRateLimitRemainingTokens, "0")
	}
	c.Set(auditKeyErrorCode, string(re.Code))
	setRootSpanError(c, re.Message)
	c.JSON(domain.HTTPStatusFor(re.Code), domain.RouterErrorResponse{Error: re})
}

// setDispatchedAgentAudit records the in-flight agent ID (if any) for the audit log.
func setDispatchedAgentAudit(c *gin.Context, pending *domain.PendingRequest) {
	if v := pending.DispatchedAgentID.Load(); v != nil {
		c.Set(auditKeyAgentID, v.(string))
	} else {
		c.Set(auditKeyAgentID, "")
	}
}

// ListModels returns all models currently served by registered agents,
// aggregated into an OpenAI-compatible GET /v1/models response.
func (h *Handlers) ListModels(c *gin.Context) {
	h.writeModelList(c, true)
}

// AdminListModels handles GET /admin/models — operator view of every model
// currently served by registered agents, identical shape to /v1/models but
// **unfiltered**. The admin auth chain gates this; the per-key allow-set is
// intentionally bypassed so operators always see ground truth (debugging,
// capacity planning, support tooling). See /v1/models for the tenant view.
func (h *Handlers) AdminListModels(c *gin.Context) {
	h.writeModelList(c, false)
}

// writeModelList is the shared body for ListModels and AdminListModels.
// applyAllowSet=true filters the response by the caller's effective allow-set
// (per-model quota keys → keys of per_model; legacy keys → allowed_models;
// otherwise unrestricted). applyAllowSet=false skips the filter entirely and
// is reserved for the admin path.
func (h *Handlers) writeModelList(c *gin.Context, applyAllowSet bool) {
	agents, err := h.storage.ListAgents()
	if err != nil {
		log.Errorf("writeModelList: failed to list agents: %v", err)
		writeRouterError(c, http.StatusInternalServerError, domain.ErrCodeBackendError, "internal server error", domain.SourceRouter)
		return
	}

	models := buildModelMap(agents)
	allowSet, unrestricted := effectiveAllowedModels(c)

	data := make([]domain.ModelObject, 0, len(models))
	for id, m := range models {
		if applyAllowSet && !unrestricted {
			if _, ok := allowSet[id]; !ok {
				continue
			}
		}
		data = append(data, m)
	}

	c.JSON(http.StatusOK, gin.H{
		"object": "list",
		"data":   data,
	})
}

// GetModel returns the detail for a single model identified by :model.
// Returns 404 if the model is not visible to the caller — either because no
// agent is registered for it, or because the caller's API key has no quota
// declared / explicit allowlist entry for it. The 404-vs-403 choice is
// deliberate: returning 403 would confirm the model exists to a caller who
// can't use it, leaking the cluster's model catalog across tenants.
func (h *Handlers) GetModel(c *gin.Context) {
	h.writeModelDetail(c, true)
}

// AdminGetModel handles GET /admin/models/:model — same shape as /v1/models/:model
// but skips the per-key allow-set check. Admin-only.
func (h *Handlers) AdminGetModel(c *gin.Context) {
	h.writeModelDetail(c, false)
}

// writeModelDetail is the shared body for GetModel and AdminGetModel.
// applyAllowSet=true returns 404 (not 403) for models the caller cannot use,
// preserving the "don't leak the catalog" guarantee.
func (h *Handlers) writeModelDetail(c *gin.Context, applyAllowSet bool) {
	// Gin's catch-all (*model) always prefixes with "/"; strip it.
	modelID := strings.TrimPrefix(c.Param("model"), "/")

	if applyAllowSet {
		allowSet, unrestricted := effectiveAllowedModels(c)
		if !unrestricted {
			if _, ok := allowSet[modelID]; !ok {
				writeRouterError(c, http.StatusNotFound, domain.ErrCodeModelNotFound, "No agents registered for model: "+modelID, domain.SourceRouter)
				return
			}
		}
	}

	agents, err := h.storage.ListAgents()
	if err != nil {
		log.Errorf("writeModelDetail: failed to list agents: %v", err)
		writeRouterError(c, http.StatusInternalServerError, domain.ErrCodeBackendError, "internal server error", domain.SourceRouter)
		return
	}

	models := buildModelMap(agents)
	m, ok := models[modelID]
	if !ok {
		writeRouterError(c, http.StatusNotFound, domain.ErrCodeModelNotFound, "No agents registered for model: "+modelID, domain.SourceRouter)
		return
	}

	// Populate the per-agent list for the detail view (all agents included).
	for _, a := range agents {
		if a.Model != modelID {
			continue
		}
		m.Agents.List = append(m.Agents.List, domain.ModelAgentEntry{
			PeerID:       a.PeerID,
			Engine:       a.Metadata.Engine,
			Version:      a.Metadata.Version,
			Region:       a.Metadata.Region,
			Organization: a.Metadata.Organization,
			Machine:      a.Metadata.Machine,
			Capacity:     a.Capacity,
			IsHealthy:    a.IsHealthy,
			LastSeen:     a.LastSeen.Unix(),
		})
	}

	c.JSON(http.StatusOK, m)
}

// buildModelMap aggregates a flat agent list into a map keyed by model name.
// All agents are included; the hide_llm flag is set based on agent metadata.
// The returned ModelObject has Agents.List unset (populated only by GetModel).
func buildModelMap(agents []*domain.AgentRegistration) map[string]domain.ModelObject {
	type seen struct {
		engines map[string]struct{}
		regions map[string]struct{}
	}
	meta := make(map[string]*seen)
	objs := make(map[string]domain.ModelObject)

	for _, a := range agents {
		m, exists := objs[a.Model]
		if !exists {
			m = domain.ModelObject{
				ID:     a.Model,
				Object: "model",
				Agents: domain.ModelAgents{},
			}
			meta[a.Model] = &seen{
				engines: make(map[string]struct{}),
				regions: make(map[string]struct{}),
			}
		}
		// Mark the model as hidden if any agent serving it has HideLLM=true.
		if a.Metadata.HideLLM {
			m.HideLLM = true
		}
		// Take the first non-empty pretty name / info / capability across agents for this model.
		// Assumption: all agents sharing a model name serve the same capability. If two agents
		// register the same model name with different capabilities (e.g. one "llm" and one
		// "embedding"), only the first capability seen will appear in the /v1/models listing.
		// Use distinct model names to serve the same weights with different capabilities.
		if m.PrettyName == "" && a.Metadata.LLMPrettyName != "" {
			m.PrettyName = a.Metadata.LLMPrettyName
		}
		if m.Info == "" && a.Metadata.LLMInfo != "" {
			m.Info = a.Metadata.LLMInfo
		}
		if m.Capability == "" && a.Metadata.Capability != "" {
			m.Capability = a.Metadata.Capability
		}
		m.Agents.Total++
		if a.IsHealthy {
			m.Agents.Healthy++
		}
		m.Agents.TotalCapacity += a.Capacity
		if a.Metadata.Engine != "" {
			meta[a.Model].engines[a.Metadata.Engine] = struct{}{}
		}
		if a.Metadata.Region != "" {
			meta[a.Model].regions[a.Metadata.Region] = struct{}{}
		}
		objs[a.Model] = m
	}

	// Convert sets to sorted slices.
	for modelID, obj := range objs {
		for e := range meta[modelID].engines {
			obj.Agents.Engines = append(obj.Agents.Engines, e)
		}
		for r := range meta[modelID].regions {
			obj.Agents.Regions = append(obj.Agents.Regions, r)
		}
		if obj.Agents.Engines == nil {
			obj.Agents.Engines = []string{}
		}
		if obj.Agents.Regions == nil {
			obj.Agents.Regions = []string{}
		}
		objs[modelID] = obj
	}

	return objs
}

// Embeddings handles POST /v1/embeddings — routes the request through the policy
// queue to a healthy embedding agent, applying the same policy pipeline as LLM requests.
func (h *Handlers) Embeddings(c *gin.Context) {
	h.enqueueInferenceRequest(c, domain.CapabilityEmbedding)
}

// Rerank handles POST /v1/rerank — routes the request through the policy queue
// to a healthy reranking agent, applying the same policy pipeline as LLM requests.
func (h *Handlers) Rerank(c *gin.Context) {
	h.enqueueInferenceRequest(c, domain.CapabilityReranker)
}

// enqueueInferenceRequest is the shared handler for embedding and reranking requests.
// It parses the model field, enqueues a PendingRequest with the given capability,
// and waits for the processor to return the raw response bytes from the agent.
func (h *Handlers) enqueueInferenceRequest(c *gin.Context, capability string) {
	// Parse the model field — QuotaMiddleware already cached the body via ShouldBindBodyWith.
	var req struct {
		Model string `json:"model"`
	}
	if err := c.ShouldBindBodyWith(&req, binding.JSON); err != nil || req.Model == "" {
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "model field is required", domain.SourceRouter)
		return
	}
	if len(req.Model) > 256 {
		writeRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "model name exceeds maximum length of 256 characters", domain.SourceRouter)
		return
	}

	// Set audit model key now so all subsequent early returns carry the model in the audit log.
	c.Set(auditKeyModel, req.Model)

	// Check model access: if the authenticated key has a model allowlist, verify
	// the requested model is on it. Empty allowlist = access to all models.
	if raw, exists := c.Get("allowed_models"); exists {
		if allowedModels, ok := raw.([]string); ok && len(allowedModels) > 0 {
			permitted := false
			for _, m := range allowedModels {
				if m == req.Model {
					permitted = true
					break
				}
			}
			if !permitted {
				writeRouterError(c, http.StatusForbidden, domain.ErrCodeModelForbidden, "your API key does not have access to model: "+req.Model, domain.SourceRouter)
				return
			}
		}
	}

	// Get the raw body cached by QuotaMiddleware.
	rawBody, _ := c.Get(gin.BodyBytesKey)
	rawBytes, _ := rawBody.([]byte)

	tenantID, keyID, deploymentID := callerIDs(c)

	if h.onRequestReceived != nil {
		h.onRequestReceived(MetricContext{TenantID: tenantID, KeyID: keyID, DeploymentID: deploymentID})
	}

	chatReq := &domain.ChatRequest{
		Model:    req.Model,
		RawBytes: rawBytes,
	}
	requestID := generateRequestID()
	pending := domain.NewPendingRequest(requestID, chatReq, h.timeout)
	pending.Ctx = c.Request.Context()
	pending.TenantID = tenantID
	pending.KeyID = keyID
	pending.DeploymentID = deploymentID
	pending.Capability = capability

	select {
	case h.requestQueue <- pending:
	case <-time.After(5 * time.Second):
		writeRouterError(c, http.StatusServiceUnavailable, domain.ErrCodeQueueFull, "Request queue is full, please retry", domain.SourceRouter)
		return
	}

	select {
	case resp := <-pending.Response:
		c.Set(auditKeyAgentID, resp.ProcessedBy)
		domain.CopyHttpHeaders(c.Writer.Header(), resp.HttpHeaders)
		c.Data(http.StatusOK, "application/json", resp.RawBytes)

	case err := <-pending.Error:
		var re *domain.RouterError
		if errors.As(err, &re) {
			c.Set(auditKeyErrorCode, string(re.Code))
			setRootSpanError(c, re.Message)
			c.JSON(domain.HTTPStatusFor(re.Code), domain.RouterErrorResponse{Error: re})
		} else {
			c.Set(auditKeyErrorCode, string(domain.ErrCodeBackendError))
			setRootSpanError(c, err.Error())
			writeRouterError(c, http.StatusInternalServerError, domain.ErrCodeBackendError, err.Error(), domain.SourceBackend)
		}

	case <-time.After(h.timeout):
		writeRouterError(c, http.StatusGatewayTimeout, domain.ErrCodeRequestTimeout, "Request timeout", domain.SourceRouter)
	}
}

// passthroughAllowlist is the set of HTTP paths the router forwards to a backend
// agent (handled by Passthrough). This is the PUBLIC SECURITY BOUNDARY for the
// generic passthrough: any path not listed here is rejected at the router edge
// and never reaches an agent. Keep it NARROW and inference-only — never add a
// vLLM control-plane path such as /v1/load_lora_adapter, /sleep, /collective_rpc,
// or /metrics, which a caller could otherwise use to load arbitrary weights or
// stall the GPU.
var passthroughAllowlist = map[string]bool{
	"/v1/chat/completions":      true, // OpenAI Chat Completions
	"/v1/messages":              true, // Anthropic Messages API (e.g. Claude Code)
	"/v1/messages/count_tokens": true, // Anthropic token counting (stateless, read-only)
}

// callerIDs extracts the three billing/attribution identifiers from the Gin
// context. All three are set by middleware before handlers run; the defaults
// fire only when the router is called without the full middleware chain (e.g.
// in tests or direct local calls).
func callerIDs(c *gin.Context) (tenantID, keyID, deploymentID string) {
	tenantID = "anonymous"
	if raw, ok := c.Get("tenant_id"); ok {
		if s, ok := raw.(string); ok && s != "" {
			tenantID = s
		}
	}
	keyID = "anonymous"
	if raw, ok := c.Get("key_id"); ok {
		if s, ok := raw.(string); ok && s != "" {
			keyID = s
		}
	}
	// deployment_id is not available at request-entry time — the router derives it
	// from the selected agent's metadata after routing. Pre-routing metrics (auth
	// failures, rate limits, token limits) correctly carry "unset".
	deploymentID = "unset"
	return
}

// setRootSpanError sets the error status and message on the root HTTP span
// so Tempo's statusMessage is visible in Grafana dashboards.
func setRootSpanError(c *gin.Context, msg string) {
	if span := trace.SpanFromContext(c.Request.Context()); span.IsRecording() {
		span.SetStatus(codes.Error, msg)
	}
}

// writeRouterError responds with a structured RouterErrorResponse, stamps the
// audit log with the error code, and marks the root HTTP span as ERROR with
// the message. Use this for every error return path so the Live Traces
// dashboard's Status and Error columns stay in sync with the HTTP status —
// a 400/401/404 without this call shows up as "OK" in Grafana because the
// span's OTEL status defaults to OK.
func writeRouterError(c *gin.Context, httpStatus int, code domain.ErrorCode, message string, source domain.ErrorSource) {
	c.Set(auditKeyErrorCode, string(code))
	setRootSpanError(c, message)
	c.JSON(httpStatus, domain.RouterErrorResponse{
		Error: domain.NewRouterError(code, message, source),
	})
}

// abortWithRouterError is the middleware-side counterpart to writeRouterError
// that calls c.AbortWithStatusJSON so remaining handlers in the chain don't
// run. Matches the span / audit side effects so early-reject middleware
// failures (auth, rate limit) surface correctly in Grafana.
func abortWithRouterError(c *gin.Context, httpStatus int, code domain.ErrorCode, message string, source domain.ErrorSource) {
	c.Set(auditKeyErrorCode, string(code))
	setRootSpanError(c, message)
	c.AbortWithStatusJSON(httpStatus, domain.RouterErrorResponse{
		Error: domain.NewRouterError(code, message, source),
	})
}

// generateRequestID creates a unique request identifier.
func generateRequestID() string {

	// Use nanosecond timestamp for uniqueness.
	return fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
}

// sseFlushingWriter wraps a gin.ResponseWriter and flushes after every Write so
// SSE chunks reach the client as soon as they arrive from the backend.
type sseFlushingWriter struct {
	http.ResponseWriter
	flusher http.Flusher
}

func (fw *sseFlushingWriter) Write(p []byte) (n int, err error) {
	n, err = fw.ResponseWriter.Write(p)
	if n > 0 {
		fw.flusher.Flush()
	}
	return
}
