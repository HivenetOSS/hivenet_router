// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"fmt"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/metrics"

	"github.com/gin-gonic/gin"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

var log = logging.Logger("api")

// Server represents the HTTP API server
type Server struct {

	// Handles incoming HTTP requests and routes them to handlers.
	engine *gin.Engine

	// Handlers holds all the HTTP handler functions.
	handlers *Handlers

	// TCP the server will listen on.
	port string

	// apiAuth authenticates requests to /v1/* endpoints.
	apiAuth auth.Provider

	// adminAuth authenticates requests to /admin/* endpoints.
	adminAuth auth.Provider

	// rateLimiter enforces per-tenant RPM and daily token limits on /v1/* endpoints.
	rateLimiter auth.RateLimiter

	// routerMetrics provides tenant quota gauge recording.
	routerMetrics *metrics.RouterMetrics

	// healthyAgentCount reports the live count of healthy agents serving a
	// model. QuotaMiddleware uses it to compute the effective per-minute
	// ceiling for per-model quotas as requests_per_minute_per_replica × replicas.
	//
	// May be nil. A nil counter does NOT disable per-model quota enforcement —
	// per-model keys remain enforced, but the per-replica multiplication is
	// skipped and requests_per_minute_per_replica is treated as a flat ceiling
	// for the (tenant, model) bucket. The "no live backends → skip quota and
	// let routing surface its own error" behaviour also stops applying when
	// the counter is nil (with no way to read replica count, we can't detect
	// the zero-replicas case). Production wiring always passes a non-nil
	// counter; tests sometimes omit it.
	healthyAgentCount func(model string) int

	// maxRequestBytes caps request body size on /v1/* inference endpoints.
	// <= 0 disables the limit. See BodyLimitMiddleware.
	maxRequestBytes int64
}

// NewServer creates a new HTTP server (Follows a "constructor" pattern).
// healthyAgentCount may be nil — per-model RPM quotas remain enforced but the
// per-replica scaling is disabled (per_replica acts as a flat ceiling). The
// legacy flat path does not consult healthyAgentCount at all and is unaffected.
func NewServer(handlers *Handlers, port string, apiAuth, adminAuth auth.Provider, rateLimiter auth.RateLimiter, m *metrics.RouterMetrics, healthyAgentCount func(model string) int, maxRequestBytes int64) *Server {

	// Set Gin to "release" mode (production).
	gin.SetMode(gin.ReleaseMode)

	// Create a new Gin engine *without* any default middleware.
	engine := gin.New()

	// Attach global middleware. Order matters:
	//   1. RequestIDMiddleware  — assigns X-Request-ID before anything else reads it
	//   2. AuditMiddleware      — must run after RequestIDMiddleware (reads request_id)
	//                             and before RecoveryMiddleware (defer fires after panic recovery)
	//   3. LoggerWithFormatter  — logs method/path/status; Authorization header excluded
	//   4. RecoveryMiddleware   — converts panics to 500 responses
	//   5. CORSMiddleware       — sets CORS headers
	engine.Use(
		otelgin.Middleware("hivenet-router"),
		REDMiddleware(m),
		TraceResponseMiddleware(),
		RequestIDMiddleware(),
		AuditMiddleware(),
		gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
			// Standard gin format — Authorization header is deliberately excluded.
			return fmt.Sprintf("[GIN] %v | %3d | %13v | %15s | %-7s %#v\n%s",
				param.TimeStamp.Format("2006/01/02 - 15:04:05"),
				param.StatusCode,
				param.Latency,
				param.ClientIP,
				param.Method,
				param.Path,
				param.ErrorMessage,
			)
		}),
		RecoveryMiddleware(),
		CORSMiddleware(),
	)

	// Create the Server struct and populate its fields.
	s := &Server{
		engine:            engine,
		handlers:          handlers,
		port:              port,
		apiAuth:           apiAuth,
		adminAuth:         adminAuth,
		rateLimiter:       rateLimiter,
		routerMetrics:     m,
		healthyAgentCount: healthyAgentCount,
		maxRequestBytes:   maxRequestBytes,
	}

	// Register all HTTP routes/endpoints (Keep routing logic separate from startup).
	s.registerRoutes()

	// Return the fully initialized server.
	return s
}

// registerRoutes defines all HTTP routes for the API.
func (s *Server) registerRoutes() {

	// GET /health — public liveness probe (load balancers, k8s probes).
	// Returns {"status":"ok"} only — no operational data exposed.
	s.engine.GET("/health", s.handlers.Liveness)

	// /v1/* — OpenAI-compatible API, protected by apiAuth.
	//
	// Quota enforcement applies to inference verbs only. Metadata reads
	// (/models, /models/*model) have no request body, no agent dispatch, and
	// no model name to gate against — wrapping them in QuotaMiddleware would
	// make peekModel resolve to "" and trip the strict per-model enumeration
	// check, 429-ing every discovery call for any key that uses per_model
	// quotas (HAI-231). Clients need /models reachable before they can pick a
	// model to call, which is also how OpenAI / Anthropic expose it.
	v1 := s.engine.Group("/v1")
	v1.Use(AuthMiddleware(s.apiAuth))
	{
		// GET /v1/models — list all models currently served by registered agents
		v1.GET("/models", s.handlers.ListModels)

		// GET /v1/models/:model — per-model detail with full agent breakdown
		v1.GET("/models/*model", s.handlers.GetModel)
	}

	inference := v1.Group("")
	// Cap body size before quota/handler work buffers the payload into memory.
	inference.Use(BodyLimitMiddleware(s.maxRequestBytes))
	inference.Use(QuotaMiddleware(s.rateLimiter, s.routerMetrics, s.healthyAgentCount))
	{
		// Generic allowlisted passthrough: routed by top-level "model" and
		// forwarded verbatim to the backend's native endpoint at the same path.
		// The set of accepted paths is passthroughAllowlist in handlers.go (the
		// public security boundary). OpenAI Chat Completions and the Anthropic
		// Messages API (e.g. Claude Code) share this single mechanism.
		inference.POST("/chat/completions", s.handlers.Passthrough)
		inference.POST("/messages", s.handlers.Passthrough)
		inference.POST("/messages/count_tokens", s.handlers.Passthrough)

		// POST /v1/embeddings — forward to a healthy embedding agent
		inference.POST("/embeddings", s.handlers.Embeddings)

		// POST /v1/rerank — forward to a healthy reranking agent
		inference.POST("/rerank", s.handlers.Rerank)
	}

	// /admin/* — operational visibility, protected by adminAuth middleware.
	admin := s.engine.Group("/admin")
	admin.Use(AuthMiddleware(s.adminAuth))
	{
		// GET /admin/health — full router status: agent count, health, queue depth, per-agent list
		admin.GET("/health", s.handlers.AdminHealth)

		// GET /admin/routing-table — full routing table snapshot (all metrics, SRTT, hardware, engine)
		admin.GET("/routing-table", s.handlers.ListRoutingTable)

		// GET /admin/registration-stream — long-lived SSE feed of agent
		// registration deltas, for consumers that need to react to an agent
		// arriving or leaving faster than polling the routing table allows.
		admin.GET("/registration-stream", s.handlers.RegistrationStream)

		// GET /admin/storage — BadgerDB key counts, disk size, GC status
		admin.GET("/storage", s.handlers.StorageInfo)

		// GET /admin/models           — operator view of every model served by registered agents (no per-key filter)
		// GET /admin/models/*model    — operator detail view for a single model
		// Mirrors /v1/models{,/:model} but unfiltered; admin auth gates these.
		admin.GET("/models", s.handlers.AdminListModels)
		admin.GET("/models/*model", s.handlers.AdminGetModel)

		// POST /admin/metrics/reset — clear persisted lifetime per-agent counters
		// (disk + memory + Prometheus series) so dashboards start from a clean baseline
		admin.POST("/metrics/reset", s.handlers.ResetMetrics)

		// GET /admin/policy  — returns the currently active routing policy as JSON
		admin.GET("/policy", s.handlers.GetPolicy)

		// PUT /admin/policy  — replaces the active routing policy with a YAML body (ephemeral)
		admin.PUT("/policy", s.handlers.PutPolicy)

		// GET    /admin/policy/models        — list all named policy documents
		// GET    /admin/policy/models/*name  — get a policy document by name
		// PUT    /admin/policy/models/*name  — create or replace a named policy document (YAML body, ephemeral)
		// DELETE /admin/policy/models/*name  — remove a named policy document
		admin.GET("/policy/models", s.handlers.GetModelPolicies)
		admin.GET("/policy/models/*name", s.handlers.GetModelPolicy)
		admin.PUT("/policy/models/*name", s.handlers.PutModelPolicy)
		admin.DELETE("/policy/models/*name", s.handlers.DeleteModelPolicy)

		// GET    /admin/api-keys              — list all active API keys (dynamic mode)
		// GET    /admin/api-keys/:id          — get a single API key by ID (dynamic mode)
		// PUT    /admin/api-keys/:id          — add or update a single API key (dynamic mode)
		// DELETE /admin/api-keys/:id          — revoke an API key (dynamic mode)
		// POST /admin/api-keys/replace        — atomically replace the entire key registry (dynamic mode)
		// GET  /admin/api-keys/version        — current registry version and key count (dynamic mode)
		admin.GET("/api-keys", s.handlers.ListAPIKeys)
		admin.GET("/api-keys/:id", s.handlers.GetAPIKey)
		admin.PUT("/api-keys/:id", s.handlers.UpsertAPIKey)
		admin.DELETE("/api-keys/:id", s.handlers.DeleteAPIKey)
		admin.POST("/api-keys/replace", s.handlers.ReplaceAPIKeys)
		admin.GET("/api-keys/version", s.handlers.GetAPIKeyVersion)
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	log.Infof("HTTP API starting on %s", s.port)

	// Start the Gin HTTP server on the given port.
	// This is a blocking call — it runs until the server stops or errors.
	return s.engine.Run(s.port)
}
