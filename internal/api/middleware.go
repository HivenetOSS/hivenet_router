// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// REDMiddleware records per-endpoint RED metrics (rate, errors, duration) and
// tracks in-flight requests. Registered before route handlers so every request
// — including 401s and 404s — is captured. Uses Gin's FullPath() for the route
// label so cardinality stays bounded (no path parameters leak into labels).
func REDMiddleware(m *metrics.RouterMetrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		route := c.FullPath()
		if route == "" {
			route = "unmatched" // 404s / unknown routes
		}

		m.HTTPActiveRequestsInc(method, route)
		start := time.Now()

		c.Next()

		duration := time.Since(start).Seconds()
		m.HTTPActiveRequestsDec(method, route)
		m.ObserveHTTPRequest(method, route, c.Writer.Status(), duration)
	}
}

// BodyLimitMiddleware rejects requests whose body exceeds maxBytes, protecting
// the router from memory exhaustion by an oversized payload on the /v1/*
// inference endpoints. maxBytes <= 0 disables it.
//
// It fails fast on a declared Content-Length, then reads the body once through
// an http.MaxBytesReader so a missing or dishonest Content-Length (e.g. chunked
// transfer) can't bypass the cap. Because the /v1 handlers buffer the whole body
// anyway (ShouldBindBodyWith), reading it here is free — and lets every oversize
// case surface as a single, consistent 413 through the standard error envelope
// rather than a downstream 400/429. The buffered bytes are put back on the
// request so handlers read them normally.
func BodyLimitMiddleware(maxBytes int64) gin.HandlerFunc {
	tooLarge := func(c *gin.Context) {
		abortWithRouterError(c, http.StatusRequestEntityTooLarge, domain.ErrCodeRequestInvalid,
			fmt.Sprintf("request body too large: limit is %d bytes", maxBytes), domain.SourceRouter)
	}
	return func(c *gin.Context) {
		if maxBytes <= 0 {
			c.Next()
			return
		}
		if c.Request.ContentLength > maxBytes {
			tooLarge(c)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes))
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				tooLarge(c)
				return
			}
			abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
				"failed to read request body", domain.SourceRouter)
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		c.Request.ContentLength = int64(len(body))
		c.Next()
	}
}

// CORSMiddleware adds CORS headers to responses
// CORS controls whether browsers are allowed to call this API
func CORSMiddleware() gin.HandlerFunc {

	// Middleware in Gin is just a function that receives a request context.
	return func(c *gin.Context) {

		// Allow requests from ANY origin ("*").
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")

		// Specify which HTTP methods are allowed when accessing this API.
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

		// Specify which HTTP headers the client is allowed to send.
		// "Authorization" allows Bearer tokens, API keys.
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		// This passes control to the next middleware or handler in the chain.
		c.Next()
	}
}

// RecoveryMiddleware handles panics gracefully
func RecoveryMiddleware() gin.HandlerFunc {

	// gin.Recovery() is Gin's built-in recovery middleware.
	return gin.Recovery()
}

// RequestIDMiddleware attaches a trace ID to every request and response.
// If the caller provides an X-Request-ID header that is a valid UUID (any
// version) it is reused; otherwise a fresh UUID v4 is generated. Accepting
// any UUID version allows callers using v1/v7 time-ordered IDs (e.g. from
// OpenTelemetry or Jaeger) to propagate their own trace IDs. Rejecting
// non-UUID values prevents audit log poisoning via crafted request IDs.
// The ID is stored in the Gin context ("request_id") and echoed back in the
// X-Request-ID response header.
// Applied globally so every response — including 401s — is traceable.
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if _, err := uuid.Parse(id); err != nil {
			// Header absent, empty, or not a valid UUID — generate a fresh v4.
			id = uuid.New().String()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)

		// Link the request ID to the active trace span so Tempo traces
		// can be correlated with Loki audit logs that carry the same ID.
		if span := trace.SpanFromContext(c.Request.Context()); span.IsRecording() {
			span.SetAttributes(attribute.String("request.id", id))
		}

		c.Next()
	}
}

// TraceResponseMiddleware injects the W3C traceparent header into HTTP responses.
// This allows clients (including benchmarks) to extract the trace ID and look up
// the full distributed trace in Tempo or any OTLP-compatible backend.
// Must be registered after otelgin.Middleware so the span already exists.
// The header is set before c.Next() because the span context is available
// immediately after otelgin creates the span — waiting until after c.Next()
// risks the response already being flushed.
func TraceResponseMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		span := trace.SpanFromContext(c.Request.Context())
		sc := span.SpanContext()
		if sc.IsValid() {
			flags := "00"
			if sc.IsSampled() {
				flags = "01"
			}
			c.Header("traceparent", fmt.Sprintf("00-%s-%s-%s",
				sc.TraceID().String(), sc.SpanID().String(), flags))
		}
		c.Next()
	}
}

// AuthMiddleware authenticates requests using the given Provider.
// On success it stores "tenant_id" and "quota_limits" in the Gin context
// for downstream handlers and quota enforcement.
// Both ErrMissingCredentials and ErrInvalidCredentials return 401 with a
// WWW-Authenticate challenge — the response does not reveal which case
// occurred (security best practice: no information leak).
// Any other error returns 500.
// No per-request logging is done here; that is the provider's responsibility.
func AuthMiddleware(provider auth.Provider) gin.HandlerFunc {
	return func(c *gin.Context) {
		result, err := provider.Authenticate(c.Request)
		if err != nil {
			if errors.Is(err, auth.ErrMissingCredentials) || errors.Is(err, auth.ErrInvalidCredentials) {
				c.Header("WWW-Authenticate", `Bearer realm="hivenet-router"`)
				abortWithRouterError(c, 401, domain.ErrCodeUnauthorized, "unauthorized", domain.SourceRouter)
				return
			}
			abortWithRouterError(c, 500, domain.ErrCodeBackendError, "authentication error", domain.SourceRouter)
			return
		}
		c.Set("tenant_id", result.TenantID)
		c.Set("key_id", result.KeyID)
		c.Set("key_preview", result.KeyPreview)
		c.Set("allowed_models", result.AllowedModels)
		c.Set("quota_limits", result.QuotaLimits)
		c.Next()
	}
}

// effectiveTPDContextKey carries the per-model daily token budget resolved by
// QuotaMiddleware down to the Passthrough handler so the handler does not need
// to repeat the per-model lookup. Absent when the key is on the legacy flat
// shape — the handler then falls back to QuotaLimits.TokensPerDay.
const effectiveTPDContextKey = "effective_tpd"

// quotaModelContextKey carries the model name used by the limiter buckets so
// AllowInputTokens / AllowOutputTokens charge the same (tenant, model) bucket
// QuotaMiddleware admitted from. Empty for the legacy flat shape.
const quotaModelContextKey = "quota_model"

// quotaCaller bundles the per-request identifiers QuotaMiddleware uses to
// label metrics and audit fields. Pulled out of the context once so each
// helper below takes one struct instead of three repeated string extractions.
type quotaCaller struct {
	tenantID, keyID, deploymentID string
}

// resolveCaller pulls (tenant_id, key_id, deployment_id) from the Gin context
// and applies the same defaults the old inline middleware used so external
// behaviour stays identical.
func resolveCaller(c *gin.Context) quotaCaller {
	out := quotaCaller{tenantID: "anonymous", keyID: "anonymous", deploymentID: "unset"}
	if raw, ok := c.Get("tenant_id"); ok {
		if s, _ := raw.(string); s != "" {
			out.tenantID = s
		}
	}
	if raw, ok := c.Get("key_id"); ok {
		if s, _ := raw.(string); s != "" {
			out.keyID = s
		}
	}
	if raw, ok := c.Get("deployment_id"); ok {
		if s, _ := raw.(string); s != "" {
			out.deploymentID = s
		}
	}
	return out
}

// denyRPM is the shared 429 path: emit the rate-limit metrics, zero the
// remaining-requests header, and abort with rate_limit_exceeded. msg
// customises the response body so the per-model "no quota declared" case can
// carry a clearer error.
func denyRPM(c *gin.Context, m *metrics.RouterMetrics, caller quotaCaller, model, msg string) {
	m.TenantRateLimited(caller.tenantID, caller.keyID)
	m.TenantRequestFailed(caller.tenantID, caller.keyID, caller.deploymentID, model)
	m.AdmissionRejected("b4_rpm", model) // also surface in the unified 429-by-reason counter

	c.Header(headerRateLimitRemainingRequests, "0")
	abortWithRouterError(c, http.StatusTooManyRequests, domain.ErrCodeRateLimitExceeded, msg, domain.SourceRouter)
}

// setRemainingRequests writes the X-RateLimit-Remaining-Requests header when
// the limiter returned a real (non-sentinel) value. -1 means "unlimited" and
// must NOT surface as a header to clients.
func setRemainingRequests(c *gin.Context, remaining int) {
	if remaining >= 0 {
		c.Header(headerRateLimitRemainingRequests, strconv.Itoa(remaining))
	}
}

// enforceFlatQuota runs the legacy per-tenant umbrella RPM check — bit-for-bit
// what the middleware did before per-model quotas existed. Returns false (and
// has already written the 429) when the request is denied.
func enforceFlatQuota(c *gin.Context, limiter auth.RateLimiter, m *metrics.RouterMetrics, caller quotaCaller, limits auth.QuotaLimits) bool {
	m.TenantSetQuotaLimit(caller.tenantID, limits.RequestsPerMinute)
	m.TenantSetTPDLimit(caller.tenantID, limits.TokensPerDay)

	allowed, remaining, _ := limiter.AllowRequest(caller.tenantID, "", limits.RequestsPerMinute)
	if !allowed {
		model := peekModel(c)
		c.Set(auditKeyModel, model)
		denyRPM(c, m, caller, model, "rate limit exceeded")
		return false
	}
	setRemainingRequests(c, remaining)
	return true
}

// effectivePerModelRPM computes the per-minute ceiling for a per-model quota
// entry: per_replica × live healthy replicas. When healthyAgentCount is nil
// (legacy deployments / tests) it degrades to a flat per_replica ceiling.
// Returns (rpm, skipQuota) — skipQuota is true when zero replicas are live, so
// the caller short-circuits admission and lets the routing layer surface the
// real "no healthy backends" error instead of a misleading rate-limit message.
func effectivePerModelRPM(entry auth.PerModelQuotaLimits, model string, healthyAgentCount func(string) int) (rpm int, skipQuota bool) {
	if healthyAgentCount == nil {
		return entry.RequestsPerMinutePerReplica, false
	}
	replicas := healthyAgentCount(model)
	if replicas == 0 {
		return 0, true
	}
	return entry.RequestsPerMinutePerReplica * replicas, false
}

// enforcePerModelQuota runs the per-model RPM check. The model is peeked from
// the body and the matching entry is resolved by exact match — strict
// enumeration, no wildcard. Returns false (and has already written the 429)
// when the request is denied; sets the effective_tpd / quota_model context
// keys so the downstream handler charges the SAME (tenant, model) bucket
// admission ran against.
func enforcePerModelQuota(c *gin.Context, limiter auth.RateLimiter, m *metrics.RouterMetrics, caller quotaCaller, limits auth.QuotaLimits, healthyAgentCount func(string) int) bool {
	model := peekModel(c)
	c.Set(auditKeyModel, model)

	entry, ok := limits.ResolvePerModel(model)
	if !ok {
		// Loud rejection — operator must enumerate the model. A missing
		// entry must never silently mean "unlimited"; that's the whole
		// point of the strict shape.
		denyRPM(c, m, caller, model, "no quota declared for model "+model+" on this API key")
		return false
	}

	// Emit per-(tenant, model) gauges so dashboards can render the right
	// ceiling for THIS model rather than seeing the per-tenant gauges
	// oscillate as different models hit the same key. Per-model keys leave
	// the legacy per-tenant gauges alone — they were seeded to 0 at startup
	// and stay there, which is the dashboard signal "use the per-model series."
	m.TenantSetPerModelTPDLimit(caller.tenantID, model, entry.TokensPerDay)

	effectiveRPM, skipQuota := effectivePerModelRPM(entry, model, healthyAgentCount)
	if skipQuota {
		// No live backends — skip quota; routing returns the real error.
		// We still stash the resolved budget so the downstream handler
		// targets the right bucket if the request later succeeds. The
		// per-model RPM gauge is set to 0 explicitly so dashboards reflect
		// "no live capacity right now" instead of carrying over a stale
		// pre-drain value.
		c.Set(effectiveTPDContextKey, entry.TokensPerDay)
		c.Set(quotaModelContextKey, model)
		m.TenantSetPerModelQuotaLimit(caller.tenantID, model, 0)
		return true
	}
	m.TenantSetPerModelQuotaLimit(caller.tenantID, model, effectiveRPM)

	allowed, remaining, _ := limiter.AllowRequest(caller.tenantID, model, effectiveRPM)
	if !allowed {
		denyRPM(c, m, caller, model, "rate limit exceeded")
		return false
	}
	setRemainingRequests(c, remaining)
	c.Set(effectiveTPDContextKey, entry.TokensPerDay)
	c.Set(quotaModelContextKey, model)
	return true
}

// QuotaMiddleware enforces per-tenant RPM limits using a RateLimiter.
// Applied on the /v1/* group after AuthMiddleware. Returns 429 with
// rate_limit_exceeded on RPM exhaustion. Records configured RPM/TPD as
// Prometheus gauges per request, and sets X-RateLimit-Remaining-Requests.
//
// Two quota shapes are supported on a per-key basis:
//
//   - Legacy flat (QuotaLimits.PerModel == nil): one umbrella RPM + TPD that
//     applies to every model. The limiter buckets are per-tenant. Enforced
//     by enforceFlatQuota.
//
//   - Per-model (QuotaLimits.PerModel != nil): the middleware peeks the
//     "model" field, looks up the exact entry, and computes the effective
//     per-minute ceiling as requests_per_minute_per_replica × live healthy
//     replicas — so capacity grows automatically with the fleet. Enforced by
//     enforcePerModelQuota.
//
// healthyAgentCount may be nil in legacy deployments; per-model keys then
// fall back to a flat per_replica ceiling (no per-replica multiplication).
func QuotaMiddleware(limiter auth.RateLimiter, m *metrics.RouterMetrics, healthyAgentCount func(model string) int) gin.HandlerFunc {
	return func(c *gin.Context) {
		caller := resolveCaller(c)
		limits, _ := c.Get("quota_limits")
		quota, _ := limits.(auth.QuotaLimits)

		var allowed bool
		if quota.PerModel == nil {
			allowed = enforceFlatQuota(c, limiter, m, caller, quota)
		} else {
			allowed = enforcePerModelQuota(c, limiter, m, caller, quota, healthyAgentCount)
		}
		if allowed {
			c.Next()
		}
	}
}

// peekModel reads the "model" field from the JSON request body without
// consuming it for downstream handlers. Uses ShouldBindBodyWith to cache the
// body so it can be re-read by downstream handlers using the same binding
// method.
//
// There is intentionally no size gate here: per-model quota enforcement
// depends on the model value on every request, and real chat sessions
// routinely exceed 32 KB (50+ turn conversations or document-heavy prompts).
// A size gate would silently break per-model quotas — model would resolve to
// "" and the strict-enumeration check would 429 every large valid request.
//
// Memory-wise the buffering is paid once: the downstream Passthrough handler
// also calls ShouldBindBodyWith and receives the SAME cached bytes, so peek
// here costs nothing the request would not have paid anyway. Body-size
// protection lives at the http.MaxBytesReader / Gin layer where it belongs.
// On parse failure (non-JSON, malformed body) return "" — the regular
// request-invalid path picks up the error downstream.
func peekModel(c *gin.Context) string {
	var req struct {
		Model string `json:"model"`
	}
	if err := c.ShouldBindBodyWith(&req, binding.JSON); err != nil {
		return ""
	}
	return req.Model
}
