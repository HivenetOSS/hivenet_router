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
	"strings"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/jsonbody"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/semantic"

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

// requestBody returns the request body, reading and caching it for the
// middlewares and handlers after it when no earlier step has. ok is false
// once it has answered 400 because the body could not be read.
func requestBody(c *gin.Context) (body []byte, ok bool) {
	if body, ok := cachedBody(c); ok {
		return body, true
	}
	b, err := io.ReadAll(c.Request.Body)
	if err != nil {
		abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
			"failed to read request body", domain.SourceRouter)
		return nil, false
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(b))
	c.Set(gin.BodyBytesKey, b)
	return b, true
}

// JSONObjectBody refuses, with 400 request_invalid, an inference body that is
// not exactly one JSON object: invalid JSON, another JSON value, or an object
// followed by more data; and one the cache salt could not prepare for some
// engine (cachesalt.CheckBody: an extra_params that is not an object on
// /v1/messages). The handlers' JSON decoding stops after the first
// value, so such a body would otherwise be accepted and only fail later, when
// the router edits it for the agent (cache salt, alias rewrite), after an
// agent was already chosen. Runs after BodyLimitMiddleware; the body it read
// is kept for the middlewares and handlers after it.
func JSONObjectBody() gin.HandlerFunc {
	return func(c *gin.Context) {
		body, ok := requestBody(c)
		if !ok {
			return
		}
		if err := jsonbody.ValidObject(body); err != nil {
			abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
				"Invalid request: the body must be exactly one JSON object", domain.SourceRouter)
			return
		}
		// What some engine's edit could not handle is refused here too, so
		// the answer does not depend on which agent would serve the request.
		if err := cachesalt.CheckBody(c.FullPath(), body); err != nil {
			abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
				"Invalid request: "+err.Error(), domain.SourceRouter)
			return
		}
		c.Next()
	}
}

// routerReadFields are the top-level request fields the router itself reads:
// what the handlers decode (domain.ChatRequest; embeddings and rerank read
// only "model"), what semantic aliases read (semantic.DecodedFields), and the
// model and stream the middlewares peek at. A new struct the router decodes
// from an inference body belongs here.
var routerReadFields = func() []string {
	var out []string
	seen := map[string]bool{}
	for _, names := range [][]string{jsonbody.FieldNames(domain.ChatRequest{}), semantic.DecodedFields(), {"model", "stream"}} {
		for _, n := range names {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	return out
}()

// routerReadShape is what the router decodes of an inference body, top level
// and nested, built from the structs it decodes into so a field added to them
// is covered: the top-level fields (routerReadFields); each messages[] entry
// (domain.ChatCompletionMessage, the semantic view's message); each content
// part or block, in a message's content, the Anthropic system array and a
// tool_result's content (domain.ContentPart with its image_url and
// input_audio, the semantic view's part). The prompt estimate behind every
// input budget and the image cap are computed from these. Free-form subtrees
// (tool definitions, tool_calls, metadata, response_format, reasoning,
// thinking) are not in it: the router only counts their bytes or reads them
// as hints for choosing among models the key may use, after which the model
// allowlist is checked again.
var routerReadShape = func() *jsonbody.Shape {
	part := jsonbody.ObjectOf(jsonbody.FieldNames(domain.ContentPart{}), semantic.DecodedPartFields())
	part.Fields["image_url"] = jsonbody.ObjectOf(jsonbody.FieldNames(domain.ImageURLConfig{}))
	part.Fields["input_audio"] = jsonbody.ObjectOf(jsonbody.FieldNames(domain.InputAudioConfig{}))
	parts := &jsonbody.Shape{Items: part} // a string is not looked into
	part.Fields["content"] = parts
	message := jsonbody.ObjectOf(jsonbody.FieldNames(domain.ChatCompletionMessage{}), semantic.DecodedMessageFields())
	message.Fields["content"] = parts
	top := jsonbody.ObjectOf(routerReadFields)
	top.Fields["messages"] = &jsonbody.Shape{Items: message}
	top.Fields["system"] = parts
	return top
}()

// UnambiguousFields refuses, with 400 request_invalid, an inference body the
// router and the backend would read differently. The router decodes with
// encoding/json, which matches top-level names case-insensitively and keeps
// the last of a repeated name, then forwards the body as sent to a backend
// that matches names exactly. So {"model":"coder","Model":"chat"} would pass
// the model allowlist as chat while the backend serves coder, and a
// "Max_Tokens" would size the router's capacity reservation but not the
// generation, and a "Content" next to a message's "content" would hide the
// prompt from every input budget. Refused, in every object the router
// decodes (routerReadShape): a name that appears twice, and a name that
// matches one of the object's read fields only under case folding. A body that
// is not one JSON object is refused too: this is the access boundary, and
// the handlers' decoder would read the first object and ignore the rest.
// Runs after JSONObjectBody and before everything that reads the body.
func UnambiguousFields() gin.HandlerFunc {
	return func(c *gin.Context) {
		body, ok := requestBody(c)
		if !ok {
			return
		}
		if err := jsonbody.CheckShape(body, routerReadShape); err != nil {
			msg := "Invalid request: the body must be exactly one JSON object"
			var ne *jsonbody.NameError
			if errors.As(err, &ne) {
				msg = "Invalid request: " + ne.Error() + "; send each field once, spelled exactly"
			}
			abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, msg, domain.SourceRouter)
			return
		}
		c.Next()
	}
}

// CORSMiddleware adds CORS headers to responses
// CORS controls whether browsers are allowed to call this API
func CORSMiddleware() gin.HandlerFunc {

	// Middleware in Gin is just a function that receives a request context.
	return func(c *gin.Context) {

		// The operator-only admin API is not offered to browsers: no CORS
		// headers, so no web page on another origin can read an admin
		// response with a credential it obtained. A preflight there gets
		// no CORS answer: no /admin route takes OPTIONS, so it is a 404.
		if p := c.Request.URL.Path; p == "/admin" || strings.HasPrefix(p, "/admin/") {
			c.Next()
			return
		}

		// Allow requests from ANY origin ("*").
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")

		// Specify which HTTP methods are allowed when accessing this API.
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

		// Specify which HTTP headers the client is allowed to send.
		// "Authorization" allows Bearer tokens, API keys.
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Client-Request-ID, X-Request-ID")
		// Response headers a browser client's JavaScript may read.
		c.Writer.Header().Set("Access-Control-Expose-Headers", "X-Request-ID, X-Client-Request-ID, X-Upstream-Request-ID")
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

// Client request ID headers. A client sends its own ID in
// X-Client-Request-ID, or in X-Request-ID as before; the router echoes it in
// X-Client-Request-ID.
const (
	headerRequestID       = "X-Request-ID"
	headerClientRequestID = "X-Client-Request-ID"
	ctxKeyClientRequestID = "client_request_id"
)

// RequestIDMiddleware gives every request one ID, used end to end.
//
// The router always mints the ID, a UUIDv7, so no two requests share one,
// whatever clients send. It is stored in the Gin context ("request_id"),
// returned in the X-Request-ID response header and forwarded to the agent
// and backend in X-Request-ID.
//
// A client may also send its own ID: X-Client-Request-ID, or X-Request-ID as
// before. The first of the two that is a valid UUID (any version) is kept
// in canonical form as the client request ID ("client_request_id" in the context and the audit record) and
// echoed in X-Client-Request-ID. It labels the request and lets a retry be
// recognised as the same request. It is never forwarded, and an invalid
// value is dropped, so crafted text can't reach any log.
//
// Applied globally so every response — including 401s — is traceable.
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		for _, h := range []string{headerClientRequestID, headerRequestID} {
			if id, ok := domain.CanonicalRequestID(c.GetHeader(h)); ok {
				setClientRequestID(c, id)
				break
			}
		}
		c.Request.Header.Del(headerClientRequestID)
		setRequestID(c, newRequestID())
		c.Next()
	}
}

// setRequestID stores id as the request's ID: the Gin context, the
// X-Request-ID response header, the X-Request-ID request header (so the
// value forwarded to the agent and backend is this ID) and the active trace
// span (so traces can be correlated with audit logs that carry the same ID).
func setRequestID(c *gin.Context, id string) {
	c.Set("request_id", id)
	c.Request.Header.Set(headerRequestID, id)
	c.Header(headerRequestID, id)
	if span := trace.SpanFromContext(c.Request.Context()); span.IsRecording() {
		span.SetAttributes(attribute.String("request.id", id))
	}
}

// setClientRequestID stores the client's own ID: the Gin context and the
// X-Client-Request-ID response header.
func setClientRequestID(c *gin.Context, id string) {
	c.Set(ctxKeyClientRequestID, id)
	c.Header(headerClientRequestID, id)
	if span := trace.SpanFromContext(c.Request.Context()); span.IsRecording() {
		span.SetAttributes(attribute.String("request.client_id", id))
	}
}

// newRequestID mints a UUIDv7: time-ordered, so IDs sort by creation time in
// logs, and random enough that several router instances never collide.
func newRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// NewV7 only fails if the system random source fails. Fall back to
		// uuid.New, the previous behaviour, which panics in that case.
		return uuid.New().String()
	}
	return id.String()
}

// requestIDFrom returns the request ID assigned by RequestIDMiddleware. A
// handler reached without the middleware gets a fresh ID, stored like the
// middleware's so every later reader sees it, and a logged error: it is a
// wiring mistake, and readers that ran earlier (the audit log, early spans)
// saw no ID.
func requestIDFrom(c *gin.Context) string {
	if id := c.GetString("request_id"); id != "" {
		return id
	}
	log.Errorf("request_id missing on %s %s: RequestIDMiddleware is not installed on this route", c.Request.Method, c.Request.URL.Path)
	id := newRequestID()
	setRequestID(c, id)
	return id
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
		c.Set(ctxKeyRoutingStrategies, result.RoutingStrategies)
		c.Set(ctxKeyScopeMax, result.ScopeMax)
		c.Set(ctxKeyHomeCountry, result.HomeCountry)
		c.Set(ctxKeyReservation, result.Reservation)
		c.Set("quota_limits", result.QuotaLimits)
		if result.Redirect != nil {
			// Accepted once RedirectTarget has checked the body.
			c.Set(ctxKeyRedirect, result.Redirect)
		}
		c.Next()
	}
}

// effectiveTPDContextKey carries the per-model daily token budget resolved by
// QuotaMiddleware down to the Passthrough handler so the handler does not need
// to repeat the per-model lookup. Absent when the key is on the legacy flat
// shape — the handler then falls back to QuotaLimits.TokensPerDay.
const effectiveTPDContextKey = "effective_tpd"

// quotaModelContextKey carries the model name used by the limiter buckets so
// AllowInputTokens / AllowOutputTokens charge the same (key, model) bucket
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
	out := quotaCaller{tenantID: auth.AnonymousID, keyID: auth.AnonymousID, deploymentID: "unset"}
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

// enforceFlatQuota runs the flat-shape RPM check on the calling key's own
// bucket. Returns false (and has already written the 429) when the request is
// denied. The per-tenant limit gauges are not written here: limits are per key,
// so the tenant gauges carry the sum over the tenant's keys, seeded by
// publishTenantQuotas whenever the key set changes.
func enforceFlatQuota(c *gin.Context, limiter auth.RateLimiter, m *metrics.RouterMetrics, caller quotaCaller, limits auth.QuotaLimits) bool {
	allowed, remaining, _ := limiter.AllowRequest(caller.tenantID, caller.keyID, "", limits.RequestsPerMinute)
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
// keys so the downstream handler charges the SAME (key, model) bucket
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

	// The per-(tenant, model) TPD gauge is seeded by publishTenantQuotas
	// (summed over the tenant's keys). The RPM gauge is written here because
	// its value depends on the live replica count; with several per-model
	// keys in one tenant it shows the last admitting key's ceiling.

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

	allowed, remaining, _ := limiter.AllowRequest(caller.tenantID, caller.keyID, model, effectiveRPM)
	if !allowed {
		denyRPM(c, m, caller, model, "rate limit exceeded")
		return false
	}
	setRemainingRequests(c, remaining)
	c.Set(effectiveTPDContextKey, entry.TokensPerDay)
	c.Set(quotaModelContextKey, model)
	return true
}

// QuotaMiddleware enforces per-key RPM limits using a RateLimiter.
// Applied on the /v1/* group after AuthMiddleware. Returns 429 with
// rate_limit_exceeded on RPM exhaustion, records the per-model RPM gauge, and
// sets X-RateLimit-Remaining-Requests for the calling key.
//
// Two quota shapes are supported on a per-key basis:
//
//   - Legacy flat (QuotaLimits.PerModel == nil): one umbrella RPM + TPD that
//     applies to every model. The limiter buckets are per key. Enforced
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
	var replicas func(c *gin.Context, model string) int
	if healthyAgentCount != nil {
		replicas = func(_ *gin.Context, model string) int { return healthyAgentCount(model) }
	}
	return QuotaMiddlewareWithReplicas(limiter, m, replicas)
}

// QuotaMiddlewareWithReplicas is QuotaMiddleware with a replica count that
// may depend on the request (its endpoint, for a request that may be held
// while its model wakes: see Handlers.QuotaReplicas).
func QuotaMiddlewareWithReplicas(limiter auth.RateLimiter, m *metrics.RouterMetrics, replicas func(c *gin.Context, model string) int) gin.HandlerFunc {
	return func(c *gin.Context) {
		caller := resolveCaller(c)
		limits, _ := c.Get("quota_limits")
		quota, _ := limits.(auth.QuotaLimits)

		var allowed bool
		if quota.PerModel == nil {
			allowed = enforceFlatQuota(c, limiter, m, caller, quota)
		} else {
			var count func(string) int
			if replicas != nil {
				count = func(model string) int { return replicas(c, model) }
			}
			allowed = enforcePerModelQuota(c, limiter, m, caller, quota, count)
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
//
// The result is cached in the gin context (ctxKeyPeekedModel): AliasMiddleware
// peeks first, and when it resolves an alias it overwrites the cache with the
// resolved model, so later peeks see the concrete model without re-decoding.
func peekModel(c *gin.Context) string {
	if v, ok := c.Get(ctxKeyPeekedModel); ok {
		if m, ok := v.(string); ok {
			return m
		}
	}
	var req struct {
		Model string `json:"model"`
	}
	if err := c.ShouldBindBodyWith(&req, binding.JSON); err != nil {
		return ""
	}
	c.Set(ctxKeyPeekedModel, req.Model)
	return req.Model
}
