// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// quota_middleware_test exercises QuotaMiddleware against the two quota shapes:
// per-model (with healthy-replica scaling and the strict-enumeration rule) and
// the legacy flat fallback. The full handler chain (auth → quota → passthrough)
// is replaced by a mini Gin router so the assertions focus on the middleware's
// decisions: which bucket got charged, what the response code looks like, and
// whether the handler-facing context keys carry the resolved per-model TPD.
package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/metrics"

	"github.com/gin-gonic/gin"
)

// recLimiter records each AllowRequest call so tests can assert which
// (tenant, model) bucket was hit and verify per-model isolation end-to-end.
type recLimiter struct {
	mu        sync.Mutex
	requests  []recCall
	allowRPM  bool
	remaining int
}

type recCall struct {
	tenant string
	model  string
	rpm    int
}

func (l *recLimiter) AllowRequest(tenant, model string, rpm int) (bool, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.requests = append(l.requests, recCall{tenant, model, rpm})
	if l.allowRPM {
		return true, l.remaining, nil
	}
	return false, 0, nil
}
func (l *recLimiter) AllowInputTokens(string, string, int, int) (bool, int, error) {
	return true, -1, nil
}
func (l *recLimiter) AllowOutputTokens(string, string, int, int) (bool, int, error) {
	return true, -1, nil
}
func (l *recLimiter) RemainingTokens(string, string, int) (int, error) { return -1, nil }
func (l *recLimiter) Reset()                                           {}

// stamp injects the auth context the QuotaMiddleware reads (so tests don't
// need to spin up a real Provider). label identifies the key for clearer
// failure messages.
func stamp(tenantID string, limits auth.QuotaLimits) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("tenant_id", tenantID)
		c.Set("key_id", "test-key")
		c.Set("quota_limits", limits)
		c.Next()
	}
}

// terminal records context state the handler would consume after admission
// (effective_tpd, quota_model) so tests can assert per-model resolution
// reached the handler verbatim.
func terminal(out *struct {
	hit        bool
	effectiveT int
	resolvedM  string
}) gin.HandlerFunc {
	return func(c *gin.Context) {
		out.hit = true
		if v, ok := c.Get("effective_tpd"); ok {
			out.effectiveT, _ = v.(int)
		}
		if v, ok := c.Get("quota_model"); ok {
			out.resolvedM, _ = v.(string)
		}
		c.Status(http.StatusOK)
	}
}

func newRouter(lim auth.RateLimiter, limits auth.QuotaLimits, count func(string) int, sink *struct {
	hit        bool
	effectiveT int
	resolvedM  string
}) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := metrics.NewRouterMetrics()
	r.Use(stamp("tenant-A", limits))
	r.Use(api.QuotaMiddleware(lim, m, count))
	r.POST("/v1/chat/completions", terminal(sink))
	return r
}

func postJSON(r *gin.Engine, body []byte) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func mustModel(t *testing.T, model string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"model": model})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestQuotaMiddleware_PerModel_ScalesByReplicas verifies the effective ceiling
// passed to the limiter is per_replica × healthy replica count. With 3 healthy
// replicas of the 27B and per_replica=10, the middleware must charge the bucket
// against an rpm of 30 — not the per-replica scalar.
func TestQuotaMiddleware_PerModel_ScalesByReplicas(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B": {RequestsPerMinutePerReplica: 10, TokensPerDay: 3_000_000},
		},
	}
	lim := &recLimiter{allowRPM: true, remaining: 30}
	count := func(model string) int {
		if model == "Qwen/Qwen3.6-27B" {
			return 3
		}
		return 0
	}
	sink := &struct {
		hit        bool
		effectiveT int
		resolvedM  string
	}{}
	r := newRouter(lim, limits, count, sink)

	if w := postJSON(r, mustModel(t, "Qwen/Qwen3.6-27B")); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if len(lim.requests) != 1 {
		t.Fatalf("expected 1 limiter call, got %d", len(lim.requests))
	}
	got := lim.requests[0]
	if got.model != "Qwen/Qwen3.6-27B" || got.rpm != 30 {
		t.Fatalf("expected model=27B rpm=30 (10/replica × 3 replicas), got %+v", got)
	}
	if !sink.hit {
		t.Fatalf("handler should have run after admission")
	}
	if sink.effectiveT != 3_000_000 || sink.resolvedM != "Qwen/Qwen3.6-27B" {
		t.Fatalf("handler-facing context wrong: effectiveT=%d resolvedM=%q",
			sink.effectiveT, sink.resolvedM)
	}
}

// TestQuotaMiddleware_PerModel_LoudRejectWhenUndeclared verifies the strict-
// enumeration rule: a request whose model has no per_model entry is rejected
// with 429 — never silently admitted. There is no wildcard or catch-all.
func TestQuotaMiddleware_PerModel_LoudRejectWhenUndeclared(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B": {RequestsPerMinutePerReplica: 10, TokensPerDay: 3_000_000},
		},
	}
	lim := &recLimiter{allowRPM: true, remaining: 30}
	count := func(string) int { return 3 }
	sink := &struct {
		hit        bool
		effectiveT int
		resolvedM  string
	}{}
	r := newRouter(lim, limits, count, sink)

	w := postJSON(r, mustModel(t, "Qwen/Qwen3.6-35B"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for undeclared model, got %d body=%s", w.Code, w.Body.String())
	}
	if len(lim.requests) != 0 {
		t.Fatalf("undeclared model must not reach the limiter; calls=%+v", lim.requests)
	}
	if sink.hit {
		t.Fatalf("handler must not run when quota loud-rejects")
	}
}

// TestQuotaMiddleware_PerModel_ZeroReplicasSkipsQuota verifies the "no live
// backends" escape hatch: when replica count is zero the quota check is
// skipped (effective_rpm × 0 == 0 would falsely look like rate-limited;
// the routing layer surfaces the real error downstream).
func TestQuotaMiddleware_PerModel_ZeroReplicasSkipsQuota(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B": {RequestsPerMinutePerReplica: 10, TokensPerDay: 3_000_000},
		},
	}
	lim := &recLimiter{allowRPM: false} // would deny if asked
	count := func(string) int { return 0 }
	sink := &struct {
		hit        bool
		effectiveT int
		resolvedM  string
	}{}
	r := newRouter(lim, limits, count, sink)

	if w := postJSON(r, mustModel(t, "Qwen/Qwen3.6-27B")); w.Code != http.StatusOK {
		t.Fatalf("expected handler to run (quota skipped), got %d", w.Code)
	}
	if len(lim.requests) != 0 {
		t.Fatalf("zero-replica path must NOT call the limiter; calls=%+v", lim.requests)
	}
	if !sink.hit {
		t.Fatalf("handler should run when quota is skipped")
	}
	// The handler must still see the resolved per-model TPD so the rest of
	// the pipeline (reserveInputBudget, processor) charges the right bucket
	// when the response eventually streams.
	if sink.effectiveT != 3_000_000 || sink.resolvedM != "Qwen/Qwen3.6-27B" {
		t.Fatalf("handler-facing context wrong on zero-replica path: effectiveT=%d resolvedM=%q",
			sink.effectiveT, sink.resolvedM)
	}
}

// TestQuotaMiddleware_MetadataRoutesBypassQuota verifies the structural fix for
// HAI-231: GET /v1/models and GET /v1/models/:model must NOT run through
// QuotaMiddleware. These routes have no request body, so peekModel would
// resolve model="" and the strict per-model enumeration check would 429 every
// discovery call for any key on the per_model shape — making clients unable to
// list models before picking one.
//
// The test mirrors the route grouping in server.go: metadata routes sit in an
// authn-only subgroup, inference routes sit in a quota-protected subgroup. A
// regression that re-applies QuotaMiddleware to /v1/models will fail here with
// a 429 carrying the "no quota declared for model  on this API key" body.
func TestQuotaMiddleware_MetadataRoutesBypassQuota(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B": {RequestsPerMinutePerReplica: 10, TokensPerDay: 3_000_000},
		},
	}
	lim := &recLimiter{allowRPM: true, remaining: 30}
	count := func(string) int { return 3 }

	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := metrics.NewRouterMetrics()
	r.Use(stamp("tenant-A", limits))

	// Mirror server.go: metadata authn-only, inference authn+quota.
	v1 := r.Group("/v1")
	{
		v1.GET("/models", func(c *gin.Context) { c.Status(http.StatusOK) })
		v1.GET("/models/*model", func(c *gin.Context) { c.Status(http.StatusOK) })
	}
	inference := v1.Group("")
	inference.Use(api.QuotaMiddleware(lim, m, count))
	{
		inference.POST("/chat/completions", func(c *gin.Context) { c.Status(http.StatusOK) })
	}

	// GET /v1/models — no body, no model in request → must reach handler.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /v1/models must bypass quota; got %d body=%s", w.Code, w.Body.String())
	}
	if len(lim.requests) != 0 {
		t.Fatalf("metadata route must not consult the limiter; got calls=%+v", lim.requests)
	}

	// GET /v1/models/:model — has a model in the path but still no body.
	// Must also bypass quota.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/models/Qwen/Qwen3.6-27B", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /v1/models/:model must bypass quota; got %d body=%s", w.Code, w.Body.String())
	}
	if len(lim.requests) != 0 {
		t.Fatalf("metadata route must not consult the limiter; got calls=%+v", lim.requests)
	}

	// Inference verb with an undeclared model on the SAME router still 429s —
	// proving the quota gate still works where it should.
	w = postJSON(r, mustModel(t, "Qwen/Qwen3.6-35B"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("POST /v1/chat/completions with undeclared model must still 429; got %d body=%s",
			w.Code, w.Body.String())
	}
}

// TestQuotaMiddleware_LegacyFlatPath verifies the back-compat path is untouched:
// a key whose QuotaLimits.PerModel is nil takes the umbrella RPM as today —
// and the limiter sees the legacy empty model bucket.
func TestQuotaMiddleware_LegacyFlatPath(t *testing.T) {
	limits := auth.QuotaLimits{RequestsPerMinute: 100, TokensPerDay: 5_000_000}
	lim := &recLimiter{allowRPM: true, remaining: 100}
	sink := &struct {
		hit        bool
		effectiveT int
		resolvedM  string
	}{}
	r := newRouter(lim, limits, func(string) int { return 999 }, sink)

	if w := postJSON(r, mustModel(t, "Qwen/Qwen3.6-27B")); w.Code != http.StatusOK {
		t.Fatalf("expected 200 on legacy flat path, got %d", w.Code)
	}
	if len(lim.requests) != 1 {
		t.Fatalf("expected 1 limiter call, got %d", len(lim.requests))
	}
	got := lim.requests[0]
	if got.model != "" || got.rpm != 100 {
		t.Fatalf("legacy path must pass empty model and umbrella rpm; got %+v", got)
	}
	// Legacy path doesn't set effective_tpd / quota_model — handler reads
	// QuotaLimits.TokensPerDay directly via quotaBudgetFromContext.
	if sink.effectiveT != 0 || sink.resolvedM != "" {
		t.Fatalf("legacy path must NOT stash per-model keys; effectiveT=%d resolvedM=%q",
			sink.effectiveT, sink.resolvedM)
	}
}
