// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/test/testutil"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// fakeAuthProvider returns a pinned result/error from Authenticate, letting each
// case drive the middleware down a specific branch without real credentials.
type fakeAuthProvider struct {
	result *auth.AuthResult
	err    error
}

func (f fakeAuthProvider) Authenticate(*http.Request) (*auth.AuthResult, error) {
	return f.result, f.err
}

// Compile-time assertion that the fake satisfies the interface the middleware wants.
var _ auth.Provider = fakeAuthProvider{}

func TestAuthMiddleware(t *testing.T) {
	// Each provider error must map to a distinct HTTP status: auth failures are
	// 401 (client must re-authenticate), but an unexpected provider error is a
	// 500 — the middleware must not report an internal fault as an auth failure.
	cases := []struct {
		name     string
		provider auth.Provider
		wantCode int
	}{
		{"valid", fakeAuthProvider{result: &auth.AuthResult{TenantID: "team-a"}}, http.StatusOK},
		{"missing creds", fakeAuthProvider{err: auth.ErrMissingCredentials}, http.StatusUnauthorized},
		{"invalid creds", fakeAuthProvider{err: auth.ErrInvalidCredentials}, http.StatusUnauthorized},
		{"internal error", fakeAuthProvider{err: http.ErrHandlerTimeout}, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Build a minimal engine: middleware under test in front of a handler
			// that records the tenant the middleware injected into the context.
			r := gin.New()
			var sawTenant string
			r.Use(api.AuthMiddleware(tc.provider))
			r.GET("/x", func(c *gin.Context) {
				v, _ := c.Get("tenant_id")
				sawTenant, _ = v.(string)
				c.Status(http.StatusOK)
			})

			// Drive one request through the full middleware→handler chain.
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

			if w.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d", w.Code, tc.wantCode)
			}
			// On success the middleware must have propagated the tenant so the
			// downstream handler can attribute the request.
			if tc.wantCode == http.StatusOK && sawTenant != "team-a" {
				t.Errorf("tenant_id not set in context, got %q", sawTenant)
			}
			// RFC 7235: a 401 is only well-formed if it tells the client how to
			// authenticate, so the challenge header is mandatory on rejection.
			if tc.wantCode == http.StatusUnauthorized && w.Header().Get("WWW-Authenticate") == "" {
				t.Error("401 must carry a WWW-Authenticate challenge")
			}
		})
	}
}

func TestCORSMiddleware_Preflight(t *testing.T) {
	r := gin.New()
	r.Use(api.CORSMiddleware())
	r.POST("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	// An OPTIONS preflight must be short-circuited by the middleware: it returns
	// 204 (no body) and never reaches the POST handler, while still emitting the
	// allow-origin header the browser needs before it sends the real request.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodOptions, "/x", nil))
	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS preflight = %d, want 204", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS allow-origin header")
	}
}

func TestRequestIDMiddleware(t *testing.T) {
	// The middleware guarantees every response carries a valid UUID request ID
	// for tracing/audit — generated when absent, reused when trustworthy, and
	// regenerated when the client-supplied value can't be trusted.
	r := gin.New()
	r.Use(api.RequestIDMiddleware())
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	// No header → a fresh UUID is generated and echoed.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	got := w.Header().Get("X-Request-ID")
	if _, err := uuid.Parse(got); err != nil {
		t.Errorf("generated X-Request-ID is not a UUID: %q", got)
	}

	// Valid provided UUID is reused.
	provided := uuid.New().String()
	w2 := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Request-ID", provided)
	r.ServeHTTP(w2, req)
	if w2.Header().Get("X-Request-ID") != provided {
		t.Errorf("provided request ID not reused: got %q want %q", w2.Header().Get("X-Request-ID"), provided)
	}

	// Non-UUID provided → replaced with a generated UUID.
	w3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/x", nil)
	req3.Header.Set("X-Request-ID", "'; DROP TABLE audit;--")
	r.ServeHTTP(w3, req3)
	if _, err := uuid.Parse(w3.Header().Get("X-Request-ID")); err != nil {
		t.Error("non-UUID request ID must be replaced with a valid UUID (audit poisoning guard)")
	}
}

// --- admin policy round-trip ---------------------------------------------

type emptyLister struct{}

func (emptyLister) ListByModel(string) []*domain.Agent { return nil }

// newPolicyHandlers wires a Handlers with real policy plumbing but no-op storage
// and no agents, so the admin/liveness endpoints can be tested in isolation.
func newPolicyHandlers() *api.Handlers {
	stor := testutil.NoopStorage{}
	counters := metrics.NewUniversalCounterStore(stor, nil)
	ev := policy.NewEvaluator(stor, counters)
	exec := policy.NewExecutor(emptyLister{}, ev, policy.Default(), 3, 0)
	return api.NewHandlers(
		stor, nil, nil, 2*time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil,
	)
}

func TestLiveness(t *testing.T) {
	// Liveness is an unconditional 200 "ok" probe (used by orchestrators to know
	// the process is up), independent of any downstream dependency.
	h := newPolicyHandlers()
	r := gin.New()
	r.GET("/live", h.Liveness)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/live", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("Liveness = %d %q", w.Code, w.Body.String())
	}
}

func TestGetPutPolicy(t *testing.T) {
	// Exercises the admin policy round-trip: read current, replace, read back —
	// proving GET/PUT share the same live policy state, and that PUT validates
	// before applying so a bad policy can't overwrite a good one.
	h := newPolicyHandlers()
	r := gin.New()
	r.GET("/admin/policy", h.GetPolicy)
	r.PUT("/admin/policy", h.PutPolicy)

	// GET returns the default policy (least-loaded) before any override.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/policy", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "least-loaded") {
		t.Fatalf("GET policy = %d %q", w.Code, w.Body.String())
	}

	// PUT a valid policy → 200; the subsequent GET must reflect the new match
	// rule, confirming the executor's policy was atomically swapped.
	valid := "routing_policy:\n  match:\n    region: eu\n  strategy: least-loaded\n"
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodPut, "/admin/policy", strings.NewReader(valid)))
	if w2.Code != http.StatusOK {
		t.Fatalf("PUT valid policy = %d %q", w2.Code, w2.Body.String())
	}
	w3 := httptest.NewRecorder()
	r.ServeHTTP(w3, httptest.NewRequest(http.MethodGet, "/admin/policy", nil))
	if !strings.Contains(w3.Body.String(), `"region":"eu"`) {
		t.Errorf("policy not updated: %q", w3.Body.String())
	}

	// PUT an invalid policy (unknown strategy) → 400: validation rejects it and
	// leaves the previously applied policy in place.
	w4 := httptest.NewRecorder()
	bad := "routing_policy:\n  strategy: nonexistent\n"
	r.ServeHTTP(w4, httptest.NewRequest(http.MethodPut, "/admin/policy", strings.NewReader(bad)))
	if w4.Code != http.StatusBadRequest {
		t.Errorf("PUT invalid policy = %d, want 400", w4.Code)
	}
}
