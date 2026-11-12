// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/metrics"
)

// TestMetricsHandler_BearerToken: without a token /metrics is open (as
// before); with one, a request without it, or with a wrong one, gets 401 and
// no series, and the right one gets the metrics. The tenant series (which
// name every tenant and key) are what the token protects.
func TestMetricsHandler_BearerToken(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.TenantSetQuotaLimit("tenant-secret-name", 10)
	get := func(h http.Handler, auth string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		h.ServeHTTP(w, r)
		return w
	}

	if w := get(m.MetricsHandler(""), ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "tenant-secret-name") {
		t.Fatalf("no token: status %d, want 200 with the tenant series", w.Code)
	}
	h := m.MetricsHandler("s3cret-token")
	for name, auth := range map[string]string{
		"missing": "", "wrong": "Bearer nope", "no scheme": "s3cret-token", "prefix": "Bearer s3cret", "basic": "Basic czNjcmV0LXRva2Vu",
	} {
		w := get(h, auth)
		if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "tenant-secret-name") || w.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s credential: status %d, want 401 with WWW-Authenticate and no series", name, w.Code)
		}
	}
	if w := get(h, "Bearer s3cret-token"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "tenant-secret-name") {
		t.Errorf("right token: status %d, want 200 with the series", w.Code)
	}
}

// TestIsWildcardAddr: a listen address with no host or an unspecified IP
// accepts connections on every interface; a specific IP or hostname does
// not; an unparsable address counts as wildcard (the safe answer).
func TestIsWildcardAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		":2112": true, "0.0.0.0:2112": true, "[::]:2112": true, "2112": true, "": true,
		"127.0.0.1:2112": false, "10.0.0.5:2112": false, "[::1]:2112": false, "localhost:2112": false,
	} {
		if got := metrics.IsWildcardAddr(addr); got != want {
			t.Errorf("IsWildcardAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}
