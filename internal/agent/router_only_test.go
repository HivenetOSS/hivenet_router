// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"hivenet_router/internal/transport/p2p"
)

// A request that does not come from an authenticated router must never reach
// the inference handlers.
func TestRouterOnlyRejectsUnknownCaller(t *testing.T) {
	a := &Agent{gater: p2p.NewRouterGater()}
	reached := false
	h := a.routerOnly(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))

	for _, path := range []string{"/v1/chat/completions", "/v1/models", "/health"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", path, rec.Code)
		}
	}
	if reached {
		t.Fatal("handler was reached by an unknown caller")
	}
}
