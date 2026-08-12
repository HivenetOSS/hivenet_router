// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for the dynamic admin key API's admission validation: a key
// created at runtime must pass the same cross-config invariants a static
// auth.yaml key passes at startup — an ITPM bucket that cannot hold one
// maximum-size prompt on a reachable serverless model is rejected with 400
// instead of silently capping that key's usable context.
package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/policy"

	"github.com/gin-gonic/gin"
)

// newAdminKeyHandlers builds a Handlers with a live dynamic key registry and
// an executor serving pol for every model.
func newAdminKeyHandlers(pol *policy.Policy) (*api.Handlers, *auth.DynamicKeyProvider) {
	exec := policy.NewExecutor(nil, nil, pol, 0, 0)
	reg := auth.NewDynamicKeyProvider()
	h := api.NewHandlers(
		nil, nil, nil, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, reg, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil,
	)
	return h, reg
}

// upsertKey drives PUT /admin/api-keys/k1 with the given quota block and
// occupancy share, returning the recorder.
func upsertKey(t *testing.T, h *api.Handlers, quota map[string]any, share float64) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"version":             "v1",
		"key_hash":            strings.Repeat("ab", 32),
		"owner":               "acme",
		"name":                "runtime-key",
		"enabled":             true,
		"quota":               quota,
		"max_occupancy_share": share,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPut, "/admin/api-keys/k1", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "id", Value: "k1"}}
	h.UpsertAPIKey(c)
	return w
}

// TestAdminUpsert_RejectsITPMBelowMaxInput verifies the D16 invariant at key
// creation: on a serverless policy with max_input_tokens 262144, a key whose
// ITPM bucket is smaller can never admit a full-context prompt, so the upsert
// is a 400 — the runtime analogue of the static loader refusing to start.
func TestAdminUpsert_RejectsITPMBelowMaxInput(t *testing.T) {
	h, reg := newAdminKeyHandlers(&policy.Policy{Mode: policy.ModeServerless, MaxInputTokens: 262144})
	w := upsertKey(t, h, map[string]any{"input_tokens_per_minute": 100000}, 0)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an ITPM below max_input_tokens, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "input_tokens_per_minute") {
		t.Errorf("error must name the violated invariant, got %s", w.Body.String())
	}
	if _, n := reg.Version(); n != 0 {
		t.Errorf("a rejected key must not enter the registry, got %d keys", n)
	}
}

// TestAdminUpsert_AcceptsCompliantKey verifies a key whose ITPM covers the
// context and whose share is in range is accepted, and that the share reaches
// the stored quota (the value the B4 occupancy gate reads).
func TestAdminUpsert_AcceptsCompliantKey(t *testing.T) {
	h, reg := newAdminKeyHandlers(&policy.Policy{Mode: policy.ModeServerless, MaxInputTokens: 262144})
	w := upsertKey(t, h, map[string]any{"input_tokens_per_minute": 524288}, 0.4)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for a compliant key, got %d (%s)", w.Code, w.Body.String())
	}
	key, ok := reg.GetKey("k1")
	if !ok {
		t.Fatal("key not found after upsert")
	}
	if key.Quota.MaxOccupancyShare != 0.4 {
		t.Errorf("max_occupancy_share = %v, want 0.4", key.Quota.MaxOccupancyShare)
	}
}

// TestAdminUpsert_RejectsShareOutOfRange verifies the registry's share bound
// surfaces as a 400 through the admin API.
func TestAdminUpsert_RejectsShareOutOfRange(t *testing.T) {
	h, _ := newAdminKeyHandlers(&policy.Policy{})
	w := upsertKey(t, h, map[string]any{}, 1.5)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for share 1.5, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "max_occupancy_share") {
		t.Errorf("error must name max_occupancy_share, got %s", w.Body.String())
	}
}

// TestAdminUpsert_ReservedPolicySkipsITPMCheck verifies the cross-check only
// binds where per-key caps apply: on a reserved-mode policy a small ITPM is
// accepted (B4 is inert there).
func TestAdminUpsert_ReservedPolicySkipsITPMCheck(t *testing.T) {
	h, _ := newAdminKeyHandlers(&policy.Policy{Mode: policy.ModeReserved, MaxInputTokens: 262144})
	w := upsertKey(t, h, map[string]any{"input_tokens_per_minute": 100000}, 0)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on a reserved policy, got %d (%s)", w.Code, w.Body.String())
	}
}
