// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"hivenet_router/internal/policy"

	"github.com/gin-gonic/gin"
)

// A pushed key's routing_strategies must name implemented strategies, each
// once: a bad list is a 400 and never reaches the registry, a good one is
// stored in order — on both the single-key and the replace endpoints.
func TestAdminKeys_RoutingStrategies(t *testing.T) {
	keyBody := func(strategies []string) map[string]any {
		return map[string]any{
			"key_hash": strings.Repeat("ab", 32), "owner": "acme", "name": "k", "enabled": true,
			"routing_strategies": strategies,
		}
	}
	upsert := func(t *testing.T, strategies []string) (*httptest.ResponseRecorder, func(string) []string) {
		h, reg := newAdminKeyHandlers(policy.Default())
		b := keyBody(strategies)
		b["version"] = "v1"
		body, _ := json.Marshal(b)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPut, "/admin/api-keys/k1", bytes.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Params = gin.Params{{Key: "id", Value: "k1"}}
		h.UpsertAPIKey(c)
		return w, func(id string) []string { e, _ := reg.GetKey(id); return e.RoutingStrategies }
	}

	t.Run("valid list is stored in order", func(t *testing.T) {
		w, stored := upsert(t, []string{"least-loaded", "prefix-aware"})
		if w.Code != http.StatusOK {
			t.Fatalf("status %d, want 200; body %s", w.Code, w.Body)
		}
		if got := stored("k1"); !slices.Equal(got, []string{"least-loaded", "prefix-aware"}) {
			t.Errorf("stored %v, want [least-loaded prefix-aware]", got)
		}
	})
	for _, bad := range [][]string{{"round-robin"}, {"prefix-aware", "prefix-aware"}} {
		t.Run("rejected: "+strings.Join(bad, ","), func(t *testing.T) {
			w, stored := upsert(t, bad)
			if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "routing_strategies") {
				t.Fatalf("status %d body %s, want 400 naming routing_strategies", w.Code, w.Body)
			}
			if got := stored("k1"); got != nil {
				t.Errorf("rejected key reached the registry with %v", got)
			}
		})
	}

	t.Run("replace endpoint validates too", func(t *testing.T) {
		h, reg := newAdminKeyHandlers(policy.Default())
		k := keyBody([]string{"round-robin"})
		k["id"] = "k1"
		body, _ := json.Marshal(map[string]any{"version": "v1", "keys": []any{k}})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/admin/api-keys/replace", bytes.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		h.ReplaceAPIKeys(c)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400; body %s", w.Code, w.Body)
		}
		if _, ok := reg.GetKey("k1"); ok {
			t.Error("rejected key reached the registry")
		}
	})
}

// scope_max and home_country are validated on push: an invalid scope, an
// invalid country, or a home outside scope_max is a 400.
func TestAdminKeys_Scope(t *testing.T) {
	cases := []struct {
		max, home string
		ok        bool
	}{
		{"", "", true}, {"EU", "FR", true}, {"country:FR", "FR", true}, {"all", "US", true},
		{"europe", "", false}, {"EU", "fr", false}, {"EU", "US", false}, {"country:FR", "DE", false},
	}
	for _, tc := range cases {
		h, reg := newAdminKeyHandlers(policy.Default())
		body, _ := json.Marshal(map[string]any{"version": "v1", "key_hash": strings.Repeat("ab", 32), "owner": "o", "name": "k",
			"enabled": true, "scope_max": tc.max, "home_country": tc.home})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPut, "/admin/api-keys/k1", bytes.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Params = gin.Params{{Key: "id", Value: "k1"}}
		h.UpsertAPIKey(c)
		if (w.Code == http.StatusOK) != tc.ok {
			t.Errorf("scope_max %q home %q: status %d (%s), want ok=%v", tc.max, tc.home, w.Code, w.Body, tc.ok)
		}
		if e, found := reg.GetKey("k1"); tc.ok && (!found || e.ScopeMax != tc.max || e.HomeCountry != tc.home) {
			t.Errorf("scope_max %q home %q: stored %+v", tc.max, tc.home, e)
		}
	}
}

// external_fallback: pushed with the client's provider key, read back with
// its preview only; refused on a key bound to a scope.
func TestAdminKeys_ExternalFallback(t *testing.T) {
	const cred = "sk-client-0123456789abcdef"
	put := func(h interface {
		UpsertAPIKey(*gin.Context)
	}, extra map[string]any) *httptest.ResponseRecorder {
		b := map[string]any{"version": "v1", "key_hash": strings.Repeat("ab", 32), "owner": "o", "name": "k", "enabled": true,
			"external_fallback": map[string]any{"provider": "openai", "model": "gpt-4o-mini", "api_key": cred}}
		for k, v := range extra {
			b[k] = v
		}
		body, _ := json.Marshal(b)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPut, "/admin/api-keys/k1", bytes.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Params = gin.Params{{Key: "id", Value: "k1"}}
		h.UpsertAPIKey(c)
		return w
	}
	h, reg := newAdminKeyHandlers(policy.Default())
	if w := put(h, nil); w.Code != http.StatusOK {
		t.Fatalf("push: %d %s", w.Code, w.Body)
	}
	if e, _ := reg.GetKey("k1"); e.ExternalFallback == nil || e.ExternalFallback.APIKey.Reveal() != cred {
		t.Fatalf("stored %+v", e.ExternalFallback)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/admin/api-keys/k1", nil)
	c.Params = gin.Params{{Key: "id", Value: "k1"}}
	h.GetAPIKey(c)
	if strings.Contains(w.Body.String(), "0123456789") || !strings.Contains(w.Body.String(), "sk-...def") {
		t.Errorf("GET shows %s, want the preview only", w.Body)
	}
	h2, _ := newAdminKeyHandlers(policy.Default())
	if w := put(h2, map[string]any{"scope_max": "EU"}); w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), "0123456789") {
		t.Errorf("scoped key with external fallback: %d %s, want 400 without the credential", w.Code, w.Body)
	}

	// A read-modify-write keeps the stored credential: api_key sent back as
	// the preview a read returned, or left out.
	for i, fb := range []map[string]any{
		{"provider": "openai", "model": "gpt-4o", "api_key": "sk-...def"},
		{"provider": "openai", "model": "gpt-4o"},
	} {
		if w := put(h, map[string]any{"version": fmt.Sprintf("v%d", i+2), "external_fallback": fb}); w.Code != http.StatusOK {
			t.Fatalf("write-back %d: %d %s", i, w.Code, w.Body)
		}
		if e, _ := reg.GetKey("k1"); e.ExternalFallback.APIKey.Reveal() != cred || e.ExternalFallback.Model != "gpt-4o" {
			t.Errorf("write-back %d stored %q / %s, want the credential kept and the new model", i, e.ExternalFallback.APIKey.Preview(), e.ExternalFallback.Model)
		}
	}
	// A preview with nothing stored to keep (or for another provider) is refused.
	h3, _ := newAdminKeyHandlers(policy.Default())
	if w := put(h3, map[string]any{"external_fallback": map[string]any{"provider": "openai", "model": "m", "api_key": "sk-...def"}}); w.Code != http.StatusBadRequest {
		t.Errorf("preview as a new credential: %d %s, want 400", w.Code, w.Body)
	}
	if w := put(h, map[string]any{"version": "v9", "external_fallback": map[string]any{"provider": "anthropic", "model": "m", "api_key": "sk-...def"}}); w.Code != http.StatusBadRequest {
		t.Errorf("preview for another provider: %d %s, want 400", w.Code, w.Body)
	}
}
