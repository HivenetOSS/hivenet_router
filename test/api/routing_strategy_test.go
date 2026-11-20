// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

// Routing strategy per API key (routing_strategies) and per request
// (X-Hivenet-Routing-Strategy), through the same middleware chain as the
// redirect tests: a request may pick a strategy only from its key's list,
// the key's first strategy is its default, and a 307 carries the client's
// choice to the sibling site in its signed token.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/redirect"
)

// strategyKeys: key-1 (redirectKey) may route with strategies, first its
// default; key-2 (otherKey) has no list.
func strategyKeys(t *testing.T, strategies ...string) *auth.DynamicKeyProvider {
	t.Helper()
	keys := auth.NewDynamicKeyProvider()
	for i, k := range []struct {
		id, raw    string
		strategies []string
	}{{"key-1", redirectKey, strategies}, {"key-2", otherKey, nil}} {
		h := sha256.Sum256([]byte(k.raw))
		if err := keys.Upsert(string(rune('1'+i)), auth.DynamicKeyEntry{
			ID: k.id, KeyHash: hex.EncodeToString(h[:]), Owner: "tenant-" + k.id, Name: k.id, Enabled: true,
			Quota: auth.QuotaLimits{RequestsPerMinute: 1000}, RoutingStrategies: k.strategies,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func withStrategy(key, strategy string) map[string]string {
	h := map[string]string{"Authorization": "Bearer " + key}
	if strategy != "" {
		h[api.RoutingStrategyHeader] = strategy
	}
	return h
}

func TestRoutingStrategy_Resolution(t *testing.T) {
	cases := []struct {
		name, key, header    string
		wantStrategy, source string
	}{
		{"no header: the key's default", redirectKey, "", "prefix-aware", "key"},
		{"header within the key's list", redirectKey, "least-loaded", "least-loaded", "header"},
		{"header naming the default", redirectKey, "prefix-aware", "prefix-aware", "header"},
		{"key without a list: the model's policy decides", otherKey, "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSite(t, "a", map[string]int{"m": 1}, strategyKeys(t, "prefix-aware", "least-loaded"))
			done := sendAsync(s, chatPath, chatBody, withStrategy(tc.key, tc.header))
			p := serveOne(t, s)
			if w := <-done; w.Code != http.StatusOK {
				t.Fatalf("status %d, want 200; body %s", w.Code, w.Body)
			}
			if p.RoutingStrategy != tc.wantStrategy || p.RoutingStrategySource != tc.source {
				t.Errorf("strategy %q from %q, want %q from %q", p.RoutingStrategy, p.RoutingStrategySource, tc.wantStrategy, tc.source)
			}
		})
	}
}

// A strategy outside the key's list is refused with 403 before anything is
// queued or charged — also for a key with no list at all.
func TestRoutingStrategy_Forbidden(t *testing.T) {
	cases := []struct{ name, key, header string }{
		{"not in the key's list", redirectKey, "least-loaded"},
		{"unknown strategy", redirectKey, "round-robin"},
		{"key without a list", otherKey, "prefix-aware"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSite(t, "a", map[string]int{"m": 1}, strategyKeys(t, "prefix-aware"))
			w := send(s, chatPath, chatBody, withStrategy(tc.key, tc.header))
			if w.Code != http.StatusForbidden {
				t.Fatalf("status %d, want 403; body %s", w.Code, w.Body)
			}
			var resp domain.RouterErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Error == nil ||
				resp.Error.Code != domain.ErrCodeRoutingStrategyForbidden {
				t.Errorf("body %s, want code %s", w.Body, domain.ErrCodeRoutingStrategyForbidden)
			}
			if len(s.queue) != 0 || *s.charges != 0 {
				t.Errorf("queued %d, charged %d: want nothing", len(s.queue), *s.charges)
			}
		})
	}
}

// A 307 carries the strategy the client asked for, not the key's default;
// the target routes with it after checking it against the key, and ignores a
// header the follow-up request might carry.
func TestRoutingStrategy_AcrossRedirect(t *testing.T) {
	t.Run("the token carries only the client's choice", func(t *testing.T) {
		a := newSite(t, "a", nil, strategyKeys(t, "prefix-aware", "least-loaded"))
		push(t, fleetDoc(1), a)
		if _, c := locationToken(t, send(a, chatPath, chatBody, withStrategy(redirectKey, "least-loaded"))); c.Strategy != "least-loaded" {
			t.Errorf("token strategy %q, want least-loaded", c.Strategy)
		}
		if _, c := locationToken(t, send(a, chatPath, chatBody, withStrategy(redirectKey, ""))); c.Strategy != "" {
			t.Errorf("token strategy %q, want empty (the key's default is applied at the target)", c.Strategy)
		}
	})

	sign := func(t *testing.T, strategy string) string {
		return testSigner(t).Sign(redirect.Claims{KeyID: "key-1", RequestID: "11111111-2222-3333-4444-555555555555",
			Model: "m", Scope: redirect.ScopeAll, Strategy: strategy, From: "a", Site: "b", Path: chatPath,
			Hop: 1, BodySHA: redirect.BodySHA(chatBody)}, time.Now())
	}
	t.Run("the target applies the token's strategy", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, strategyKeys(t, "prefix-aware", "least-loaded"))
		push(t, fleetDoc(1), b)
		done := sendAsync(b, chatPath+"?rt="+sign(t, "least-loaded"), chatBody,
			map[string]string{api.RoutingStrategyHeader: "prefix-aware"})
		p := serveOne(t, b)
		if w := <-done; w.Code != http.StatusOK || p.RoutingStrategy != "least-loaded" || p.RoutingStrategySource != "header" {
			t.Fatalf("status %d strategy %q from %q, want 200 with least-loaded from header", w.Code, p.RoutingStrategy, p.RoutingStrategySource)
		}
	})
	t.Run("the target refuses a strategy the key no longer allows", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, strategyKeys(t, "prefix-aware"))
		push(t, fleetDoc(1), b)
		if w := send(b, chatPath+"?rt="+sign(t, "least-loaded"), chatBody, nil); w.Code != http.StatusForbidden {
			t.Fatalf("status %d, want 403; body %s", w.Code, w.Body)
		}
	})
}
