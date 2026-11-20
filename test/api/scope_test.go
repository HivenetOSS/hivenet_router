// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

// Sovereignty: a key's scope_max bounds X-Hivenet-Scope (a request narrows,
// never widens), its home_country is the default, and the resolved scope
// reaches the processor (agent choice) and bounds a redirect.

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/redirect"
)

// scopedKeys: key-1 (redirectKey) with the given scope fields; key-2
// (otherKey) with none.
func scopedKeys(t *testing.T, scopeMax, home string) *auth.DynamicKeyProvider {
	t.Helper()
	keys := auth.NewDynamicKeyProvider()
	for i, k := range []struct{ id, raw, max, home string }{{"key-1", redirectKey, scopeMax, home}, {"key-2", otherKey, "", ""}} {
		h := sha256.Sum256([]byte(k.raw))
		if err := keys.Upsert(string(rune('1'+i)), auth.DynamicKeyEntry{
			ID: k.id, KeyHash: hex.EncodeToString(h[:]), Owner: "tenant-" + k.id, Name: k.id, Enabled: true,
			Quota: auth.QuotaLimits{RequestsPerMinute: 1000}, ScopeMax: k.max, HomeCountry: k.home,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestScope_Resolution(t *testing.T) {
	cases := []struct {
		name, max, home, key, header string
		want                         domain.Scope
	}{
		{"no key scope, no header: none", "", "", otherKey, "", ""},
		{"no key scope, header", "", "", otherKey, "EU", domain.ScopeEU},
		{"home country is the default", "EU", "FR", redirectKey, "", "country:FR"},
		{"scope_max is the default without a home", "EU", "", redirectKey, "", domain.ScopeEU},
		{"header narrows scope_max", "EU", "FR", redirectKey, "country:DE", "country:DE"},
		{"header equal to scope_max", "EU", "FR", redirectKey, "EU", domain.ScopeEU},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSite(t, "a", map[string]int{"m": 1}, scopedKeys(t, tc.max, tc.home))
			h := map[string]string{"Authorization": "Bearer " + tc.key}
			if tc.header != "" {
				h["X-Hivenet-Scope"] = tc.header
			}
			done := sendAsync(s, chatPath, chatBody, h)
			p := serveOne(t, s)
			if w := <-done; w.Code != http.StatusOK || p.Scope != tc.want {
				t.Fatalf("status %d scope %q, want 200 and %q", w.Code, p.Scope, tc.want)
			}
		})
	}
}

func TestScope_Refused(t *testing.T) {
	cases := []struct {
		name, max, header string
		code              int
		errCode           domain.ErrorCode
	}{
		{"wider than scope_max (all > EU)", "EU", "all", http.StatusForbidden, domain.ErrCodeScopeForbidden},
		{"outside scope_max (US not in EU)", "EU", "country:US", http.StatusForbidden, domain.ErrCodeScopeForbidden},
		{"another country", "country:FR", "country:DE", http.StatusForbidden, domain.ErrCodeScopeForbidden},
		{"invalid header", "", "mars", http.StatusBadRequest, domain.ErrCodeRequestInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSite(t, "a", map[string]int{"m": 1}, scopedKeys(t, tc.max, ""))
			w := send(s, chatPath, chatBody, scoped(tc.header))
			if w.Code != tc.code || !strings.Contains(w.Body.String(), string(tc.errCode)) {
				t.Fatalf("status %d body %s, want %d %s", w.Code, w.Body, tc.code, tc.errCode)
			}
			if len(s.queue) != 0 || *s.charges != 0 {
				t.Errorf("queued %d, charged %d: want nothing", len(s.queue), *s.charges)
			}
		})
	}
}

// The key's scope bounds the redirect: a FR-only key is not sent to the DE
// sibling; an EU key may be.
func TestScope_BoundsTheRedirect(t *testing.T) {
	a := newSite(t, "a", nil, scopedKeys(t, "country:FR", ""))
	doc := fleetDoc(1)
	doc.Placements = []redirect.Placement{placement("m", redirect.KindChat, "c")} // m on c (DE) only
	push(t, doc, a)
	if w := send(a, chatPath, chatBody, bearer()); w.Code == http.StatusTemporaryRedirect {
		t.Fatalf("FR-only key redirected to DE: %s", w.Header().Get("Location"))
	}
	a2 := newSite(t, "a", nil, scopedKeys(t, "EU", ""))
	push(t, doc, a2)
	if _, c := locationToken(t, send(a2, chatPath, chatBody, bearer())); c.Scope != "EU" {
		t.Errorf("token scope %q, want EU (the key's scope_max)", c.Scope)
	}
}

// At the target, a token scope wider than the key's scope_max there is refused.
func TestScope_TargetChecksTheKey(t *testing.T) {
	b := newSite(t, "b", map[string]int{"m": 1}, scopedKeys(t, "country:FR", ""))
	push(t, fleetDoc(1), b)
	tok := testSigner(t).Sign(redirect.Claims{KeyID: "key-1", RequestID: "11111111-2222-3333-4444-555555555555",
		Model: "m", Scope: redirect.ScopeAll, ScopeAgents: true, From: "a", Site: "b", Path: chatPath, Hop: 1, BodySHA: redirect.BodySHA(chatBody)}, time.Now())
	if w := send(b, chatPath+"?rt="+tok, chatBody, nil); w.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403; body %s", w.Code, w.Body)
	}
}

// A request whose client asked for no scope (a key without scope fields, no
// header) is redirected only inside the first site's country, but that
// default never filters the target's agents: an agent that declares no
// country still serves it. A scope from the client or the key does.
func TestScope_DefaultRedirectScopeDoesNotFilterAgents(t *testing.T) {
	a := newSite(t, "a", nil, scopedKeys(t, "", ""))
	push(t, fleetDoc(1), a)
	_, c := locationToken(t, send(a, chatPath, chatBody, map[string]string{"Authorization": "Bearer " + otherKey}))
	if c.Scope != "country:FR" || c.ScopeAgents {
		t.Fatalf("token scope %q (agents %v), want country:FR for the sites only", c.Scope, c.ScopeAgents)
	}
	_, c = locationToken(t, send(a, chatPath, chatBody, map[string]string{"Authorization": "Bearer " + otherKey, "X-Hivenet-Scope": "EU"}))
	if c.Scope != "EU" || !c.ScopeAgents {
		t.Fatalf("token scope %q (agents %v), want EU bounding the agents too", c.Scope, c.ScopeAgents)
	}

	sign := func(scope redirect.Scope, agents bool) string {
		return testSigner(t).Sign(redirect.Claims{KeyID: "key-2", RequestID: "11111111-2222-3333-4444-555555555555",
			Model: "m", Scope: scope, ScopeAgents: agents, From: "a", Site: "b", Path: chatPath, Hop: 1,
			BodySHA: redirect.BodySHA(chatBody)}, time.Now())
	}
	for _, tc := range []struct {
		name   string
		agents bool
		want   domain.Scope
	}{{"default scope: agents not filtered", false, ""}, {"client scope: agents filtered", true, "country:FR"}} {
		t.Run(tc.name, func(t *testing.T) {
			b := newSite(t, "b", map[string]int{"m": 1}, scopedKeys(t, "", ""))
			push(t, fleetDoc(1), b)
			done := sendAsync(b, chatPath+"?rt="+sign("country:FR", tc.agents), chatBody, nil)
			p := serveOne(t, b)
			if w := <-done; w.Code != http.StatusOK || p.Scope != tc.want {
				t.Fatalf("status %d scope %q, want 200 and %q", w.Code, p.Scope, tc.want)
			}
		})
	}
}
