// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

// Tenant-isolation checklist (test/isolation/CHECKLIST.md), API layer. These
// tests run requests through the real route table and middleware chain of
// api.Server, so they also catch a route added outside its auth group.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/redirect"

	"github.com/gin-gonic/gin"
)

const (
	isoKeyA     = "sk-iso-tenant-a"
	isoKeyB     = "sk-iso-tenant-b"
	isoAdminKey = "sk-iso-admin"
)

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// isoServer is a full api.Server with two static-key tenants (tenant-a,
// tenant-b), one admin key, and a queue standing in for the processor.
type isoServer struct {
	srv     *api.Server
	apiProv auth.Provider
	queue   chan *domain.PendingRequest
}

// isoKeys are the two tenants' keys newIsoServer serves: no allowlist, no
// quota.
func isoKeys() []auth.APIKeyEntry {
	return []auth.APIKeyEntry{
		{KeyHash: sha256Hex(isoKeyA), KeyPreview: "sk-...a", Metadata: auth.KeyMetadata{Name: "a", Owner: "tenant-a"}},
		{KeyHash: sha256Hex(isoKeyB), KeyPreview: "sk-...b", Metadata: auth.KeyMetadata{Name: "b", Owner: "tenant-b"}},
	}
}

func newIsoServer(t *testing.T) *isoServer {
	t.Helper()
	return newIsoServerWith(t, isoKeys(), nil)
}

// newIsoServerWith serves the given static keys, with one healthy LLM agent
// per name in models for the discovery routes.
func newIsoServerWith(t *testing.T, keys []auth.APIKeyEntry, models []string) *isoServer {
	t.Helper()
	apiProv, err := auth.NewStaticKeyProvider(keys)
	if err != nil {
		t.Fatal(err)
	}
	adminProv, err := auth.NewStaticAdminKeyProvider([]string{isoAdminKey})
	if err != nil {
		t.Fatal(err)
	}
	q := make(chan *domain.PendingRequest, 8)
	lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000}
	healthy := func(string) int { return 1 }
	h := api.NewHandlers(
		aliasStorage{models: models}, nil, q, 2*time.Second,
		nil, nil, nil, lim,
		nil, nil, nil, nil, nil, nil,
		healthy,
		nil, nil, nil, nil, nil, nil, nil,
	)
	srv := api.NewServer(h, ":0", apiProv, adminProv, lim, metrics.NewRouterMetrics(), healthy, 1<<20)
	return &isoServer{srv: srv, apiProv: apiProv, queue: q}
}

// keyIDOf is the key ID the provider itself assigns to token: what every
// per-key counter, metric label and audit line must carry for it.
func (s *isoServer) keyIDOf(t *testing.T, token string) string {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	res, err := s.apiProv.Authenticate(r)
	if err != nil {
		t.Fatalf("authenticate %q: %v", token, err)
	}
	return res.KeyID
}

func (s *isoServer) do(method, path string, body []byte, header map[string]string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		r.Header.Set(k, v)
	}
	s.srv.ServeHTTP(w, r)
	return w
}

// serve answers the next queued request and returns it.
func (s *isoServer) serve(t *testing.T) *domain.PendingRequest {
	t.Helper()
	select {
	case p := <-s.queue:
		p.Response <- &domain.ChatResponse{RawBytes: []byte(`{"served":true}`)}
		return p
	case <-time.After(2 * time.Second):
		t.Fatal("request was not queued")
		return nil
	}
}

func bearerOf(key string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + key}
}

// isoInferenceBodies is one valid body per inference endpoint. The bodies
// carry fields a client might use to claim another tenant's identity.
var isoInferenceBodies = map[string]string{
	"/v1/chat/completions":      `{"model":"m","user":"tenant-b","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`,
	"/v1/messages":              `{"model":"m","max_tokens":5,"metadata":{"user_id":"tenant-b"},"messages":[{"role":"user","content":"hi"}]}`,
	"/v1/messages/count_tokens": `{"model":"m","metadata":{"user_id":"tenant-b"},"messages":[{"role":"user","content":"hi"}]}`,
	"/v1/embeddings":            `{"model":"e","user":"tenant-b","input":"x"}`,
	"/v1/rerank":                `{"model":"r","query":"q","documents":["d"]}`,
}

type isoCall struct{ path, body string }

// isoInferenceCalls returns one request per POST /v1 route of the real route
// table, so a new inference endpoint is covered as soon as it exists: a route
// with no body in isoInferenceBodies fails the test instead of being skipped.
func isoInferenceCalls(t *testing.T, s *isoServer) []isoCall {
	t.Helper()
	var calls []isoCall
	for _, rt := range s.srv.Routes() {
		if rt.Method != http.MethodPost || !strings.HasPrefix(rt.Path, "/v1/") {
			continue
		}
		body, ok := isoInferenceBodies[rt.Path]
		if !ok {
			t.Errorf("POST %s has no request body in isoInferenceBodies: add one so the isolation tests cover it", rt.Path)
			continue
		}
		calls = append(calls, isoCall{rt.Path, body})
	}
	if len(calls) == 0 {
		t.Fatal("no POST /v1 routes found")
	}
	return calls
}

// TestIsolation_TenantFromKeyOnly: the tenant and key ID on the queued
// request come only from the presented credential. Headers and body fields
// that name another tenant or key change nothing, on every inference
// endpoint.
func TestIsolation_TenantFromKeyOnly(t *testing.T) {
	s := newIsoServer(t)
	wantKeyID := s.keyIDOf(t, isoKeyA)
	if !strings.HasPrefix(wantKeyID, "static-") {
		t.Fatalf("static key ID %q, want a derived static- ID", wantKeyID)
	}
	spoof := bearerOf(isoKeyA)
	for _, h := range []string{"X-Tenant-ID", "X-Organization", "X-Owner", "X-Key-ID", "X-API-Key-ID", "OpenAI-Organization"} {
		spoof[h] = "tenant-b"
	}
	for _, tc := range isoInferenceCalls(t, s) {
		t.Run(tc.path, func(t *testing.T) {
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- s.do(http.MethodPost, tc.path, []byte(tc.body), spoof) }()
			p := s.serve(t)
			if w := <-done; w.Code != http.StatusOK {
				t.Fatalf("status %d, want 200; body %s", w.Code, w.Body)
			}
			if p.TenantID != "tenant-a" {
				t.Errorf("queued TenantID %q, want tenant-a (from the key, not the headers or body)", p.TenantID)
			}
			if p.KeyID != wantKeyID {
				t.Errorf("queued KeyID %q, want %q (the key's own ID)", p.KeyID, wantKeyID)
			}
		})
	}
}

// isoPublicRoutes are the only routes outside /v1 and /admin, and the only
// ones that answer without a credential.
var isoPublicRoutes = map[string]bool{"GET /health": true}

// TestIsolation_NoUnclassifiedRoutes: every route is under /v1 (API keys),
// under /admin (admin keys), or on the short public list. A new root route
// (say /debug) fails here instead of escaping both auth walks.
func TestIsolation_NoUnclassifiedRoutes(t *testing.T) {
	s := newIsoServer(t)
	for _, rt := range s.srv.Routes() {
		if strings.HasPrefix(rt.Path, "/v1/") || strings.HasPrefix(rt.Path, "/admin/") || isoPublicRoutes[rt.Method+" "+rt.Path] {
			continue
		}
		t.Errorf("%s %s is neither under /v1 or /admin nor on the public list: put it behind an auth group or list it in isoPublicRoutes", rt.Method, rt.Path)
	}
}

// TestIsolation_DiscoveryHidesModelsOutsideAllowlist: /v1/models lists, and
// /v1/models/:model describes, only the models the calling key may use,
// whether its allowlist comes from models (allowed_models) or quota.per_model.
// A key without either sees every model. The answer for a hidden model is the
// same 404 as for a model that does not exist.
func TestIsolation_DiscoveryHidesModelsOutsideAllowlist(t *testing.T) {
	const (
		keyListed   = "sk-iso-listed"
		keyPerModel = "sk-iso-per-model"
		keyOpen     = "sk-iso-open"
	)
	ten, rpm := 10, 10
	s := newIsoServerWith(t, []auth.APIKeyEntry{
		{KeyHash: sha256Hex(keyListed), Metadata: auth.KeyMetadata{Name: "listed", Owner: "tenant-a"}, Models: []string{"m"}},
		{KeyHash: sha256Hex(keyPerModel), Metadata: auth.KeyMetadata{Name: "per-model", Owner: "tenant-b"},
			Quota: auth.QuotaConfig{PerModel: map[string]*auth.PerModelQuotaConfig{"e": {RequestsPerMinutePerReplica: &rpm, TokensPerDay: &ten}}}},
		{KeyHash: sha256Hex(keyOpen), Metadata: auth.KeyMetadata{Name: "open", Owner: "tenant-c"}},
	}, []string{"m", "e", "secret"})

	listed := func(t *testing.T, key string) []string {
		t.Helper()
		w := s.do(http.MethodGet, "/v1/models", nil, bearerOf(key))
		if w.Code != http.StatusOK {
			t.Fatalf("GET /v1/models: status %d, body %s", w.Code, w.Body)
		}
		var resp struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(resp.Data))
		for _, d := range resp.Data {
			ids = append(ids, d.ID)
		}
		sort.Strings(ids)
		return ids
	}
	for _, tc := range []struct {
		key     string
		visible []string
	}{
		{keyListed, []string{"m"}},
		{keyPerModel, []string{"e"}},
		{keyOpen, []string{"e", "m", "secret"}},
	} {
		t.Run(tc.key, func(t *testing.T) {
			if got := listed(t, tc.key); strings.Join(got, ",") != strings.Join(tc.visible, ",") {
				t.Errorf("GET /v1/models lists %v, want %v", got, tc.visible)
			}
			for _, m := range []string{"m", "e", "secret"} {
				want := http.StatusNotFound
				if slices.Contains(tc.visible, m) {
					want = http.StatusOK
				}
				if w := s.do(http.MethodGet, "/v1/models/"+m, nil, bearerOf(tc.key)); w.Code != want {
					t.Errorf("GET /v1/models/%s: status %d, want %d", m, w.Code, want)
				}
			}
			unknown := s.do(http.MethodGet, "/v1/models/no-such-model", nil, bearerOf(tc.key))
			hidden := s.do(http.MethodGet, "/v1/models/secret", nil, bearerOf(tc.key))
			if !slices.Contains(tc.visible, "secret") && hidden.Body.String() != strings.ReplaceAll(unknown.Body.String(), "no-such-model", "secret") {
				t.Errorf("a hidden model answers differently from an unknown one:\nhidden  %s\nunknown %s", hidden.Body, unknown.Body)
			}
		})
	}
}

// TestIsolation_V1RoutesRequireKey: every route under /v1 answers 401 without
// a key and with an unknown key. Walks the real route table, so a route added
// outside the auth group fails here.
func TestIsolation_V1RoutesRequireKey(t *testing.T) {
	s := newIsoServer(t)
	seen := 0
	for _, rt := range s.srv.Routes() {
		if !strings.HasPrefix(rt.Path, "/v1/") {
			continue
		}
		seen++
		path := strings.ReplaceAll(rt.Path, "*model", "m")
		for name, hdr := range map[string]map[string]string{
			"no key":      nil,
			"unknown key": bearerOf("sk-not-a-key"),
			"admin key":   bearerOf(isoAdminKey),
		} {
			if w := s.do(rt.Method, path, []byte(`{"model":"m"}`), hdr); w.Code != http.StatusUnauthorized {
				t.Errorf("%s %s with %s: status %d, want 401", rt.Method, rt.Path, name, w.Code)
			}
		}
	}
	if seen == 0 {
		t.Fatal("no /v1 routes found")
	}
	if len(s.queue) != 0 {
		t.Errorf("%d requests queued, want none", len(s.queue))
	}
}

// TestIsolation_AdminRoutesRejectAPIKeys: every route under /admin answers
// 401 without credentials and with a tenant's API key. An admin key gets past
// auth (any status but 401).
func TestIsolation_AdminRoutesRejectAPIKeys(t *testing.T) {
	s := newIsoServer(t)
	seen := 0
	for _, rt := range s.srv.Routes() {
		if !strings.HasPrefix(rt.Path, "/admin/") {
			continue
		}
		seen++
		path := strings.NewReplacer(":id", "key-1", "*model", "m", "*name", "p").Replace(rt.Path)
		for name, hdr := range map[string]map[string]string{
			"no key":       nil,
			"tenant-a key": bearerOf(isoKeyA),
			"tenant-b key": bearerOf(isoKeyB),
		} {
			if w := s.do(rt.Method, path, nil, hdr); w.Code != http.StatusUnauthorized {
				t.Errorf("%s %s with %s: status %d, want 401", rt.Method, rt.Path, name, w.Code)
			}
		}
	}
	if seen == 0 {
		t.Fatal("no /admin routes found")
	}
	if w := s.do(http.MethodGet, "/admin/api-keys/version", nil, bearerOf(isoAdminKey)); w.Code == http.StatusUnauthorized {
		t.Errorf("admin key on /admin: status 401, want it past auth")
	}
}

// TestIsolation_AdminKeyIsNotAnAPIKey: an admin key cannot be used as a
// tenant's API key on inference, so an operator credential never makes
// requests that land in some tenant's counters or logs.
func TestIsolation_AdminKeyIsNotAnAPIKey(t *testing.T) {
	s := newIsoServer(t)
	for _, tc := range isoInferenceCalls(t, s) {
		if w := s.do(http.MethodPost, tc.path, []byte(tc.body), bearerOf(isoAdminKey)); w.Code != http.StatusUnauthorized {
			t.Errorf("%s with the admin key: status %d, want 401", tc.path, w.Code)
		}
	}
	if len(s.queue) != 0 {
		t.Errorf("%d requests queued, want none", len(s.queue))
	}
}

// TestIsolation_RedirectTokenCannotWidenScope: at a redirect target, the
// scope signed into the token is authoritative. An X-Hivenet-Scope header
// cannot widen it on the next hop.
func TestIsolation_RedirectTokenCannotWidenScope(t *testing.T) {
	const rid = "11111111-2222-3333-4444-555555555555"
	sign := func(t *testing.T, scope redirect.Scope) string {
		return testSigner(t).Sign(redirect.Claims{KeyID: "key-1", RequestID: rid, Model: "m", Scope: scope,
			From: "a", Site: "b", Path: chatPath, Hop: 1, BodySHA: redirect.BodySHA(chatBody)}, time.Now())
	}

	// b has no agent for m. With scope country:FR the only other FR site
	// is a, the origin, so there is nowhere to go: served here. Asking for
	// "all" must not open c (DE).
	t.Run("country:FR stays FR", func(t *testing.T) {
		b := newSite(t, "b", nil, testKeys(t))
		push(t, fleetDoc(1), b)
		hdr := map[string]string{"X-Hivenet-Scope": "all"}
		done := sendAsync(b, chatPath+"?rt="+sign(t, "country:FR"), chatBody, hdr)
		serveOne(t, b)
		if w := <-done; w.Header().Get("Location") != "" {
			t.Fatalf("redirected to %s; a country:FR token must not reach another country", w.Header().Get("Location"))
		}
	})

	// The header is ignored in the other direction too: a token scoped "all"
	// goes on to c even if the client asks for country:FR.
	t.Run("token scope is carried, not the header", func(t *testing.T) {
		b := newSite(t, "b", nil, testKeys(t))
		push(t, fleetDoc(1), b)
		hdr := map[string]string{"X-Hivenet-Scope": "country:FR"}
		loc, next := locationToken(t, send(b, chatPath+"?rt="+sign(t, redirect.ScopeAll), chatBody, hdr))
		if loc.Host != "c.example.com" || next.Scope != redirect.ScopeAll {
			t.Fatalf("Location %s scope %q, want c with the token's scope all", loc, next.Scope)
		}
	})
}

// TestIsolation_ScopeMaxCapsKeyScope: a key's scope_max ceiling caps the
// scope a request may ask for. Owned by Epic F (W5.5), which adds scope_max.
func TestIsolation_ScopeMaxCapsKeyScope(t *testing.T) {
	t.Skip("owned by Epic F (W5.5): scope_max does not exist yet")
}

// TestIsolation_StaticKeysHaveDistinctB4State: two static auth.yaml keys of
// one tenant each get their own input-token-per-minute bucket, output-token-
// per-minute bucket and occupancy share. Before static keys had an ID, every
// static key of every tenant shared one "anonymous" state, so one key
// draining it 429'd all the others.
func TestIsolation_StaticKeysHaveDistinctB4State(t *testing.T) {
	type server struct {
		e *gin.Engine
		q chan *domain.PendingRequest
	}
	newServer := func(t *testing.T, quota auth.QuotaConfig, share float64, pol *policy.Policy) server {
		prov, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
			{KeyHash: sha256Hex(isoKeyA), Metadata: auth.KeyMetadata{Name: "a1", Owner: "tenant-a"}, Quota: quota, MaxOccupancyShare: share},
			{KeyHash: sha256Hex(isoKeyB), Metadata: auth.KeyMetadata{Name: "a2", Owner: "tenant-a"}, Quota: quota, MaxOccupancyShare: share},
		})
		if err != nil {
			t.Fatal(err)
		}
		q := make(chan *domain.PendingRequest, 2)
		e := gin.New()
		e.POST("/v1/chat/completions", api.AuthMiddleware(prov), newB4Handlers(q, pol).Passthrough)
		return server{e, q}
	}
	// start sends a request and returns its status channel and, if it was
	// queued, the queued request (nil when it was refused before queueing).
	start := func(t *testing.T, s server, key string, body []byte) (<-chan int, *domain.PendingRequest) {
		done := make(chan int, 1)
		go func() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+key)
			s.e.ServeHTTP(w, r)
			done <- w.Code
		}()
		select {
		case p := <-s.q:
			return done, p
		case code := <-done:
			done <- code
			return done, nil
		case <-time.After(2 * time.Second):
			t.Fatal("request neither queued nor rejected")
			return nil, nil
		}
	}
	answer := func(p *domain.PendingRequest, completion int) {
		p.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`),
			Usage: domain.Usage{PromptTokens: 5, CompletionTokens: completion, TotalTokens: 5 + completion}}
	}
	send := func(t *testing.T, s server, key string, body []byte, completion int) int {
		done, p := start(t, s, key, body)
		if p != nil {
			answer(p, completion)
		}
		return <-done
	}

	// Each request is ~14 input tokens (40 chars / 4 + 4), against a burst of 20.
	t.Run("input tokens per minute", func(t *testing.T) {
		s := newServer(t, auth.QuotaConfig{InputTokensPerMinute: 20}, 0, serverless(0))
		if code := send(t, s, isoKeyA, textBody(40, 0), 1); code != http.StatusOK {
			t.Fatalf("key a1 first request: %d, want 200", code)
		}
		if code := send(t, s, isoKeyA, textBody(40, 0), 1); code != http.StatusTooManyRequests {
			t.Fatalf("key a1 second request: %d, want 429 (its bucket is drained)", code)
		}
		if code := send(t, s, isoKeyB, textBody(40, 0), 1); code != http.StatusOK {
			t.Errorf("key a2 after a1 drained its bucket: %d, want 200 (own bucket)", code)
		}
	})

	// a1's first response produces 50 output tokens against a limit of 10 a
	// minute, so its next request is held off; a2's bucket is untouched.
	t.Run("output tokens per minute", func(t *testing.T) {
		s := newServer(t, auth.QuotaConfig{OutputTokensPerMinute: 10}, 0, serverless(0))
		if code := send(t, s, isoKeyA, textBody(40, 0), 50); code != http.StatusOK {
			t.Fatalf("key a1 first request: %d, want 200", code)
		}
		if code := send(t, s, isoKeyA, textBody(40, 0), 1); code != http.StatusTooManyRequests {
			t.Fatalf("key a1 second request: %d, want 429 (its output bucket is spent)", code)
		}
		if code := send(t, s, isoKeyB, textBody(40, 0), 1); code != http.StatusOK {
			t.Errorf("key a2 after a1 spent its output bucket: %d, want 200 (own bucket)", code)
		}
	})

	// Share 0.4 of a 1000-token budget: a1 holds 400 in flight, so a second
	// a1 request does not fit its share; a2 has its own share.
	t.Run("occupancy share", func(t *testing.T) {
		s := newServer(t, auth.QuotaConfig{}, 0.4, serverless(1000))
		held, p := start(t, s, isoKeyA, b2Body(396))
		if p == nil {
			t.Fatalf("key a1 first request refused: %d", <-held)
		}
		if code := send(t, s, isoKeyA, b2Body(396), 1); code != http.StatusTooManyRequests {
			t.Fatalf("key a1 second request: %d, want 429 (its share is in flight)", code)
		}
		if code := send(t, s, isoKeyB, b2Body(396), 1); code != http.StatusOK {
			t.Errorf("key a2 while a1 holds its share: %d, want 200 (own share)", code)
		}
		answer(p, 1)
		if code := <-held; code != http.StatusOK {
			t.Errorf("key a1 first request: %d, want 200", code)
		}
	})
}

// TestIsolation_RateLimitHeadersArePerKey: X-RateLimit-Remaining-Requests
// describes the calling key's own bucket. Requests on one key of a tenant do
// not show up in another key's remaining count.
func TestIsolation_RateLimitHeadersArePerKey(t *testing.T) {
	quota := auth.QuotaConfig{RequestsPerMinute: 10}
	prov, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: sha256Hex(isoKeyA), Metadata: auth.KeyMetadata{Name: "a1", Owner: "tenant-a"}, Quota: quota},
		{KeyHash: sha256Hex(isoKeyB), Metadata: auth.KeyMetadata{Name: "a2", Owner: "tenant-a"}, Quota: quota},
	})
	if err != nil {
		t.Fatal(err)
	}
	e := gin.New()
	e.POST("/v1/chat/completions", api.AuthMiddleware(prov),
		api.QuotaMiddleware(auth.NewInMemoryLimiter(), metrics.NewRouterMetrics(), nil),
		func(c *gin.Context) { c.Status(http.StatusOK) })
	remaining := func(key string) string {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
		r.Header.Set("Authorization", "Bearer "+key)
		e.ServeHTTP(w, r)
		return w.Header().Get("X-RateLimit-Remaining-Requests")
	}
	for range 3 {
		remaining(isoKeyA)
	}
	if got := remaining(isoKeyA); got != "6" {
		t.Errorf("key a1 after 4 requests: remaining %q, want 6", got)
	}
	if got := remaining(isoKeyB); got != "9" {
		t.Errorf("key a2 first request: remaining %q, want 9 (a1's requests are not counted)", got)
	}
}
