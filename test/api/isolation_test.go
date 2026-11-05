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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
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
	"hivenet_router/internal/semantic"

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
	charges *int // RPM charges, for servers built on a countingLimiter
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

// aclServer is a full api.Server whose storage lists models "chat" and
// "coder", with the "auto" semantic alias over both, three keys of one tenant
// ("open" has no allowlist, "listed" lists chat, "permodel" declares a
// per-model quota for chat) and the given isolation switches.
func aclServer(t *testing.T, iso api.Isolation) *isoServer {
	t.Helper()
	exec := policy.NewExecutor(nil, nil, policy.Default(), 3, 0)
	doc, err := policy.LoadModelDocBytes([]byte(aliasDocYAML))
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.SetNamedPolicy("auto", doc); err != nil {
		t.Fatal(err)
	}
	healthy := func(string) int { return 1 }
	q := make(chan *domain.PendingRequest, 8)
	lim := newCountingLimiter()
	h := api.NewHandlers(aliasStorage{models: []string{"chat", "coder"}}, nil, q, 2*time.Second,
		exec, nil, nil, lim, nil, nil, nil, nil, nil, nil, healthy, nil, nil, nil, nil, nil, nil, nil)
	h.SetIsolation(iso)
	rpm, tpd := 100, 0
	prov, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: sha256Hex("open"), Metadata: auth.KeyMetadata{Name: "open", Owner: "t"}},
		{KeyHash: sha256Hex("listed"), Metadata: auth.KeyMetadata{Name: "listed", Owner: "t"}, Models: []string{"chat"},
			Quota: auth.QuotaConfig{RequestsPerMinute: 100}},
		{KeyHash: sha256Hex("permodel"), Metadata: auth.KeyMetadata{Name: "permodel", Owner: "t"},
			Quota: auth.QuotaConfig{PerModel: map[string]*auth.PerModelQuotaConfig{"chat": {RequestsPerMinutePerReplica: &rpm, TokensPerDay: &tpd}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &isoServer{srv: api.NewServer(h, ":0", prov, prov, lim, metrics.NewRouterMetrics(), healthy, 1<<20), queue: q, charges: &lim.charges}
}

// call sends one request and, when it reaches the queue, answers it.
func (s *isoServer) call(t *testing.T, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	var b []byte
	if body != "" {
		b = []byte(body)
	}
	go func() { done <- s.do(method, path, b, bearerOf(key)) }()
	select {
	case w := <-done:
		return w
	case p := <-s.queue:
		p.Response <- &domain.ChatResponse{RawBytes: []byte(`{"served":true}`)}
		return <-done
	case <-time.After(3 * time.Second):
		t.Fatalf("%s %s: no response", method, path)
		return nil
	}
}

// TestIsolation_RequireModelAllowlist: with RequireModelAllowlist on, a key
// without an allowlist can neither call nor list any model: chat, messages,
// embeddings, rerank, the auto alias, /v1/models and /v1/models/:model all
// refuse it. Keys with an allowlist (models or per_model) are unaffected, and
// with the switch off an unrestricted key still reaches every model.
func TestIsolation_RequireModelAllowlist(t *testing.T) {
	chat := `{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}`
	calls := []struct{ name, path, body string }{
		{"chat", "/v1/chat/completions", fmt.Sprintf(chat, "chat")},
		{"messages", "/v1/messages", fmt.Sprintf(chat, "chat")},
		{"count_tokens", "/v1/messages/count_tokens", `{"model":"chat","messages":[{"role":"user","content":"hi"}]}`},
		{"embeddings", "/v1/embeddings", `{"model":"chat","input":"x"}`},
		{"rerank", "/v1/rerank", `{"model":"chat","query":"q","documents":["d"]}`},
		{"auto alias", "/v1/chat/completions", fmt.Sprintf(chat, "auto")},
	}
	for _, required := range []bool{false, true} {
		s := aclServer(t, api.Isolation{RequireModelAllowlist: required})
		for _, key := range []string{"open", "listed", "permodel"} {
			denied := required && key == "open"
			for _, c := range calls {
				w := s.call(t, http.MethodPost, c.path, c.body, key)
				if denied != (w.Code == http.StatusForbidden) {
					t.Errorf("required=%v key=%s %s: status %d, want denied=%v; body %s", required, key, c.name, w.Code, denied, w.Body)
				}
			}
			list := s.call(t, http.MethodGet, "/v1/models", "", key)
			if listsChat := strings.Contains(list.Body.String(), `"chat"`); listsChat == denied {
				t.Errorf("required=%v key=%s /v1/models lists chat=%v; body %s", required, key, listsChat, list.Body)
			}
			if w := s.call(t, http.MethodGet, "/v1/models/chat", "", key); denied != (w.Code == http.StatusNotFound) {
				t.Errorf("required=%v key=%s /v1/models/chat: status %d, want hidden=%v", required, key, w.Code, denied)
			}
		}
	}

	// A model miss that would be redirected to a sibling policloud is not
	// redirected for a key without an allowlist: no token is minted for it.
	t.Run("redirect", func(t *testing.T) {
		a := newSiteWith(t, "a", nil, testKeys(t), api.Isolation{RequireModelAllowlist: true})
		push(t, fleetDoc(1), a)
		w := send(a, chatPath, chatBody, scoped("all"))
		if w.Code != http.StatusForbidden || w.Header().Get("Location") != "" || len(a.redirs) != 0 {
			t.Fatalf("status %d Location %q redirects %v, want 403 and no redirect", w.Code, w.Header().Get("Location"), a.redirs)
		}
	})

	// A request redirected here from another site carries the key's ID in
	// its token. At this site the key has no allowlist and one is required:
	// it is refused here too, before any quota, and nothing is queued.
	t.Run("redirected request at the target", func(t *testing.T) {
		b := newSiteWith(t, "b", nil, testKeys(t), api.Isolation{RequireModelAllowlist: true})
		push(t, fleetDoc(1), b)
		tok := testSigner(t).Sign(redirect.Claims{KeyID: "key-1", RequestID: "11111111-2222-3333-4444-555555555555", Model: "m",
			Scope: redirect.ScopeAll, From: "a", Site: "b", Path: chatPath, Hop: 1, BodySHA: redirect.BodySHA(chatBody)}, time.Now())
		w := send(b, chatPath+"?rt="+tok, chatBody, nil)
		if w.Code != http.StatusForbidden || len(b.queue) != 0 || *b.charges != 0 {
			t.Fatalf("status %d queued %d charges %d, want 403, nothing queued, nothing charged; body %s", w.Code, len(b.queue), *b.charges, w.Body)
		}
	})

	// A model scaled to zero here would hold the request and wake the model:
	// a key that may not use it is refused at once, nothing is held or woken.
	t.Run("model scaled to zero", func(t *testing.T) {
		s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
		s.h.SetIsolation(api.Isolation{RequireModelAllowlist: true})
		w := result(t, sendAsync(s.testSite, chatPath, chatBody, bearer()))
		if w.Code != http.StatusForbidden || s.holder.Demand()["m"] != 0 || s.charged.Load() != 0 {
			t.Fatalf("status %d demand %d charged %d, want 403 and nothing held, woken or charged", w.Code, s.holder.Demand()["m"], s.charged.Load())
		}
	})
}

// TestIsolation_ForbiddenModelCostsNothing: on every inference route, a model
// outside the key's allowlist is refused with 403 (not the per-model quota's
// 429), before the key is charged any request quota, and nothing is queued.
// So a client retrying a forbidden model in a loop never burns its own rate
// limit or writes to the shared store.
func TestIsolation_ForbiddenModelCostsNothing(t *testing.T) {
	s := aclServer(t, api.Isolation{})
	model := regexp.MustCompile(`"model":"[^"]*"`)
	for _, key := range []string{"listed", "permodel"} {
		for _, c := range isoInferenceCalls(t, s) {
			body := model.ReplaceAllString(c.body, `"model":"coder"`) // allowed to neither key
			before := *s.charges
			w := s.call(t, http.MethodPost, c.path, body, key)
			if w.Code != http.StatusForbidden || *s.charges != before || len(s.queue) != 0 {
				t.Errorf("key %s %s for a forbidden model: status %d, %d RPM charges, %d queued; want 403, none, none; body %s",
					key, c.path, w.Code, *s.charges-before, len(s.queue), w.Body)
			}
		}
	}
	// The allowed model still costs its quota, so the check above is real.
	before := *s.charges
	if w := s.call(t, http.MethodPost, "/v1/chat/completions", `{"model":"chat","messages":[{"role":"user","content":"hi"}]}`, "listed"); w.Code != http.StatusOK || *s.charges != before+1 {
		t.Errorf("allowed model: status %d, %d RPM charges; want 200 and one charge", w.Code, *s.charges-before)
	} // A forbidden model hidden behind a differently cased "Model" the
	// router would read as the allowed one: refused, nothing charged.
	before = *s.charges
	if w := s.call(t, http.MethodPost, "/v1/chat/completions", `{"model":"coder","Model":"chat","messages":[{"role":"user","content":"hi"}]}`, "listed"); w.Code != http.StatusBadRequest || *s.charges != before || len(s.queue) != 0 {
		t.Errorf("model behind Model: status %d, %d RPM charges, %d queued; want 400, none, none; body %s", w.Code, *s.charges-before, len(s.queue), w.Body)
	}
}

// TestIsolation_AmbiguousFieldsRefused: on every inference route, a body the
// router (encoding/json: case-insensitive names, last repeat wins) and the
// backend (exact names) would read differently is refused with 400 before
// anything reads it: nothing is charged or queued. That covers the model the
// allowlist checks and every other field the router acts on, such as the
// max_tokens its capacity reservation is sized from, at the top level and in
// the nested objects it decodes (messages, content parts, system blocks). A body that is not one
// JSON object is refused the same way.
func TestIsolation_AmbiguousFieldsRefused(t *testing.T) {
	s := aclServer(t, api.Isolation{})
	for _, inject := range []string{
		`"Model":"coder",`,
		`"MODEL":"coder",`,
		`"model":"coder",`,      // a second model (the body has one)
		`"mod\u0065l":"coder",`, // the same name, escaped
		`"Max_Tokens":1,`,
		`"max_to\u212aens":1,`, // Kelvin sign: folds to k
		`"ſtream":true,`,       // long s: folds to s
		`"x":1,"x":2,`,         // any repeated name
	} {
		for _, c := range isoInferenceCalls(t, s) {
			body := strings.Replace(c.body, "{", "{"+inject, 1)
			before := *s.charges
			w := s.call(t, http.MethodPost, c.path, body, "open")
			if w.Code != http.StatusBadRequest || *s.charges != before || len(s.queue) != 0 {
				t.Errorf("%s with %s: status %d, %d RPM charges, %d queued; want 400, none, none; body %s",
					c.path, inject, w.Code, *s.charges-before, len(s.queue), w.Body)
			}
		}
	}
	for _, c := range isoInferenceCalls(t, s) {
		if w := s.call(t, http.MethodPost, c.path, c.body+` {"model":"coder"}`, "open"); w.Code != http.StatusBadRequest {
			t.Errorf("%s with a second object after the body: status %d, want 400", c.path, w.Code)
		}
	}
	// Nested objects the router decodes are checked the same way: a message,
	// its content parts (and a tool_result's), an image_url, the Anthropic
	// system blocks. "Content" next to "content" would let a 100 KB prompt
	// be estimated, and charged to every input budget, as "hi".
	big := strings.Repeat("x", 100_000)
	for name, body := range map[string]string{
		"Content next to content": `{"model":"chat","messages":[{"role":"user","content":"` + big + `","Content":"hi"}]}`,
		"role twice":              `{"model":"chat","messages":[{"role":"user","role":"assistant","content":"hi"}]}`,
		"part Type":               `{"model":"chat","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"},"Type":"text"}]}]}`,
		"image_url URL":           `{"model":"chat","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"a","URL":"b"}}]}]}`,
		"system block Text":       `{"model":"chat","system":[{"type":"text","text":"a","Text":"b"}],"messages":[{"role":"user","content":"hi"}]}`,
		"tool_result part TEXT":   `{"model":"chat","messages":[{"role":"user","content":[{"type":"tool_result","content":[{"type":"text","text":"a","TEXT":"b"}]}]}]}`,
	} {
		for _, path := range []string{"/v1/chat/completions", "/v1/messages"} {
			before := *s.charges
			w := s.call(t, http.MethodPost, path, body, "open")
			if w.Code != http.StatusBadRequest || *s.charges != before || len(s.queue) != 0 {
				t.Errorf("%s on %s: status %d, %d RPM charges, %d queued; want 400, none, none; body %.200s",
					name, path, w.Code, *s.charges-before, len(s.queue), w.Body)
			}
		}
	}
	// A field the router does not read may differ in case, and free-form
	// subtrees (tool schemas, tool_calls, metadata) are the backend's
	// business: they still pass.
	for name, body := range map[string]string{
		"unread top-level field": `{"model":"chat","Seed":1,"messages":[{"role":"user","content":"hi"}]}`,
		"tool schema":            `{"model":"chat","tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"A":{},"a":{}}}}}],"messages":[{"role":"user","content":"hi"}]}`,
		"metadata":               `{"model":"chat","metadata":{"user_id":"u","User_ID":"v"},"messages":[{"role":"user","content":"hi"}]}`,
		"tool_calls entry":       `{"model":"chat","messages":[{"role":"assistant","content":"","tool_calls":[{"id":"1","ID":"2","type":"function","function":{"name":"f","arguments":"{}"}}]},{"role":"user","content":"hi"}]}`,
	} {
		if w := s.call(t, http.MethodPost, "/v1/chat/completions", body, "open"); w.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200; body %s", name, w.Code, w.Body)
		}
	}
}

// TestEmbeddings_HonoursEffectiveAllowlist: embeddings and rerank apply the
// same allowlist as chat, including a per_model quota block acting as the
// allowlist. Before, they read only allowed_models, so a per_model key's
// allowlist was enforced only indirectly by the quota layer.
func TestEmbeddings_HonoursEffectiveAllowlist(t *testing.T) {
	perModel := auth.QuotaLimits{PerModel: map[string]auth.PerModelQuotaLimits{"e": {}}}
	for _, tc := range []struct {
		path, body string
		run        func(*api.Handlers, *gin.Context)
	}{
		{"/v1/embeddings", `{"model":"other","input":"x"}`, (*api.Handlers).Embeddings},
		{"/v1/rerank", `{"model":"other","query":"q","documents":["d"]}`, (*api.Handlers).Rerank},
	} {
		q := make(chan *domain.PendingRequest, 1)
		h := newTestHandlers(q, &fakeLimiter{})
		c, w := newCtx(tc.path, []byte(tc.body))
		c.Set("tenant_id", "t")
		c.Set("quota_limits", perModel)
		tc.run(h, c)
		if w.Code != http.StatusForbidden || len(q) != 0 {
			t.Errorf("%s for a model outside the per_model allowlist: status %d queued %d, want 403 and nothing queued", tc.path, w.Code, len(q))
		}
	}
}

// TestAllowlist_IntersectsAllowedModelsAndPerModel: a key with both an
// allowlist (models) and quota.per_model may use only the models in both, on
// every inference endpoint and in discovery. per_model used to win, so a
// model it listed but the allowlist did not was served (and listed); neither
// list widens the other now.
func TestAllowlist_IntersectsAllowedModelsAndPerModel(t *testing.T) {
	const key = "sk-iso-both"
	zero := 0
	s := newIsoServerWith(t, []auth.APIKeyEntry{{
		KeyHash: sha256Hex(key), Metadata: auth.KeyMetadata{Name: "both", Owner: "tenant-a"},
		Models: []string{"chat", "legacy"},
		Quota: auth.QuotaConfig{PerModel: map[string]*auth.PerModelQuotaConfig{
			"chat":  {RequestsPerMinutePerReplica: &zero, TokensPerDay: &zero},
			"coder": {RequestsPerMinutePerReplica: &zero, TokensPerDay: &zero},
		}},
	}}, []string{"chat", "coder", "legacy"})
	model := regexp.MustCompile(`"model":"[^"]*"`)
	for _, c := range isoInferenceCalls(t, s) {
		for m, want := range map[string]int{"chat": http.StatusOK, "coder": http.StatusForbidden, "legacy": http.StatusForbidden} {
			body := model.ReplaceAllString(c.body, `"model":"`+m+`"`)
			if w := s.call(t, http.MethodPost, c.path, body, key); w.Code != want {
				t.Errorf("%s model %s: status %d, want %d; body %s", c.path, m, w.Code, want, w.Body)
			}
		}
	}
	w := s.do(http.MethodGet, "/v1/models", nil, bearerOf(key))
	for m, listed := range map[string]bool{"chat": true, "coder": false, "legacy": false} {
		if got := strings.Contains(w.Body.String(), `"`+m+`"`); got != listed {
			t.Errorf("/v1/models lists %s = %v, want %v; body %s", m, got, listed, w.Body)
		}
	}
}

// TestIsolation_TrailingDataRejected: on every inference route, a body that is
// not exactly one JSON object (an object followed by more data, another JSON
// value, invalid JSON) is refused with 400 before routing: nothing is queued.
// The handlers' decoding stops after the first value, so before this check a
// body like {"model":"m",...} x was accepted and only failed when the router
// edited it for the agent, after an agent had been chosen.
func TestIsolation_TrailingDataRejected(t *testing.T) {
	s := newIsoServer(t)
	for _, c := range isoInferenceCalls(t, s) {
		for name, body := range map[string]string{
			"trailing data": c.body + " x",
			"second object": c.body + "{}",
			"array":         "[" + c.body + "]",
			"invalid JSON":  strings.TrimSuffix(c.body, "}"),
			"empty":         "",
		} {
			w := s.call(t, http.MethodPost, c.path, body, isoKeyA)
			if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "request_invalid") || len(s.queue) != 0 {
				t.Errorf("%s %s: status %d, %d queued, body %s; want 400 request_invalid and nothing queued", c.path, name, w.Code, len(s.queue), w.Body)
			}
		}
		// The same body, well formed, is served.
		if w := s.call(t, http.MethodPost, c.path, c.body, isoKeyA); w.Code != http.StatusOK {
			t.Errorf("%s well formed: status %d, want 200", c.path, w.Code)
		}
	}
	// On /v1/messages, an extra_params llama.cpp could not carry the prompt
	// cache switch in is refused for every engine; null passes like absent.
	for body, want := range map[string]int{
		`{"model":"m","max_tokens":5,"messages":[],"extra_params":"x"}`:  http.StatusBadRequest,
		`{"model":"m","max_tokens":5,"messages":[],"extra_params":[1]}`:  http.StatusBadRequest,
		`{"model":"m","max_tokens":5,"messages":[],"extra_params":null}`: http.StatusOK,
		`{"model":"m","max_tokens":5,"messages":[],"extra_params":{}}`:   http.StatusOK,
	} {
		if w := s.call(t, http.MethodPost, "/v1/messages", body, isoKeyA); w.Code != want {
			t.Errorf("/v1/messages %s: status %d, want %d; body %s", body, w.Code, want, w.Body)
		}
	}
}

// TestIsolation_DecisionLogAttributable: each semantic-alias decision record
// carries the caller's tenant and key, so one customer's routing decisions
// can be selected from the shared decision log.
func TestIsolation_DecisionLogAttributable(t *testing.T) {
	path := t.TempDir() + "/decisions.jsonl"
	dl, err := semantic.OpenDecisionLog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	h := newAliasHandlers(t, nil)
	h.SetDecisionLog(dl)
	auth := func(c *gin.Context) {
		c.Set("tenant_id", "tenant-a")
		c.Set("key_id", "key-a1")
		c.Next()
	}
	var out seen
	if w := post(aliasRouter(h, auth, &out), "/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	if err := dl.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &rec); err != nil {
		t.Fatalf("decision log %q: %v", raw, err)
	}
	if rec["tenant_id"] != "tenant-a" || rec["key_id"] != "key-a1" {
		t.Errorf("decision record tenant_id %v key_id %v, want tenant-a / key-a1", rec["tenant_id"], rec["key_id"])
	}
}
