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
	srv   *api.Server
	queue chan *domain.PendingRequest
}

func newIsoServer(t *testing.T) *isoServer {
	t.Helper()
	apiProv, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: sha256Hex(isoKeyA), KeyPreview: "sk-...a", Metadata: auth.KeyMetadata{Name: "a", Owner: "tenant-a"}},
		{KeyHash: sha256Hex(isoKeyB), KeyPreview: "sk-...b", Metadata: auth.KeyMetadata{Name: "b", Owner: "tenant-b"}},
	})
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
		nil, nil, q, 2*time.Second,
		nil, nil, nil, lim,
		nil, nil, nil, nil, nil, nil,
		healthy,
		nil, nil, nil, nil, nil, nil, nil,
	)
	srv := api.NewServer(h, ":0", apiProv, adminProv, lim, metrics.NewRouterMetrics(), healthy, 1<<20)
	return &isoServer{srv: srv, queue: q}
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

// isoInferenceCalls is one valid request per inference endpoint. The bodies
// carry fields a client might use to claim another tenant's identity.
var isoInferenceCalls = []struct{ path, body string }{
	{"/v1/chat/completions", `{"model":"m","user":"tenant-b","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`},
	{"/v1/messages", `{"model":"m","max_tokens":5,"metadata":{"user_id":"tenant-b"},"messages":[{"role":"user","content":"hi"}]}`},
	{"/v1/embeddings", `{"model":"e","user":"tenant-b","input":"x"}`},
	{"/v1/rerank", `{"model":"r","query":"q","documents":["d"]}`},
}

// TestIsolation_TenantFromKeyOnly: the tenant and key on the queued request
// come only from the presented credential. Headers and body fields that name
// another tenant change nothing, on every inference endpoint.
func TestIsolation_TenantFromKeyOnly(t *testing.T) {
	s := newIsoServer(t)
	spoof := bearerOf(isoKeyA)
	for _, h := range []string{"X-Tenant-ID", "X-Organization", "X-Owner", "X-Key-ID", "OpenAI-Organization"} {
		spoof[h] = "tenant-b"
	}
	for _, tc := range isoInferenceCalls {
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
	for _, tc := range isoInferenceCalls {
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
// one tenant each get their own input-token-per-minute bucket. Before static
// keys had an ID, every static key of every tenant shared one "anonymous"
// bucket, so one key draining it 429'd all the others.
func TestIsolation_StaticKeysHaveDistinctB4State(t *testing.T) {
	quota := auth.QuotaConfig{InputTokensPerMinute: 20}
	prov, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: sha256Hex(isoKeyA), Metadata: auth.KeyMetadata{Name: "a1", Owner: "tenant-a"}, Quota: quota},
		{KeyHash: sha256Hex(isoKeyB), Metadata: auth.KeyMetadata{Name: "a2", Owner: "tenant-a"}, Quota: quota},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := make(chan *domain.PendingRequest, 1)
	h := newB4Handlers(q, serverless(0)) // only the ITPM cap gates
	e := gin.New()
	e.POST("/v1/chat/completions", api.AuthMiddleware(prov), h.Passthrough)

	// Each request is ~14 input tokens (40 chars / 4 + 4), against a burst of 20.
	send := func(key string) int {
		done := make(chan int, 1)
		go func() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(textBody(40, 0)))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+key)
			e.ServeHTTP(w, r)
			done <- w.Code
		}()
		select {
		case p := <-q:
			p.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
		case code := <-done:
			return code // rejected before queueing
		case <-time.After(2 * time.Second):
			t.Fatal("request neither queued nor rejected")
		}
		return <-done
	}

	if code := send(isoKeyA); code != http.StatusOK {
		t.Fatalf("key a1 first request: %d, want 200", code)
	}
	if code := send(isoKeyA); code != http.StatusTooManyRequests {
		t.Fatalf("key a1 second request: %d, want 429 (its bucket is drained)", code)
	}
	if code := send(isoKeyB); code != http.StatusOK {
		t.Errorf("key a2 after a1 drained its bucket: %d, want 200 (own bucket)", code)
	}
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
	lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000}
	h := api.NewHandlers(aliasStorage{models: []string{"chat", "coder"}}, nil, q, 2*time.Second,
		exec, nil, nil, lim, nil, nil, nil, nil, nil, nil, healthy, nil, nil, nil, nil, nil, nil, nil)
	h.SetIsolation(iso)
	rpm, tpd := 100, 0
	prov, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: sha256Hex("open"), Metadata: auth.KeyMetadata{Name: "open", Owner: "t"}},
		{KeyHash: sha256Hex("listed"), Metadata: auth.KeyMetadata{Name: "listed", Owner: "t"}, Models: []string{"chat"}},
		{KeyHash: sha256Hex("permodel"), Metadata: auth.KeyMetadata{Name: "permodel", Owner: "t"},
			Quota: auth.QuotaConfig{PerModel: map[string]*auth.PerModelQuotaConfig{"chat": {RequestsPerMinutePerReplica: &rpm, TokensPerDay: &tpd}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &isoServer{srv: api.NewServer(h, ":0", prov, prov, lim, metrics.NewRouterMetrics(), healthy, 1<<20), queue: q}
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
