// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

// Cross-site redirect (contract 3 v2) through the real middleware chain, in
// production order: AuthMiddleware over redirect.Wrap, then body limit,
// RedirectTarget, AliasMiddleware, RedirectDecision and QuotaMiddleware, then
// the handlers. Each "policloud" is a gin engine with its own healthy-agent
// counts and request queue; a stub processor answers what a policloud queues.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/redirect"

	"github.com/gin-gonic/gin"
)

const (
	redirectKey = "sk-redirect-test"  // key-1, tenant-1
	otherKey    = "sk-redirect-other" // key-2, tenant-2
	chatPath    = "/v1/chat/completions"
)

var redirectSecret = redirect.Secret{ID: "k1", Key: []byte("0123456789abcdef0123456789abcdef")}

// testDomains: the fleet's hosts, and 127.0.0.1 where the round-trip test's
// httptest servers listen.
var testDomains = []string{".example.com", ".0.0.1"}

type testSite struct {
	engine  *gin.Engine
	rd      *redirect.Redirector
	queue   chan *domain.PendingRequest
	redirs  []string // "model→policloud@hop" per 307 written
	rejects []string
	charges *int // per-minute quota charges (QuotaMiddleware → AllowRequest)
}

// countingLimiter counts per-minute quota charges.
type countingLimiter struct {
	fakeLimiter
	charges int
}

func (l *countingLimiter) AllowRequest(string, string, int) (bool, int, error) {
	l.charges++
	return true, -1, nil
}

func newCountingLimiter() *countingLimiter {
	return &countingLimiter{fakeLimiter: fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000}}
}

func testSigner(t *testing.T) *redirect.Signer {
	t.Helper()
	s, err := redirect.NewSigner([]redirect.Secret{redirectSecret}, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// newSite builds policloud id. healthy gives its healthy-agent count per model
// (absent = 0); keys is the dynamic key registry shared by every policloud.
func newSite(t *testing.T, id string, healthy map[string]int, keys *auth.DynamicKeyProvider) *testSite {
	t.Helper()
	s := &testSite{queue: make(chan *domain.PendingRequest, 4)}
	s.rd = &redirect.Redirector{
		Self: id, Map: redirect.NewMap(testDomains), Signer: testSigner(t), Replay: redirect.NewReplayGuard(),
		OnRedirect: func(model, site string, hop int) {
			s.redirs = append(s.redirs, fmt.Sprintf("%s→%s@%d", model, site, hop))
		},
		OnReject: func(reason string) { s.rejects = append(s.rejects, reason) },
	}
	lim := newCountingLimiter()
	s.charges = &lim.charges
	healthyCount := func(model string) int { return healthy[model] }
	h := api.NewHandlers(
		nil, nil, s.queue, 2*time.Second,
		nil, nil, nil, lim,
		nil, nil, nil, nil, keys, nil,
		healthyCount,
		nil, nil, nil, nil, nil, nil, nil,
	)
	h.SetRedirect(s.rd)
	s.engine = inferenceEngine(h, redirect.Wrap(s.rd, keys, keys), lim, healthyCount)
	return s
}

// inferenceEngine wires the inference routes as server.go does.
func inferenceEngine(h *api.Handlers, provider auth.Provider, lim *countingLimiter, healthy func(string) int) *gin.Engine {
	e := gin.New()
	e.Use(api.RequestIDMiddleware())
	inference := e.Group("/v1", api.AuthMiddleware(provider)).
		Group("", api.BodyLimitMiddleware(1<<20), h.RedirectTarget(), h.AliasMiddleware(),
			h.RedirectDecision(), api.QuotaMiddleware(lim, metrics.NewRouterMetrics(), healthy))
	inference.POST("/chat/completions", h.Passthrough)
	inference.POST("/messages", h.Passthrough)
	inference.POST("/embeddings", h.Embeddings)
	inference.POST("/rerank", h.Rerank)
	return e
}

func testKeys(t *testing.T, allowed ...string) *auth.DynamicKeyProvider {
	t.Helper()
	h := sha256.Sum256([]byte(redirectKey))
	keys := auth.NewDynamicKeyProvider()
	if err := keys.Upsert("v1", auth.DynamicKeyEntry{
		ID: "key-1", KeyHash: hex.EncodeToString(h[:]), Owner: "tenant-1", Name: "test", Enabled: true,
		AllowedModels: allowed, Quota: auth.QuotaLimits{RequestsPerMinute: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	h2 := sha256.Sum256([]byte(otherKey))
	if err := keys.Upsert("v2", auth.DynamicKeyEntry{
		ID: "key-2", KeyHash: hex.EncodeToString(h2[:]), Owner: "tenant-2", Name: "other", Enabled: true,
		Quota: auth.QuotaLimits{RequestsPerMinute: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

var yes = func() *bool { b := true; return &b }()

// fleetDoc: a (FR, the entry) runs nothing; chat model m on b (FR) and c
// (DE); embedding model e on b; rerank model r on c. extra placements are
// appended.
func fleetDoc(version uint64, extra ...redirect.Placement) redirect.MapDoc {
	pc := func(id, country string) redirect.Policloud {
		return redirect.Policloud{ID: id, URL: "https://" + id + ".example.com", Country: country, EU: yes}
	}
	return redirect.MapDoc{
		Schema: 2, Version: &version, Region: "eu",
		Policlouds: []redirect.Policloud{pc("a", "FR"), pc("b", "FR"), pc("c", "DE")},
		Placements: append([]redirect.Placement{
			placement("m", redirect.KindChat, "b"), placement("m", redirect.KindChat, "c"),
			placement("e", redirect.KindEmbedding, "b"), placement("r", redirect.KindRerank, "c"),
		}, extra...),
	}
}

func placement(model, kind, policloud string) redirect.Placement {
	return redirect.Placement{Model: model, Kind: kind, Policloud: policloud, ScaleFromZero: yes}
}

func push(t *testing.T, doc redirect.MapDoc, sites ...*testSite) {
	t.Helper()
	for _, s := range sites {
		if err := s.rd.Map.Replace(doc); err != nil {
			t.Fatal(err)
		}
	}
}

func send(s *testSite, target string, body []byte, header map[string]string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		r.Header.Set(k, v)
	}
	s.engine.ServeHTTP(w, r)
	return w
}

// sendAsync sends from a goroutine; serveOne then answers on the test goroutine.
func sendAsync(s *testSite, target string, body []byte, header map[string]string) chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- send(s, target, body, header) }()
	return done
}

// serveOne answers the next queued request, as the router's processor would.
func serveOne(t *testing.T, s *testSite) *domain.PendingRequest {
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

func bearer() map[string]string { return map[string]string{"Authorization": "Bearer " + redirectKey} }

// scoped is bearer() plus an X-Hivenet-Scope header.
func scoped(scope string) map[string]string {
	h := bearer()
	h["X-Hivenet-Scope"] = scope
	return h
}

var chatBody = []byte(`{"model":"m","messages":[{"role":"user","content":"hello"}],"max_tokens":5}`)

// locationToken parses the Location of a 307 and verifies its token.
func locationToken(t *testing.T, w *httptest.ResponseRecorder) (*url.URL, redirect.Claims) {
	t.Helper()
	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status %d, want 307; body %s", w.Code, w.Body)
	}
	loc, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := testSigner(t).Verify(loc.Query().Get(redirect.QueryParam), time.Now())
	if err != nil {
		t.Fatalf("token in Location: %v", err)
	}
	return loc, c
}

// TestRedirect_MissWrites307: the entry policloud has no agent for the model,
// so it answers 307 to a sibling with a v2 token, and spends nothing: no
// queueing and no quota charge. Without X-Hivenet-Scope the request stays in
// the entry's country: b (FR), never c (DE).
func TestRedirect_MissWrites307(t *testing.T) {
	a := newSite(t, "a", nil, testKeys(t))
	push(t, fleetDoc(1), a)

	w := send(a, chatPath+"?api-version=2024", chatBody, bearer())

	loc, c := locationToken(t, w)
	if loc.Scheme != "https" || loc.Host != "b.example.com" || loc.Path != chatPath {
		t.Fatalf("Location %s: want https://b.example.com%s", loc, chatPath)
	}
	if loc.Query().Get("api-version") != "2024" {
		t.Errorf("request query not kept: %s", loc.RawQuery)
	}
	want := redirect.Claims{
		KeyID: "key-1", RequestID: w.Header().Get("X-Request-ID"), Model: "m", Scope: "country:FR",
		From: "a", Site: "b", Path: chatPath, Hop: 1,
		BodySHA: redirect.BodySHA(chatBody), IssuedAt: c.IssuedAt, Exp: c.IssuedAt + 60, JTI: c.JTI,
	}
	if c != want || c.RequestID == "" || !redirect.ValidJTI(c.JTI) {
		t.Errorf("claims %+v, want %+v", c, want)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control %q, want no-store", got)
	}
	var resp domain.RouterErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Error.Code != domain.ErrCodeModelRedirected {
		t.Errorf("body %s: want error code %s for clients that do not follow", w.Body, domain.ErrCodeModelRedirected)
	}
	if len(a.queue) != 0 {
		t.Error("a redirected request must not be queued")
	}
	if *a.charges != 0 {
		t.Errorf("the redirecting policloud charged %d quota unit(s); it must charge none", *a.charges)
	}
	if len(a.redirs) != 1 || a.redirs[0] != "m→b@1" {
		t.Errorf("redirect metric %v", a.redirs)
	}
}

// TestRedirect_EmbeddingsAndRerank: every inference endpoint redirects, by
// the placement kind that matches it — and only by that kind. (Scope EU:
// rerank model r runs only in c, in DE.)
func TestRedirect_EmbeddingsAndRerank(t *testing.T) {
	a := newSite(t, "a", nil, testKeys(t))
	push(t, fleetDoc(1), a)
	for _, tc := range []struct {
		path, body, wantHost string // wantHost "" = b or c
	}{
		{"/v1/embeddings", `{"model":"e","input":"x"}`, "b.example.com"},
		{"/v1/rerank", `{"model":"r","query":"q","documents":["d"]}`, "c.example.com"},
		{"/v1/messages", `{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}`, ""},
	} {
		loc, c := locationToken(t, send(a, tc.path, []byte(tc.body), scoped("EU")))
		if (tc.wantHost != "" && loc.Host != tc.wantHost) || loc.Path != tc.path || c.Path != tc.path {
			t.Errorf("%s: Location %s, path claim %q", tc.path, loc, c.Path)
		}
	}

	// m is placed only as a chat model: an embeddings request for it is not
	// redirected but handled here as before.
	done := sendAsync(a, "/v1/embeddings", []byte(`{"model":"m","input":"x"}`), scoped("EU"))
	serveOne(t, a)
	if w := <-done; w.Header().Get("Location") != "" {
		t.Error("an embeddings request for a chat-only model must not be redirected")
	}
}

// TestRedirect_ServedHereOrUnchanged: no 307 when a local agent serves the
// model, no policloud serves it, redirect is disabled, or the key may not use
// the model.
func TestRedirect_ServedHereOrUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name    string
		healthy map[string]int
		model   string
		enable  bool
	}{
		{"local agent", map[string]int{"m": 1}, "m", true},
		{"no policloud serves the model (existing 404 path)", nil, "unknown", true},
		{"redirect disabled", nil, "m", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSite(t, "a", tc.healthy, testKeys(t))
			push(t, fleetDoc(1), s)
			if !tc.enable {
				disable(t, s)
			}
			body := []byte(`{"model":"` + tc.model + `","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`)
			done := sendAsync(s, chatPath, body, bearer())
			serveOne(t, s)
			if w := <-done; w.Code != http.StatusOK || w.Header().Get("Location") != "" {
				t.Fatalf("status %d Location %q, want 200 and no redirect", w.Code, w.Header().Get("Location"))
			}
		})
	}

	t.Run("key may not use the model: 403, not a redirect", func(t *testing.T) {
		s := newSite(t, "a", nil, testKeys(t, "other"))
		push(t, fleetDoc(1), s)
		if w := send(s, chatPath, chatBody, bearer()); w.Code != http.StatusForbidden || w.Header().Get("Location") != "" {
			t.Fatalf("status %d Location %q, want 403 and no redirect", w.Code, w.Header().Get("Location"))
		}
	})
}

// disable rewires s with redirect disabled: SetRedirect never called and the
// API provider not wrapped, as main does without a site ID.
func disable(t *testing.T, s *testSite) {
	t.Helper()
	keys := testKeys(t)
	lim := newCountingLimiter()
	none := func(string) int { return 0 }
	h := api.NewHandlers(nil, nil, s.queue, 2*time.Second, nil, nil, nil, lim,
		nil, nil, nil, nil, keys, nil, none, nil, nil, nil, nil, nil, nil, nil)
	s.engine = inferenceEngine(h, redirect.Wrap(nil, keys, keys), lim, none)
}

// TestRedirect_Scope: X-Hivenet-Scope bounds where a first hop may go; no
// header = this policloud's country; an invalid value is a 400, never
// widened; a redirected request keeps its token's scope (see AtTheTarget).
func TestRedirect_Scope(t *testing.T) {
	a := newSite(t, "a", nil, testKeys(t))
	push(t, fleetDoc(1), a)
	seen := func(hdr map[string]string) map[string]redirect.Scope {
		out := map[string]redirect.Scope{}
		for i := 0; i < 50; i++ {
			hdr["X-Session-ID"] = fmt.Sprint(i)
			loc, c := locationToken(t, send(a, chatPath, chatBody, hdr))
			out[loc.Host] = c.Scope
		}
		return out
	}
	for _, tc := range []struct {
		scope string
		want  map[string]redirect.Scope
	}{
		{"", map[string]redirect.Scope{"b.example.com": "country:FR"}},
		{"country:DE", map[string]redirect.Scope{"c.example.com": "country:DE"}},
		{"EU", map[string]redirect.Scope{"b.example.com": "EU", "c.example.com": "EU"}},
		{"all", map[string]redirect.Scope{"b.example.com": "all", "c.example.com": "all"}},
	} {
		hdr := bearer()
		if tc.scope != "" {
			hdr = scoped(tc.scope)
		}
		if got := seen(hdr); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("scope %q: targets and token scopes %v, want %v", tc.scope, got, tc.want)
		}
	}

	t.Run("no policloud in the scope: served here", func(t *testing.T) {
		done := sendAsync(a, chatPath, chatBody, scoped("country:US"))
		serveOne(t, a)
		if w := <-done; w.Header().Get("Location") != "" {
			t.Fatal("no redirect expected")
		}
	})
	for _, bad := range []string{"mars", "country:fr", "country:FRA", "eu"} {
		t.Run("invalid "+bad+": 400", func(t *testing.T) {
			charges, redirs := *a.charges, len(a.redirs)
			w := send(a, chatPath, chatBody, scoped(bad))
			if w.Code != http.StatusBadRequest || w.Header().Get("Location") != "" || len(a.redirs) != redirs || len(a.queue) != 0 || *a.charges != charges {
				t.Fatalf("status %d Location %q redirects %d queued %d, want 400 and nothing redirected, queued or charged",
					w.Code, w.Header().Get("Location"), len(a.redirs)-redirs, len(a.queue))
			}
		})
	}
	t.Run("this policloud not in the map: no default scope, served here", func(t *testing.T) {
		x := newSite(t, "x", nil, testKeys(t))
		push(t, fleetDoc(1), x)
		done := sendAsync(x, chatPath, chatBody, bearer())
		serveOne(t, x)
		if w := <-done; w.Header().Get("Location") != "" {
			t.Fatal("no redirect expected without a home country")
		}
	})
}

// TestRedirect_SessionAffinity: the same key and X-Session-ID always get the
// same policloud (scope all, so both b and c are candidates).
func TestRedirect_SessionAffinity(t *testing.T) {
	a := newSite(t, "a", nil, testKeys(t))
	push(t, fleetDoc(1), a)
	hdr := scoped("all")
	hdr["X-Session-ID"] = "conversation-42"
	first, _ := locationToken(t, send(a, chatPath, chatBody, hdr))
	for i := 0; i < 20; i++ {
		if got, _ := locationToken(t, send(a, chatPath, chatBody, hdr)); got.Host != first.Host {
			t.Fatalf("same key+session went to %s then %s", first.Host, got.Host)
		}
	}
}

// TestRedirect_AtTheTarget covers the receiving side.
func TestRedirect_AtTheTarget(t *testing.T) {
	const rid = "11111111-2222-3333-4444-555555555555"
	claims := func(mod func(*redirect.Claims)) redirect.Claims {
		c := redirect.Claims{KeyID: "key-1", RequestID: rid, Model: "m", Scope: redirect.ScopeAll,
			From: "a", Site: "b", Path: chatPath, Hop: 1, BodySHA: redirect.BodySHA(chatBody)}
		if mod != nil {
			mod(&c)
		}
		return c
	}
	sign := func(t *testing.T, c redirect.Claims) string { return testSigner(t).Sign(c, time.Now()) }

	t.Run("token only: served on the key, request ID adopted, quota charged here", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		done := sendAsync(b, chatPath+"?rt="+sign(t, claims(nil)), chatBody, nil)
		p := serveOne(t, b)
		w := <-done
		if w.Code != http.StatusOK || p.KeyID != "key-1" || p.TenantID != "tenant-1" {
			t.Fatalf("status %d key %q tenant %q, want 200 on key-1 / tenant-1", w.Code, p.KeyID, p.TenantID)
		}
		if got := w.Header().Get("X-Request-ID"); got != rid {
			t.Errorf("X-Request-ID %q, want the token's %q", got, rid)
		}
		if *b.charges != 1 {
			t.Errorf("quota charged %d times at the target, want 1", *b.charges)
		}
	})

	t.Run("the token's model is used: auto is not resolved again", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		auto := []byte(`{"model":"auto","messages":[{"role":"user","content":"hello"}],"max_tokens":5}`)
		tok := sign(t, claims(func(c *redirect.Claims) { c.BodySHA = redirect.BodySHA(auto) }))
		done := sendAsync(b, chatPath+"?rt="+tok, auto, nil)
		p := serveOne(t, b)
		if w := <-done; w.Code != http.StatusOK || p.Request.Model != "m" {
			t.Fatalf("status %d routed model %q, want 200 on the token's model m", w.Code, p.Request.Model)
		}
	})

	t.Run("different body: refused before quota", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		w := send(b, chatPath+"?rt="+sign(t, claims(nil)), []byte(`{"model":"m","messages":[{"role":"user","content":"another prompt"}]}`), nil)
		if w.Code != http.StatusUnauthorized || len(b.queue) != 0 || *b.charges != 0 {
			t.Fatalf("status %d queued %d charged %d, want 401, nothing queued, no quota", w.Code, len(b.queue), *b.charges)
		}
		if strings.Join(b.rejects, ",") != "body" {
			t.Errorf("reject reasons %v, want [body]", b.rejects)
		}
	})

	t.Run("different body + valid key: served fresh on the key", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		other := []byte(`{"model":"m","messages":[{"role":"user","content":"another prompt"}],"max_tokens":5}`)
		done := sendAsync(b, chatPath+"?rt="+sign(t, claims(nil)), other, bearer())
		p := serveOne(t, b)
		w := <-done
		if w.Code != http.StatusOK || p.KeyID != "key-1" || *b.charges != 1 {
			t.Fatalf("status %d key %q charged %d, want 200 on key-1, charged once", w.Code, p.KeyID, *b.charges)
		}
		if w.Header().Get("X-Request-ID") == rid {
			t.Error("a fresh request must not adopt the refused token's request ID")
		}
		if strings.Join(b.rejects, ",") != "body" {
			t.Errorf("reject reasons %v, want [body]", b.rejects)
		}

		// Without a local agent the fresh request is redirected as a first hop
		// from b, not as the refused token's second hop from a.
		b = newSite(t, "b", nil, testKeys(t))
		push(t, fleetDoc(1), b)
		_, next := locationToken(t, send(b, chatPath+"?rt="+sign(t, claims(nil)), other, scoped("all")))
		if next.Hop != 1 || next.From != "b" || next.Scope != redirect.ScopeAll || next.RequestID == rid || next.KeyID != "key-1" || next.BodySHA != redirect.BodySHA(other) {
			t.Errorf("fresh redirect token %+v, want hop 1 from b on key-1 with its own request ID and body", next)
		}
	})

	t.Run("stale map: a second redirect, never back to the origin", func(t *testing.T) {
		// b no longer runs m; the map lists m on a (the origin), b and c.
		b := newSite(t, "b", nil, testKeys(t))
		push(t, fleetDoc(1, placement("m", redirect.KindChat, "a")), b)
		for i := 0; i < 10; i++ {
			c := claims(func(c *redirect.Claims) { c.RequestID = fmt.Sprintf("rid-%d", i) })
			loc, next := locationToken(t, send(b, chatPath+"?rt="+sign(t, c), chatBody, map[string]string{"X-Session-ID": fmt.Sprint(i)}))
			if loc.Host != "c.example.com" {
				t.Fatalf("Location %s, want c (never a, the origin)", loc)
			}
			want := c
			want.Site, want.Hop, want.IssuedAt, want.Exp, want.JTI = "c", 2, next.IssuedAt, next.Exp, next.JTI
			if next != want || !redirect.ValidJTI(next.JTI) {
				t.Fatalf("second token %+v, want %+v", next, want)
			}
		}
	})

	t.Run("loop guard: no third redirect", func(t *testing.T) {
		c := newSite(t, "c", nil, testKeys(t))
		push(t, fleetDoc(1), c)
		done := sendAsync(c, chatPath+"?rt="+sign(t, claims(func(cl *redirect.Claims) { cl.Site, cl.Hop = "c", 2 })), chatBody, nil)
		serveOne(t, c) // falls through to the normal path (which 404s when nothing serves it)
		if w := <-done; w.Header().Get("Location") != "" {
			t.Fatal("a request redirected twice must never be redirected again")
		}
	})

	t.Run("token used on another path: refused", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"e": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		if w := send(b, "/v1/embeddings?rt="+sign(t, claims(nil)), []byte(`{"model":"e","input":"x"}`), nil); w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401: a chat token must not unlock /v1/embeddings", w.Code)
		}
	})

	t.Run("single use: the same token twice", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		tok := sign(t, claims(nil))
		done := sendAsync(b, chatPath+"?rt="+tok, chatBody, nil)
		serveOne(t, b)
		<-done
		if w := send(b, chatPath+"?rt="+tok, chatBody, nil); w.Code != http.StatusUnauthorized {
			t.Fatalf("replay: status %d, want 401", w.Code)
		}
		if strings.Join(b.rejects, ",") != "replay" {
			t.Errorf("reject reasons %v, want [replay]", b.rejects)
		}
		// With its key, the replayed request is served fresh on it.
		done = sendAsync(b, chatPath+"?rt="+tok, chatBody, bearer())
		p := serveOne(t, b)
		if w := <-done; w.Code != http.StatusOK || p.KeyID != "key-1" || w.Header().Get("X-Request-ID") == rid {
			t.Fatalf("replay + key: status %d key %q, want 200 as a fresh request on key-1", w.Code, p.KeyID)
		}
	})

	t.Run("broken token + valid key: served fresh on the key", func(t *testing.T) {
		b := newSite(t, "b", map[string]int{"m": 1}, testKeys(t))
		push(t, fleetDoc(1), b)
		expired := testSigner(t).Sign(claims(nil), time.Now().Add(-2*time.Minute))
		done := sendAsync(b, chatPath+"?rt="+expired, chatBody, bearer())
		p := serveOne(t, b)
		w := <-done
		if w.Code != http.StatusOK || p.KeyID != "key-1" {
			t.Fatalf("status %d key %q, want 200 on the presented key", w.Code, p.KeyID)
		}
		if w.Header().Get("X-Request-ID") == rid {
			t.Error("a fresh request must not adopt the refused token's request ID")
		}
		if strings.Join(b.rejects, ",") != "expired" {
			t.Errorf("reject reasons %v, want [expired]", b.rejects)
		}
	})
}

// TestRedirect_SingleUseCannotBeStolen: single use is per token (jti), and a
// token is marked used only once every check passed. Each case ends with the
// right client served (200).
func TestRedirect_SingleUseCannotBeStolen(t *testing.T) {
	const clientRID = "6f1d3a52-0c4b-4d2e-9a8f-2b7c5e1d9a00" // a client-chosen X-Request-ID
	entryAndTarget := func(t *testing.T) (a, b *testSite) {
		keys := testKeys(t)
		a, b = newSite(t, "a", nil, keys), newSite(t, "b", map[string]int{"m": 1}, keys)
		push(t, fleetDoc(1), a, b)
		return a, b
	}
	// follow repeats a 307 at its target without the key, as clients do.
	follow := func(t *testing.T, b *testSite, w *httptest.ResponseRecorder, body []byte) (*httptest.ResponseRecorder, *domain.PendingRequest) {
		t.Helper()
		loc, _ := locationToken(t, w)
		done := sendAsync(b, loc.RequestURI(), body, nil)
		p := serveOne(t, b)
		return <-done, p
	}
	withRID := func(key string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + key, "X-Request-ID": clientRID}
	}

	t.Run("a leaked link sent with another body does not use up the token", func(t *testing.T) {
		a, b := entryAndTarget(t)
		w := send(a, chatPath, chatBody, bearer())
		loc, _ := locationToken(t, w)
		if leak := send(b, loc.RequestURI(), []byte(`{"model":"m","messages":[{"role":"user","content":"stolen"}]}`), nil); leak.Code != http.StatusUnauthorized {
			t.Fatalf("leaked link with another body: status %d, want 401", leak.Code)
		}
		if got, p := follow(t, b, w, chatBody); got.Code != http.StatusOK || p.KeyID != "key-1" {
			t.Fatalf("the real client: status %d key %q, want 200 on key-1", got.Code, p.KeyID)
		}
		if strings.Join(b.rejects, ",") != "body" {
			t.Errorf("reject reasons %v, want [body]", b.rejects)
		}
	})

	t.Run("another key with the victim's X-Request-ID does not use up the victim's token", func(t *testing.T) {
		a, b := entryAndTarget(t)
		victim := send(a, chatPath, chatBody, withRID(redirectKey))
		attacker := send(a, chatPath, chatBody, withRID(otherKey))
		if got, p := follow(t, b, attacker, chatBody); got.Code != http.StatusOK || p.KeyID != "key-2" {
			t.Fatalf("the other key: status %d key %q, want 200 on its own key-2", got.Code, p.KeyID)
		}
		got, p := follow(t, b, victim, chatBody)
		if got.Code != http.StatusOK || p.KeyID != "key-1" || got.Header().Get("X-Request-ID") != clientRID {
			t.Fatalf("the victim: status %d key %q X-Request-ID %q, want 200 on key-1 under %s",
				got.Code, p.KeyID, got.Header().Get("X-Request-ID"), clientRID)
		}
	})

	t.Run("a same-key retry with its own X-Request-ID is served", func(t *testing.T) {
		a, b := entryAndTarget(t)
		for try := 1; try <= 2; try++ {
			got, p := follow(t, b, send(a, chatPath, chatBody, withRID(redirectKey)), chatBody)
			if got.Code != http.StatusOK || p.KeyID != "key-1" || got.Header().Get("X-Request-ID") != clientRID {
				t.Fatalf("try %d: status %d key %q X-Request-ID %q, want 200 on key-1 under %s",
					try, got.Code, p.KeyID, got.Header().Get("X-Request-ID"), clientRID)
			}
		}
		if len(b.rejects) != 0 {
			t.Errorf("reject reasons %v, want none", b.rejects)
		}
	})
}

// TestRedirect_RoundTrip: a real HTTP client against two TLS policlouds. It
// sends its key to a, follows the 307 without the key (as clients do when the
// host changes) and gets the answer from b under the first request ID.
func TestRedirect_RoundTrip(t *testing.T) {
	keys := testKeys(t)
	a, b := newSite(t, "a", nil, keys), newSite(t, "b", map[string]int{"m": 1}, keys)
	srvA, srvB := httptest.NewTLSServer(a.engine), httptest.NewTLSServer(b.engine)
	defer srvA.Close()
	defer srvB.Close()
	doc := fleetDoc(1)
	doc.Policlouds[0].URL, doc.Policlouds[1].URL = srvA.URL, srvB.URL
	doc.Placements = doc.Placements[:1] // m on b only
	push(t, doc, a, b)

	verifier := testSigner(t)
	var rid string
	client := srvA.Client() // httptest TLS servers share one certificate
	client.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		// Both servers are 127.0.0.1, so Go would keep the key; real clients
		// drop it on a host change. Drop it to test the token alone.
		req.Header.Del("Authorization")
		c, err := verifier.Verify(req.URL.Query().Get(redirect.QueryParam), time.Now())
		rid = c.RequestID
		return err
	}
	req, _ := http.NewRequest(http.MethodPost, srvA.URL+chatPath, bytes.NewReader(chatBody))
	req.Header.Set("Authorization", "Bearer "+redirectKey)
	req.Header.Set("Content-Type", "application/json")
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.Do(req)
		done <- result{resp, err}
	}()
	p := serveOne(t, b) // policloud b's processor answers the redirected request
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.resp.Body.Close()
	got, _ := io.ReadAll(r.resp.Body)
	if r.resp.StatusCode != http.StatusOK || string(got) != `{"served":true}` || p.KeyID != "key-1" {
		t.Fatalf("status %d body %s key %q, want 200 from b on key-1", r.resp.StatusCode, got, p.KeyID)
	}
	if !strings.HasPrefix(r.resp.Request.URL.String(), srvB.URL) || rid == "" || r.resp.Header.Get("X-Request-ID") != rid {
		t.Errorf("final URL %s, X-Request-ID %q, want b and the first router's %q", r.resp.Request.URL, r.resp.Header.Get("X-Request-ID"), rid)
	}
	if len(a.queue) != 0 || len(a.redirs) != 1 || *a.charges != 0 || *b.charges != 1 {
		t.Errorf("a queued %d, redirected %d, charged %d; b charged %d: want 0, 1, 0; 1",
			len(a.queue), len(a.redirs), *a.charges, *b.charges)
	}
}

// TestSiblingMapAdmin: GET and POST /admin/sibling-map with the v2 format.
func TestSiblingMapAdmin(t *testing.T) {
	s := newSite(t, "a", nil, testKeys(t))
	h := api.NewHandlers(nil, nil, nil, time.Second, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	e := gin.New()
	e.GET("/admin/sibling-map", h.GetSiblingMap)
	e.POST("/admin/sibling-map", h.ReplaceSiblingMap)
	call := func(method string, body any) *httptest.ResponseRecorder {
		var b []byte
		switch v := body.(type) {
		case nil:
		case string:
			b = []byte(v)
		default:
			b, _ = json.Marshal(v)
		}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, httptest.NewRequest(method, "/admin/sibling-map", bytes.NewReader(b)))
		return w
	}

	if w := call(http.MethodGet, nil); w.Code != http.StatusNotImplemented {
		t.Fatalf("disabled: status %d, want 501", w.Code)
	}
	h.SetRedirect(s.rd)

	for _, v := range []uint64{9, 10} { // numeric: 10 > 9 (as strings "10" < "9")
		doc := fleetDoc(v)
		doc.HintsExpireAt = "2026-11-03T12:00:00Z"
		if w := call(http.MethodPost, doc); w.Code != http.StatusOK {
			t.Fatalf("push v%d: %d %s", v, w.Code, w.Body)
		}
	}
	var got struct {
		SiteID        string               `json:"site_id"`
		Schema        int                  `json:"schema"`
		Version       uint64               `json:"version"`
		Region        string               `json:"region"`
		Policlouds    []redirect.Policloud `json:"policlouds"`
		Placements    []redirect.Placement `json:"placements"`
		HintsExpireAt string               `json:"hints_expire_at"`
	}
	w := call(http.MethodGet, nil)
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.SiteID != "a" || got.Schema != 2 || got.Version != 10 ||
		got.Region != "eu" || len(got.Policlouds) != 3 || len(got.Placements) != 4 || got.HintsExpireAt != "2026-11-03T12:00:00Z" {
		t.Fatalf("GET: %s", w.Body)
	}

	schema1 := fleetDoc(11)
	schema1.Schema = 1
	httpURL := fleetDoc(11)
	httpURL.Policlouds[0].URL = "http://a.example.com"
	foreign := fleetDoc(11)
	foreign.Policlouds[2].URL = "https://c.attacker.net"
	for name, tc := range map[string]struct {
		body any
		want int
	}{
		"lower version":  {fleetDoc(9), http.StatusConflict},
		"unknown schema": {schema1, http.StatusBadRequest},
		"http url":       {httpURL, http.StatusBadRequest},
		"foreign host":   {foreign, http.StatusBadRequest},
		"no version":     {`{"schema":2,"region":"eu","policlouds":[],"placements":[]}`, http.StatusBadRequest},
		"not json":       {"{", http.StatusBadRequest},
	} {
		if w := call(http.MethodPost, tc.body); w.Code != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", name, w.Code, tc.want, w.Body)
		}
	}
	if v := s.rd.Map.Version(); v != 10 {
		t.Fatalf("a refused push changed the map: version %d, want 10", v)
	}
}

// TestAccessLog_RedactsToken: gin logs path?query; the token must not reach
// the access log, other parameters stay readable.
func TestAccessLog_RedactsToken(t *testing.T) {
	line := api.AccessLogFormatter(gin.LogFormatterParams{
		TimeStamp: time.Now(), Method: http.MethodPost, StatusCode: http.StatusOK,
		Path: chatPath + "?api-version=2024&rt=v2.k1.SECRETPAYLOAD.SECRETSIG",
	})
	if strings.Contains(line, "SECRET") {
		t.Fatalf("token in access log: %s", line)
	}
	if !strings.Contains(line, "api-version=2024") || !strings.Contains(line, "rt=REDACTED") {
		t.Errorf("unexpected log line: %s", line)
	}
	plain := api.AccessLogFormatter(gin.LogFormatterParams{TimeStamp: time.Now(), Path: "/admin/api-keys/k?version=v2"})
	if !strings.Contains(plain, "/admin/api-keys/k?version=v2") {
		t.Errorf("a path without a token must be logged unchanged: %s", plain)
	}
}

// TestSiblingMapGauges: hivenet_sibling_map_version and
// hivenet_sibling_map_hints_expired are read at scrape time.
func TestSiblingMapGauges(t *testing.T) {
	m := metrics.NewRouterMetrics()
	version, expired := 0.0, 1.0
	m.RegisterSiblingMap(func() float64 { return version }, func() float64 { return expired })
	read := func() map[string]float64 {
		t.Helper()
		fams, err := m.Gather()
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, f := range fams {
			if n := f.GetName(); n == "hivenet_sibling_map_version" || n == "hivenet_sibling_map_hints_expired" {
				out[n] = f.GetMetric()[0].GetGauge().GetValue()
			}
		}
		return out
	}
	if got := read(); got["hivenet_sibling_map_version"] != 0 || got["hivenet_sibling_map_hints_expired"] != 1 || len(got) != 2 {
		t.Fatalf("before a push: %v", got)
	}
	version, expired = 42, 0
	if got := read(); got["hivenet_sibling_map_version"] != 42 || got["hivenet_sibling_map_hints_expired"] != 0 {
		t.Fatalf("after a push: %v", got)
	}
}
