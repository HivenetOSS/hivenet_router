// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

// Cross-site redirect through the real middleware chain: AuthMiddleware over
// redirect.Wrap, then Passthrough. Each "site" is a gin engine with its own
// healthy-agent count and request queue; a stub processor answers what a
// site queues.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/redirect"

	"github.com/gin-gonic/gin"
)

const (
	redirectKey = "sk-redirect-test"
	chatPath    = "/v1/chat/completions"
)

var redirectSecret = []byte("0123456789abcdef0123456789abcdef")

type testSite struct {
	engine  *gin.Engine
	rd      *redirect.Redirector
	queue   chan *domain.PendingRequest
	redirs  []string // "model→site@hop" per 307 written
	rejects []string
}

// newSite builds one site. healthy is its healthy-agent count for every model;
// keys is the dynamic key registry shared by all sites (keys are pushed to
// every router).
func newSite(t *testing.T, id string, healthy int, keys *auth.DynamicKeyProvider) *testSite {
	t.Helper()
	s := &testSite{queue: make(chan *domain.PendingRequest, 4)}
	s.rd = &redirect.Redirector{
		Self:   id,
		Map:    redirect.NewMap(),
		Signer: redirect.NewSigner(redirectSecret, 120*time.Second),
		OnRedirect: func(model, site string, hop int) {
			s.redirs = append(s.redirs, model+"→"+site+"@"+string(rune('0'+hop)))
		},
		OnReject: func(reason string) { s.rejects = append(s.rejects, reason) },
	}
	h := api.NewHandlers(
		nil, nil, s.queue, 2*time.Second,
		nil, nil, nil, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000},
		nil, nil, nil, nil, keys, nil,
		func(string) int { return healthy },
		nil, nil, nil, nil, nil, nil, nil,
	)
	h.SetRedirect(s.rd)
	s.engine = gin.New()
	s.engine.Use(api.RequestIDMiddleware())
	v1 := s.engine.Group("/v1", api.AuthMiddleware(redirect.Wrap(s.rd, keys, keys)))
	v1.POST("/chat/completions", h.Passthrough)
	v1.POST("/embeddings", func(c *gin.Context) { c.Status(http.StatusOK) })
	return s
}

func testKeys(t *testing.T) *auth.DynamicKeyProvider {
	t.Helper()
	h := sha256.Sum256([]byte(redirectKey))
	keys := auth.NewDynamicKeyProvider()
	if err := keys.Upsert("v1", auth.DynamicKeyEntry{
		ID: "key-1", KeyHash: hex.EncodeToString(h[:]), Owner: "tenant-1", Name: "test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

// fleet pushes the same sibling map to every site: site "a" (the entry) runs
// nothing, "b" and "c" run model m.
func fleet(t *testing.T, sites ...*testSite) {
	t.Helper()
	for _, s := range sites {
		if err := s.rd.Map.Replace("v1", []redirect.Site{
			{ID: "a", URL: "https://a.example.com"},
			{ID: "b", URL: "https://b.example.com", Models: []string{"m"}},
			{ID: "c", URL: "https://c.example.com", Models: []string{"m"}},
		}); err != nil {
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

func signed(c redirect.Claims) string {
	return redirect.NewSigner(redirectSecret, 120*time.Second).Sign(c, time.Now())
}

var chatBody = []byte(`{"model":"m","messages":[{"role":"user","content":"hello"}],"max_tokens":5}`)

func bodySHA(b []byte) string { return redirect.BodySHA(b) }

// TestRedirect_MissWrites307: the entry site has no agent for the model, so
// it answers 307 to a sibling: an absolute https Location with the request's
// own path and query plus a token that the sibling will accept, and nothing
// is queued or reserved here.
func TestRedirect_MissWrites307(t *testing.T) {
	keys := testKeys(t)
	a := newSite(t, "a", 0, keys)
	fleet(t, a)

	w := send(a, chatPath+"?api-version=2024", chatBody, map[string]string{"Authorization": "Bearer " + redirectKey})

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status %d, want 307; body %s", w.Code, w.Body)
	}
	loc, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if loc.Scheme != "https" || (loc.Host != "b.example.com" && loc.Host != "c.example.com") || loc.Path != chatPath {
		t.Fatalf("Location %s: want https://{b,c}.example.com%s", loc, chatPath)
	}
	if loc.Query().Get("api-version") != "2024" {
		t.Errorf("request query not kept: %s", loc.RawQuery)
	}
	c, err := redirect.NewSigner(redirectSecret, time.Minute).Verify(loc.Query().Get("rt"), time.Now())
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	want := redirect.Claims{KeyID: "key-1", RequestID: c.RequestID, Model: "m", Site: strings.TrimSuffix(loc.Host, ".example.com"),
		Path: chatPath, Hop: 1, BodySHA: bodySHA(chatBody), Exp: c.Exp}
	if c != want || c.RequestID == "" {
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
	if len(a.redirs) != 1 || a.redirs[0] != "m→"+c.Site+"@1" {
		t.Errorf("redirect metric %v", a.redirs)
	}
}

// TestRedirect_SessionAffinity: the same key and X-Session-ID always get the
// same sibling.
func TestRedirect_SessionAffinity(t *testing.T) {
	keys := testKeys(t)
	a := newSite(t, "a", 0, keys)
	fleet(t, a)
	hdr := map[string]string{"Authorization": "Bearer " + redirectKey, "X-Session-ID": "conversation-42"}
	first := send(a, chatPath, chatBody, hdr)
	for i := 0; i < 20; i++ {
		got := send(a, chatPath, chatBody, hdr)
		if host(got) != host(first) {
			t.Fatalf("same key+session went to %s then %s", host(first), host(got))
		}
	}
}

func host(w *httptest.ResponseRecorder) string {
	u, _ := url.Parse(w.Header().Get("Location"))
	return u.Host
}

// TestRedirect_ServedLocallyOrUnchanged: no 307 when a local agent serves the
// model, when no other site serves it, or when redirect is disabled — the
// request is queued exactly as before.
func TestRedirect_ServedLocallyOrUnchanged(t *testing.T) {
	cases := []struct {
		name    string
		healthy int
		model   string
		enable  bool
	}{
		{"local agent", 1, "m", true},
		{"no site serves the model (existing 404 path)", 0, "unknown", true},
		{"redirect disabled", 0, "m", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSite(t, "a", tc.healthy, testKeys(t))
			fleet(t, s)
			if !tc.enable {
				s.rd = nil
				s = rebuildWithout(t, s)
			}
			body := []byte(`{"model":"` + tc.model + `","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`)
			done := make(chan *httptest.ResponseRecorder)
			go func() { done <- send(s, chatPath, body, map[string]string{"Authorization": "Bearer " + redirectKey}) }()
			serveOne(t, s)
			if w := <-done; w.Code != http.StatusOK || w.Header().Get("Location") != "" {
				t.Fatalf("status %d Location %q, want 200 and no redirect", w.Code, w.Header().Get("Location"))
			}
		})
	}
}

// rebuildWithout returns a site identical to s but with redirect disabled
// (SetRedirect never called, API provider not wrapped).
func rebuildWithout(t *testing.T, s *testSite) *testSite {
	t.Helper()
	keys := testKeys(t)
	h := api.NewHandlers(nil, nil, s.queue, 2*time.Second, nil, nil, nil,
		&fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000},
		nil, nil, nil, nil, keys, nil, func(string) int { return 0 }, nil, nil, nil, nil, nil, nil, nil)
	s.engine = gin.New()
	s.engine.Use(api.RequestIDMiddleware())
	s.engine.Group("/v1", api.AuthMiddleware(redirect.Wrap(nil, keys, keys))).POST("/chat/completions", h.Passthrough)
	return s
}

// TestRedirect_AtTheSibling covers the receiving side: a token-only request is
// served; a different body is refused; a stale map redirects once more; the
// loop guard never redirects a third time; a token is no good on another path.
func TestRedirect_AtTheSibling(t *testing.T) {
	claims := redirect.Claims{KeyID: "key-1", Model: "m", Site: "b", Path: chatPath, Hop: 1, BodySHA: bodySHA(chatBody)}

	t.Run("token only, same body: served with the key's identity", func(t *testing.T) {
		b := newSite(t, "b", 1, testKeys(t))
		fleet(t, b)
		done := make(chan *httptest.ResponseRecorder)
		go func() { done <- send(b, chatPath+"?rt="+signed(claims), chatBody, nil) }()
		p := serveOne(t, b)
		if w := <-done; w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
		if p.KeyID != "key-1" || p.TenantID != "tenant-1" {
			t.Errorf("served as key %q tenant %q, want key-1 / tenant-1", p.KeyID, p.TenantID)
		}
	})

	t.Run("different body: refused", func(t *testing.T) {
		b := newSite(t, "b", 1, testKeys(t))
		w := send(b, chatPath+"?rt="+signed(claims), []byte(`{"model":"m","messages":[{"role":"user","content":"another prompt"}]}`), nil)
		if w.Code != http.StatusUnauthorized || len(b.queue) != 0 {
			t.Fatalf("status %d queued %d, want 401 and nothing queued", w.Code, len(b.queue))
		}
		if len(b.rejects) != 1 || b.rejects[0] != "body" {
			t.Errorf("reject reasons %v, want [body]", b.rejects)
		}
	})

	t.Run("stale map: one more redirect, hop 2", func(t *testing.T) {
		b := newSite(t, "b", 0, testKeys(t)) // the map says b runs m, but it no longer does
		fleet(t, b)
		w := send(b, chatPath+"?rt="+signed(claims), chatBody, nil)
		if w.Code != http.StatusTemporaryRedirect || host(w) != "c.example.com" {
			t.Fatalf("status %d Location %q, want 307 to c", w.Code, w.Header().Get("Location"))
		}
		loc, _ := url.Parse(w.Header().Get("Location"))
		c, err := redirect.NewSigner(redirectSecret, time.Minute).Verify(loc.Query().Get("rt"), time.Now())
		if err != nil || c.Hop != 2 || c.KeyID != "key-1" {
			t.Fatalf("second token: %+v %v", c, err)
		}
	})

	t.Run("loop guard: no third redirect", func(t *testing.T) {
		c := newSite(t, "c", 0, testKeys(t))
		fleet(t, c)
		hop2 := claims
		hop2.Site, hop2.Hop = "c", 2
		done := make(chan *httptest.ResponseRecorder)
		go func() { done <- send(c, chatPath+"?rt="+signed(hop2), chatBody, nil) }()
		serveOne(t, c) // falls through to the normal path (which 404s when nothing serves it)
		if w := <-done; w.Header().Get("Location") != "" {
			t.Fatal("a request redirected twice must never be redirected again")
		}
	})

	t.Run("token used on another path: refused", func(t *testing.T) {
		b := newSite(t, "b", 1, testKeys(t))
		w := send(b, "/v1/embeddings?rt="+signed(claims), []byte(`{"model":"m","input":"x"}`), nil)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401: a chat token must not unlock /v1/embeddings", w.Code)
		}
	})
}

// TestRedirect_RoundTrip: a real HTTP client against two TLS sites. It sends
// its key to site a, follows the 307 without the key (as clients do when
// the host changes) and gets the answer from site b.
func TestRedirect_RoundTrip(t *testing.T) {
	keys := testKeys(t)
	a, b := newSite(t, "a", 0, keys), newSite(t, "b", 1, keys)
	srvA, srvB := httptest.NewTLSServer(a.engine), httptest.NewTLSServer(b.engine)
	defer srvA.Close()
	defer srvB.Close()
	for _, s := range []*testSite{a, b} {
		if err := s.rd.Map.Replace("v1", []redirect.Site{
			{ID: "a", URL: srvA.URL}, {ID: "b", URL: srvB.URL, Models: []string{"m"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	client := srvA.Client() // httptest TLS servers share one certificate
	client.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		// Both test servers are 127.0.0.1, so Go would keep the key; real
		// clients drop it on a host change. Drop it to test the token alone.
		req.Header.Del("Authorization")
		return nil
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
	serveOne(t, b) // site b's processor answers the redirected request
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	resp := r.resp
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(got) != `{"served":true}` {
		t.Fatalf("status %d body %s, want 200 from site b", resp.StatusCode, got)
	}
	if !strings.HasPrefix(resp.Request.URL.String(), srvB.URL) {
		t.Errorf("final URL %s, want site b", resp.Request.URL)
	}
	if len(a.queue) != 0 || len(a.redirs) != 1 {
		t.Errorf("site a queued %d and redirected %d times, want 0 and 1", len(a.queue), len(a.redirs))
	}
}

// TestSiblingMapAdmin: GET and POST /admin/sibling-map.
func TestSiblingMapAdmin(t *testing.T) {
	s := newSite(t, "a", 0, testKeys(t))
	h := api.NewHandlers(nil, nil, nil, time.Second, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	e := gin.New()
	e.GET("/admin/sibling-map", h.GetSiblingMap)
	e.POST("/admin/sibling-map", h.ReplaceSiblingMap)
	call := func(method, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, httptest.NewRequest(method, "/admin/sibling-map", strings.NewReader(body)))
		return w
	}

	if w := call(http.MethodGet, ""); w.Code != http.StatusNotImplemented {
		t.Fatalf("disabled: status %d, want 501", w.Code)
	}
	h.SetRedirect(s.rd)

	push := `{"version":"v2","sites":[{"id":"b","url":"https://b.example.com","models":["m"]}]}`
	if w := call(http.MethodPost, push); w.Code != http.StatusOK {
		t.Fatalf("push: %d %s", w.Code, w.Body)
	}
	w := call(http.MethodGet, "")
	var got struct {
		SiteID  string          `json:"site_id"`
		Version string          `json:"version"`
		Sites   []redirect.Site `json:"sites"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.SiteID != "a" || got.Version != "v2" || len(got.Sites) != 1 {
		t.Fatalf("GET: %s", w.Body)
	}
	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"older version":   {`{"version":"v1","sites":[]}`, http.StatusConflict},
		"http site url":   {`{"version":"v3","sites":[{"id":"b","url":"http://b.example.com"}]}`, http.StatusBadRequest},
		"missing version": {`{"sites":[]}`, http.StatusBadRequest},
		"not json":        {`{`, http.StatusBadRequest},
	} {
		if w := call(http.MethodPost, tc.body); w.Code != tc.want {
			t.Errorf("%s: status %d, want %d", name, w.Code, tc.want)
		}
	}
}

// TestAccessLog_RedactsToken: gin logs path?query; the token must not reach
// the access log, other parameters stay readable.
func TestAccessLog_RedactsToken(t *testing.T) {
	line := api.AccessLogFormatter(gin.LogFormatterParams{
		TimeStamp: time.Now(), Method: http.MethodPost, StatusCode: http.StatusOK,
		Path: chatPath + "?api-version=2024&rt=v1.SECRETPAYLOAD.SECRETSIG",
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
