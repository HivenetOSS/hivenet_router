// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// idSeen is what one request through the middleware looks like from the
// client and from downstream code.
type idSeen struct {
	resp, respClient string // X-Request-ID and X-Client-Request-ID returned to the client
	ctx, ctxClient   string // request_id and client_request_id in the Gin context
	fwd, fwdClient   string // X-Request-ID and X-Client-Request-ID left on the request (forwarded)
}

// serveWithRequestID runs the middleware in front of a handler that records
// what downstream code sees.
func serveWithRequestID(t *testing.T, headers map[string]string) idSeen {
	t.Helper()
	var s idSeen
	r := gin.New()
	r.Use(api.RequestIDMiddleware())
	r.GET("/x", func(c *gin.Context) {
		s.ctx, s.ctxClient = c.GetString("request_id"), c.GetString("client_request_id")
		s.fwd, s.fwdClient = c.Request.Header.Get("X-Request-ID"), c.Request.Header.Get("X-Client-Request-ID")
		c.Status(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	s.resp, s.respClient = w.Header().Get("X-Request-ID"), w.Header().Get("X-Client-Request-ID")
	return s
}

// checkRouterID checks that the request ID is a router-minted UUIDv7, the
// same in the response, the context and the forwarded header, and different
// from what the client sent.
func checkRouterID(t *testing.T, s idSeen, clientSent string) {
	t.Helper()
	parsed, err := uuid.Parse(s.resp)
	if err != nil || parsed.Version() != 7 {
		t.Fatalf("X-Request-ID = %q, want a UUIDv7", s.resp)
	}
	if s.ctx != s.resp || s.fwd != s.resp {
		t.Errorf("ID differs: response %q, context %q, forwarded %q", s.resp, s.ctx, s.fwd)
	}
	if clientSent != "" && strings.EqualFold(s.resp, clientSent) {
		t.Errorf("X-Request-ID is the client's own ID %q, want one the router minted", s.resp)
	}
	if s.fwdClient != "" {
		t.Errorf("X-Client-Request-ID forwarded as %q, want it never forwarded", s.fwdClient)
	}
}

// TestRequestID_GeneratedIsV7 checks that a request without an ID gets a
// UUIDv7, the same value everywhere downstream, and no client ID.
func TestRequestID_GeneratedIsV7(t *testing.T) {
	s := serveWithRequestID(t, nil)
	checkRouterID(t, s, "")
	if s.respClient != "" || s.ctxClient != "" {
		t.Errorf("client ID %q / %q without one sent", s.respClient, s.ctxClient)
	}
}

// TestRequestID_RouterAlwaysMints checks that the router never adopts the
// client's ID as the request ID: two requests sending the same ID get two
// different request IDs, so engines and billing never see a duplicate.
func TestRequestID_RouterAlwaysMints(t *testing.T) {
	id := uuid.New().String()
	a := serveWithRequestID(t, map[string]string{"X-Request-ID": id})
	b := serveWithRequestID(t, map[string]string{"X-Request-ID": id})
	checkRouterID(t, a, id)
	checkRouterID(t, b, id)
	if a.resp == b.resp {
		t.Errorf("two requests with the same client ID got the same request ID %q", a.resp)
	}
}

// TestRequestID_ClientIDKeptCanonical checks that a valid client UUID, from
// X-Client-Request-ID or X-Request-ID, is kept as the client request ID in
// canonical form and echoed in X-Client-Request-ID.
func TestRequestID_ClientIDKeptCanonical(t *testing.T) {
	id := uuid.New()
	for _, header := range []string{"X-Request-ID", "X-Client-Request-ID"} {
		for _, in := range []string{id.String(), strings.ToUpper(id.String()), "urn:uuid:" + id.String()} {
			s := serveWithRequestID(t, map[string]string{header: in})
			checkRouterID(t, s, id.String())
			if s.respClient != id.String() || s.ctxClient != id.String() {
				t.Errorf("%s %q: client ID echoed %q, context %q; want %q", header, in, s.respClient, s.ctxClient, id.String())
			}
		}
	}
}

// TestRequestID_ClientHeaderWins checks that X-Client-Request-ID is preferred
// when a client sends both headers.
func TestRequestID_ClientHeaderWins(t *testing.T) {
	want := uuid.New().String()
	s := serveWithRequestID(t, map[string]string{"X-Client-Request-ID": want, "X-Request-ID": uuid.New().String()})
	if s.ctxClient != want {
		t.Errorf("client ID = %q, want the X-Client-Request-ID value %q", s.ctxClient, want)
	}
}

// TestRequestID_InvalidClientIDDropped checks that an invalid client value is
// not kept, echoed or forwarded, so it can't reach any log.
func TestRequestID_InvalidClientIDDropped(t *testing.T) {
	for _, header := range []string{"X-Request-ID", "X-Client-Request-ID"} {
		s := serveWithRequestID(t, map[string]string{header: "'; DROP TABLE audit;--"})
		checkRouterID(t, s, "")
		if s.respClient != "" || s.ctxClient != "" {
			t.Errorf("%s: invalid client ID kept: echoed %q, context %q", header, s.respClient, s.ctxClient)
		}
	}
}

// TestRequestID_PendingRequestUsesMiddlewareID checks that the ID the
// processor logs and traces is the same one the client sees, and that it is
// forwarded to the agent, on every inference path. It also checks that a
// backend echoing its own X-Request-ID doesn't add a second value for the
// client.
func TestRequestID_PendingRequestUsesMiddlewareID(t *testing.T) {
	cases := []struct {
		name string
		path string
		body string
		call func(h *api.Handlers, c *gin.Context)
	}{
		{"chat", "/v1/chat/completions",
			`{"model":"qwen","messages":[{"role":"user","content":"hi"}],"max_tokens":10}`,
			func(h *api.Handlers, c *gin.Context) { h.Passthrough(c) }},
		{"chat streaming", "/v1/chat/completions",
			`{"model":"qwen","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":10}`,
			func(h *api.Handlers, c *gin.Context) { h.Passthrough(c) }},
		{"messages", "/v1/messages",
			`{"model":"qwen","messages":[{"role":"user","content":"hi"}],"max_tokens":10}`,
			func(h *api.Handlers, c *gin.Context) { h.Passthrough(c) }},
		{"embeddings", "/v1/embeddings",
			`{"model":"embed","input":"hi"}`,
			func(h *api.Handlers, c *gin.Context) { h.Embeddings(c) }},
		{"rerank", "/v1/rerank",
			`{"model":"rerank","query":"q","documents":["a","b"]}`,
			func(h *api.Handlers, c *gin.Context) { h.Rerank(c) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := make(chan *domain.PendingRequest, 1)
			h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
			c, w := newCtx(tc.path, []byte(tc.body))
			c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})
			id := uuid.Must(uuid.NewV7()).String()
			c.Set("request_id", id)
			c.Request.Header.Set("X-Request-ID", id)

			done := make(chan struct{})
			go func() { tc.call(h, c); close(done) }()

			select {
			case pending := <-q:
				if pending.ID != id {
					t.Errorf("pending.ID = %q, want the middleware ID %q", pending.ID, id)
				}
				if got := pending.Request.HttpHeaders.Get("X-Request-ID"); got != id {
					t.Errorf("forwarded X-Request-ID = %q, want %q", got, id)
				}
				// The backend echoes its own X-Request-ID, as vLLM does with
				// --enable-request-id-headers.
				resp := &domain.ChatResponse{
					RawBytes:    []byte(`{"ok":true}`),
					HttpHeaders: http.Header{"X-Request-Id": {"backend-echo"}},
				}
				if strings.Contains(tc.body, `"stream":true`) {
					resp.Body = io.NopCloser(strings.NewReader("data: [DONE]\n\n"))
				}
				pending.Response <- resp
			case <-time.After(time.Second):
				t.Fatal("request was not enqueued")
			}
			<-done
			if got := w.Header().Values("X-Request-ID"); len(got) != 1 || got[0] != id {
				t.Errorf("client X-Request-ID = %q, want exactly [%q]", got, id)
			}
			if got := w.Header().Values("X-Upstream-Request-ID"); len(got) != 1 || got[0] != "backend-echo" {
				t.Errorf("X-Upstream-Request-ID = %q, want exactly [backend-echo]", got)
			}
		})
	}
}

// TestRequestID_FallbackWithoutMiddleware checks that a handler reached
// without RequestIDMiddleware (for example from a test or an embedded router)
// still gets a valid UUIDv7 instead of an empty ID.
func TestRequestID_FallbackWithoutMiddleware(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	c, _ := newCtx("/v1/embeddings", []byte(`{"model":"embed","input":"hi"}`))
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	done := make(chan struct{})
	go func() { h.Embeddings(c); close(done) }()

	select {
	case pending := <-q:
		parsed, err := uuid.Parse(pending.ID)
		if err != nil || parsed.Version() != 7 {
			t.Errorf("fallback ID = %q, want a UUIDv7", pending.ID)
		}
		if got := pending.Request.HttpHeaders.Get("X-Request-ID"); got != pending.ID {
			t.Errorf("forwarded X-Request-ID = %q, want %q", got, pending.ID)
		}
		if got := c.GetString("request_id"); got != pending.ID {
			t.Errorf("context request_id = %q, want the generated %q (audit reads it)", got, pending.ID)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
}

// TestRequestID_UpstreamEchoOfOurID checks that a backend echoing the
// router's own ID (vLLM with --enable-request-id-headers) adds no
// X-Upstream-Request-ID.
func TestRequestID_UpstreamEchoOfOurID(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	c, w := newCtx("/v1/embeddings", []byte(`{"model":"embed","input":"hi"}`))
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})
	id := uuid.Must(uuid.NewV7()).String()
	c.Set("request_id", id)

	done := make(chan struct{})
	go func() { h.Embeddings(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`), HttpHeaders: http.Header{"X-Request-Id": {id}}}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if got := w.Header().Values("X-Request-ID"); len(got) != 1 || got[0] != id {
		t.Errorf("client X-Request-ID = %q, want exactly [%q]", got, id)
	}
	if got := w.Header().Get("X-Upstream-Request-ID"); got != "" {
		t.Errorf("X-Upstream-Request-ID = %q for an echo of our own ID, want none", got)
	}
}
