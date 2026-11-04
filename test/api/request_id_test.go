// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
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

// serveWithRequestID runs the middleware in front of a handler that records
// what downstream code sees: the context ID and the request header that is
// later forwarded to the agent.
func serveWithRequestID(t *testing.T, header string) (resp, ctxID, fwdHeader string) {
	t.Helper()
	r := gin.New()
	r.Use(api.RequestIDMiddleware())
	r.GET("/x", func(c *gin.Context) {
		ctxID = c.GetString("request_id")
		fwdHeader = c.Request.Header.Get("X-Request-ID")
		c.Status(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if header != "" {
		req.Header.Set("X-Request-ID", header)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w.Header().Get("X-Request-ID"), ctxID, fwdHeader
}

// TestRequestID_GeneratedIsV7 checks that a request without an ID gets a
// UUIDv7, the same value everywhere downstream.
func TestRequestID_GeneratedIsV7(t *testing.T) {
	resp, ctxID, fwd := serveWithRequestID(t, "")
	parsed, err := uuid.Parse(resp)
	if err != nil {
		t.Fatalf("generated X-Request-ID is not a UUID: %q", resp)
	}
	if parsed.Version() != 7 {
		t.Errorf("generated ID version = %d, want 7", parsed.Version())
	}
	if ctxID != resp || fwd != resp {
		t.Errorf("ID differs: response %q, context %q, forwarded %q", resp, ctxID, fwd)
	}
}

// TestRequestID_ClientIDReusedCanonical checks that a valid client UUID is
// kept (so a retry with the same header keeps its identity) and normalised
// to the canonical lowercase form.
func TestRequestID_ClientIDReusedCanonical(t *testing.T) {
	id := uuid.New()
	for _, in := range []string{id.String(), strings.ToUpper(id.String()), "urn:uuid:" + id.String()} {
		resp, ctxID, fwd := serveWithRequestID(t, in)
		if resp != id.String() || ctxID != id.String() || fwd != id.String() {
			t.Errorf("input %q: response %q, context %q, forwarded %q; want %q everywhere",
				in, resp, ctxID, fwd, id.String())
		}
	}
}

// TestRequestID_InvalidClientIDNotForwarded checks that an invalid client
// value is replaced everywhere, including the header forwarded to the agent,
// so it can't reach agent or backend logs.
func TestRequestID_InvalidClientIDNotForwarded(t *testing.T) {
	resp, ctxID, fwd := serveWithRequestID(t, "'; DROP TABLE audit;--")
	if _, err := uuid.Parse(resp); err != nil {
		t.Fatalf("invalid client ID was not replaced: %q", resp)
	}
	if ctxID != resp || fwd != resp {
		t.Errorf("ID differs: response %q, context %q, forwarded %q", resp, ctxID, fwd)
	}
}

// TestRequestID_PendingRequestUsesMiddlewareID checks that the ID the
// processor logs and traces is the same one the client sees, and that it is
// forwarded to the agent, for both the chat and the embeddings paths.
func TestRequestID_PendingRequestUsesMiddlewareID(t *testing.T) {
	cases := []struct {
		name string
		path string
		body string
		call func(h *api.Handlers, c *gin.Context)
	}{
		{"chat", "/v1/messages",
			`{"model":"qwen","messages":[{"role":"user","content":"hi"}],"max_tokens":10}`,
			func(h *api.Handlers, c *gin.Context) { h.Passthrough(c) }},
		{"embeddings", "/v1/embeddings",
			`{"model":"embed","input":"hi"}`,
			func(h *api.Handlers, c *gin.Context) { h.Embeddings(c) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := make(chan *domain.PendingRequest, 1)
			h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
			c, _ := newCtx(tc.path, []byte(tc.body))
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
				pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
			case <-time.After(time.Second):
				t.Fatal("request was not enqueued")
			}
			<-done
		})
	}
}
