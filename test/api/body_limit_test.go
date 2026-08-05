// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/api"

	"github.com/gin-gonic/gin"
)

// TestBodyLimitMiddleware covers the branches of the /v1 body-size guard: the
// fast Content-Length rejection, the MaxBytesReader fallback for a dishonest
// Content-Length, the pass-through of an under-limit body, and the disabled case.
func TestBodyLimitMiddleware(t *testing.T) {
	const limit = 64

	// A stand-in for a real /v1 handler: it must be able to read the full body
	// that the middleware buffered and handed back.
	var gotBody []byte
	okHandler := func(c *gin.Context) {
		gotBody, _ = io.ReadAll(c.Request.Body)
		c.Status(http.StatusOK)
	}

	newReq := func(n int, lieEmpty bool) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader(bytes.Repeat([]byte("a"), n)))
		if lieEmpty {
			req.ContentLength = 0 // claim an empty body while actually sending n bytes
		}
		return req
	}

	serve := func(mw gin.HandlerFunc, req *http.Request) *httptest.ResponseRecorder {
		gotBody = nil
		r := gin.New()
		r.Use(mw)
		r.POST("/x", okHandler)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	t.Run("under limit passes and body is readable downstream", func(t *testing.T) {
		w := serve(api.BodyLimitMiddleware(limit), newReq(10, false))
		if w.Code != http.StatusOK {
			t.Fatalf("code = %d, want 200", w.Code)
		}
		if len(gotBody) != 10 {
			t.Fatalf("handler saw %d body bytes, want 10", len(gotBody))
		}
	})

	t.Run("over limit with honest length -> 413", func(t *testing.T) {
		w := serve(api.BodyLimitMiddleware(limit), newReq(limit+1, false))
		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("code = %d, want 413", w.Code)
		}
		// Must use the standard router error envelope, not a bespoke shape.
		if body := w.Body.String(); !strings.Contains(body, "request_invalid") || !strings.Contains(body, "too large") {
			t.Fatalf("413 body missing standard envelope: %s", body)
		}
	})

	t.Run("over limit with dishonest length -> 413 via reader", func(t *testing.T) {
		w := serve(api.BodyLimitMiddleware(limit), newReq(limit*4, true))
		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("code = %d, want 413", w.Code)
		}
	})

	t.Run("disabled when limit <= 0", func(t *testing.T) {
		w := serve(api.BodyLimitMiddleware(0), newReq(limit*10, false))
		if w.Code != http.StatusOK {
			t.Fatalf("code = %d, want 200 (disabled)", w.Code)
		}
	})
}
