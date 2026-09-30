// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"hivenet_router/internal/domain"
)

// The headers the router captures for forwarding must never carry the client's
// router credentials, whatever the forwarding path does with them later.
func TestCaptureRawBodyDropsClientCredentials(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	c.Request.Header.Set("Authorization", "Bearer sk-router-key")
	c.Request.Header.Set("X-Api-Key", "sk-router-key")
	c.Request.Header.Set("Cookie", "session=1")
	c.Request.Header.Set("Proxy-Authorization", "Basic x")
	c.Request.Header.Set("Anthropic-Version", "2023-06-01")
	c.Set(gin.BodyBytesKey, []byte(`{"model":"m"}`))

	req := &domain.ChatRequest{}
	if !captureRawBody(c, req, "test") {
		t.Fatal("captureRawBody failed")
	}
	for _, k := range []string{"Authorization", "X-Api-Key", "Cookie", "Proxy-Authorization"} {
		if req.HttpHeaders.Get(k) != "" {
			t.Errorf("captured headers still carry %s", k)
		}
	}
	if req.HttpHeaders.Get("Anthropic-Version") == "" {
		t.Error("other client headers must still be forwarded")
	}
	if c.Request.Header.Get("Authorization") == "" {
		t.Error("the incoming request must not be modified")
	}
}
