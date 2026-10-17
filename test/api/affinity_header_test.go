// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// X-Hivenet-Agent-Source emission on both response writers: the chat/passthrough
// writer (writeSuccessResponse, streaming and non-streaming) and the shared
// embedding/rerank writer (enqueueInferenceRequest). Covers set-if-unset
// precedence and coexistence with the alias middleware's X-Hivenet-Route-Source.
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
)

// servePassthrough drives one full Passthrough request through the handler and
// returns the recorder after the supplied response has been written.
func servePassthrough(t *testing.T, resp *domain.ChatResponse) *httptest.ResponseRecorder {
	t.Helper()
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	body := []byte(`{"model":"qwen","messages":[{"role":"user","content":"hi"}],"max_tokens":10}`)
	c, w := newCtx("/v1/messages", body)
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()

	select {
	case pending := <-q:
		pending.Response <- resp
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	return w
}

// serveEmbeddings drives one /v1/embeddings request through the shared
// embedding/rerank writer (enqueueInferenceRequest).
func serveEmbeddings(t *testing.T, resp *domain.ChatResponse) *httptest.ResponseRecorder {
	t.Helper()
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	c, w := newCtx("/v1/embeddings", []byte(`{"model":"qwen","input":"hi"}`))

	done := make(chan struct{})
	go func() { h.Embeddings(c); close(done) }()

	select {
	case pending := <-q:
		pending.Response <- resp
	case <-time.After(time.Second):
		t.Fatal("embedding request was not enqueued")
	}
	<-done
	return w
}

func TestAffinityHeader_ChatWriter(t *testing.T) {
	t.Run("decision is exposed", func(t *testing.T) {
		w := servePassthrough(t, &domain.ChatResponse{
			RawBytes:       []byte(`{"ok":true}`),
			AffinitySource: "hit",
		})
		if got := w.Header().Get(api.HeaderAgentSource); got != "hit" {
			t.Errorf("%s = %q, want hit", api.HeaderAgentSource, got)
		}
	})

	t.Run("coexists with the alias route source", func(t *testing.T) {
		w := servePassthrough(t, &domain.ChatResponse{
			RawBytes: []byte(`{"ok":true}`),
			HttpHeaders: http.Header{
				api.HeaderRouteSource: []string{"pinned"},
			},
			AffinitySource: "miss",
		})
		if got := w.Header().Get(api.HeaderRouteSource); got != "pinned" {
			t.Errorf("%s = %q, want pinned (alias middleware owns it)", api.HeaderRouteSource, got)
		}
		if got := w.Header().Get(api.HeaderAgentSource); got != "miss" {
			t.Errorf("%s = %q, want miss", api.HeaderAgentSource, got)
		}
	})

	t.Run("set-if-unset never overwrites an existing value", func(t *testing.T) {
		w := servePassthrough(t, &domain.ChatResponse{
			RawBytes: []byte(`{"ok":true}`),
			HttpHeaders: http.Header{
				api.HeaderAgentSource: []string{"preset"},
			},
			AffinitySource: "hit", // must lose to the pre-existing header
		})
		if got := w.Header().Get(api.HeaderAgentSource); got != "preset" {
			t.Errorf("%s = %q, want preset (existing value wins)", api.HeaderAgentSource, got)
		}
	})

	t.Run("affinity off writes no header", func(t *testing.T) {
		w := servePassthrough(t, &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)})
		if got := w.Header().Get(api.HeaderAgentSource); got != "" {
			t.Errorf("%s = %q, want empty when affinity is off", api.HeaderAgentSource, got)
		}
	})

	t.Run("streaming carries the header", func(t *testing.T) {
		w := servePassthrough(t, &domain.ChatResponse{
			Body: io.NopCloser(strings.NewReader(
				"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")),
			AffinitySource: "ineligible",
		})
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		if got := w.Header().Get(api.HeaderAgentSource); got != "ineligible" {
			t.Errorf("%s = %q on a stream, want ineligible", api.HeaderAgentSource, got)
		}
	})
}

func TestAffinityHeader_EmbeddingWriter(t *testing.T) {
	w := serveEmbeddings(t, &domain.ChatResponse{
		RawBytes:       []byte(`{"data":[]}`),
		AffinitySource: "miss",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get(api.HeaderAgentSource); got != "miss" {
		t.Errorf("%s = %q, want miss", api.HeaderAgentSource, got)
	}

	// Off → no header on this writer either.
	w = serveEmbeddings(t, &domain.ChatResponse{RawBytes: []byte(`{"data":[]}`)})
	if got := w.Header().Get(api.HeaderAgentSource); got != "" {
		t.Errorf("%s = %q, want empty when affinity is off", api.HeaderAgentSource, got)
	}
}
