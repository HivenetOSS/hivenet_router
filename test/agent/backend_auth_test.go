// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/domain"
)

// headerEcho records the credentials of the last request it received.
type headerEcho struct{ auth, xAPIKey, traceparent string }

func (h *headerEcho) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.auth, h.xAPIKey, h.traceparent = r.Header.Get("Authorization"), r.Header.Get("X-Api-Key"), r.Header.Get("Traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestBackendTransport: the credentials a backend receives, with and without
// a configured backend API key. Other headers always pass through, and the
// caller's request is never modified (the router reuses it).
func TestBackendTransport(t *testing.T) {
	tests := []struct {
		name               string
		clientAuth, client string // Authorization / X-Api-Key sent by the client
		apiKey             string
		wantAuth, wantXKey string
	}{
		// No key configured: client credentials pass through untouched.
		{"no key forwards client credentials", "Bearer client", "client-x", "", "Bearer client", "client-x"},
		// Key configured: Authorization replaced, x-api-key dropped.
		{"key replaces client credentials", "Bearer client", "client-x", "sk-backend", "Bearer sk-backend", ""},
		// Key configured and the client sent no credentials (health, discovery).
		{"key added when client sent none", "", "", "sk-backend", "Bearer sk-backend", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got headerEcho
			srv := got.server(t)
			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			if tc.clientAuth != "" {
				req.Header.Set("Authorization", tc.clientAuth)
				req.Header.Set("X-Api-Key", tc.client)
			}
			req.Header.Set("Traceparent", "00-abc")
			client := &http.Client{Transport: agent.BackendTransport(nil, tc.apiKey)}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if got.auth != tc.wantAuth || got.xAPIKey != tc.wantXKey || got.traceparent != "00-abc" {
				t.Errorf("backend saw auth=%q x-api-key=%q traceparent=%q, want %q/%q/00-abc", got.auth, got.xAPIKey, got.traceparent, tc.wantAuth, tc.wantXKey)
			}
			if req.Header.Get("Authorization") != tc.clientAuth {
				t.Error("the caller's request was modified")
			}
		})
	}
}

// keyedVLLM mimics vLLM started with --api-key: /health is open, every /v1/*
// route requires "Authorization: Bearer <key>".
func keyedVLLM(t *testing.T, key string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, `{"error":"Unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"org/model-a","object":"model"}]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestBackendTransport_ModelDiscovery: an agent in front of a key-protected
// vLLM can discover its model only when the backend key is configured, since
// discovery runs before any client request exists.
func TestBackendTransport_ModelDiscovery(t *testing.T) {
	srv := keyedVLLM(t, "sk-backend")
	engine := &agent.VLLMEngine{}
	for _, tc := range []struct {
		name    string
		apiKey  string
		wantErr bool
	}{
		{"with backend key", "sk-backend", false},
		{"without backend key", "", true},
		{"wrong backend key", "sk-other", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: agent.BackendTransport(nil, tc.apiKey)}
			if err := engine.WaitForReady(context.Background(), srv.URL, client); err != nil {
				t.Fatalf("WaitForReady: %v", err)
			}
			models, err := engine.DiscoverModels(context.Background(), srv.URL, client)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DiscoverModels err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && (len(models) != 1 || models[0] != "org/model-a") {
				t.Errorf("models = %v", models)
			}
		})
	}
}

// keyedBackend mimics any engine started with an API key, strictly: every
// route answers 401 unless the request carries exactly one Authorization
// header equal to "Bearer <key>" and no X-Api-Key (real servers may leave
// /health or /metrics open; guarding them all proves the key is sent on every
// path). It serves the routes of every engine and records, per path, the
// Authorization values and X-Api-Key of each request.
type keyedBackend struct {
	key  string
	mu   sync.Mutex
	seen map[string][][]string // path → Authorization values per request
	xKey []string              // X-Api-Key values seen (should stay empty)
}

func (b *keyedBackend) start(t *testing.T) string {
	t.Helper()
	b.seen = map[string][][]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		auth := r.Header.Values("Authorization")
		b.mu.Lock()
		b.seen[r.URL.Path] = append(b.seen[r.URL.Path], auth)
		if x := r.Header.Get("X-Api-Key"); x != "" {
			b.xKey = append(b.xKey, x)
		}
		b.mu.Unlock()
		if len(auth) != 1 || auth[0] != "Bearer "+b.key || r.Header.Get("X-Api-Key") != "" {
			http.Error(w, `{"error":"Unauthorized"}`, http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/v1/models":
			_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"org/model-a","object":"model"}]}`)
		case "/api/tags":
			_, _ = io.WriteString(w, `{"models":[{"name":"org/model-a:latest"}]}`)
		case "/v1/chat/completions":
			_, _ = io.WriteString(w, `{"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"PONG"}}]}`)
		case "/metrics":
			_, _ = io.WriteString(w, "vllm:num_requests_running 1\nsglang:num_running_reqs 1\nllamacpp:requests_processing 1\n")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestBackendKey_Engines: for every engine that talks to an OpenAI-compatible
// backend, a configured backend key reaches the backend exactly once on every
// call the agent makes (readiness, model discovery, chat forwarding and, where
// the engine scrapes them, metrics) and replaces the client's credentials.
// For Ollama this also covers its "Bearer ollama" default: ForwardChat gets a
// request with empty HttpHeaders, the case that default targets, and the
// backend still sees only the configured key. Without the key every call is
// rejected, so the test would catch a path that bypasses the transport.
// Streaming is covered end to end in test/e2e.
func TestBackendKey_Engines(t *testing.T) {
	const key = "sk-backend"
	engines := []struct {
		engine    agent.Engine
		readyPath string
		modelPath string
	}{
		{&agent.VLLMEngine{}, "/health", "/v1/models"},
		{&agent.SGLangEngine{}, "/health", "/v1/models"},
		{&agent.LlamaCPPEngine{}, "/health", "/v1/models"},
		{&agent.OllamaEngine{}, "/api/tags", "/api/tags"},
	}
	chat := domain.ChatRequest{RawBytes: []byte(`{"model":"org/model-a","messages":[{"role":"user","content":"hi"}]}`)}
	for _, tc := range engines {
		t.Run(tc.engine.Name(), func(t *testing.T) {
			ctx := context.Background()
			be := &keyedBackend{key: key}
			url := be.start(t)
			client := &http.Client{Transport: agent.BackendTransport(nil, key)}

			if err := tc.engine.WaitForReady(ctx, url, client); err != nil {
				t.Fatalf("WaitForReady: %v", err)
			}
			if models, err := tc.engine.DiscoverModels(ctx, url, client); err != nil || len(models) != 1 || models[0] != "org/model-a" {
				t.Fatalf("DiscoverModels = %v, %v", models, err)
			}
			// The client sent no credentials; HttpHeaders is set (and empty), as
			// Ollama's default expects.
			noAuth := chat
			noAuth.HttpHeaders = http.Header{}
			if _, _, err := tc.engine.ForwardChat(ctx, url, client, "org/model-a", noAuth, agent.WithHttpHeader(http.Header{})); err != nil {
				t.Fatalf("ForwardChat without client credentials: %v", err)
			}
			// The client sent its router key in both headers.
			clientHdr := http.Header{"Authorization": {"Bearer sk-hivenet-client"}, "X-Api-Key": {"sk-hivenet-client"}}
			if _, _, err := tc.engine.ForwardChat(ctx, url, client, "org/model-a", chat, agent.WithHttpHeader(clientHdr)); err != nil {
				t.Fatalf("ForwardChat with client credentials: %v", err)
			}
			paths := []string{tc.readyPath, tc.modelPath, "/v1/chat/completions"}
			if mp, ok := tc.engine.(agent.MetricsProvider); ok {
				if _, err := mp.ScrapeMetrics(ctx, url, client); err != nil {
					t.Fatalf("ScrapeMetrics: %v", err)
				}
				paths = append(paths, "/metrics")
			}

			be.mu.Lock()
			defer be.mu.Unlock()
			for _, p := range paths {
				if len(be.seen[p]) == 0 {
					t.Errorf("%s: no request reached the backend", p)
				}
				for _, auth := range be.seen[p] {
					if len(auth) != 1 || auth[0] != "Bearer "+key {
						t.Errorf("%s: Authorization = %q, want exactly [\"Bearer %s\"]", p, auth, key)
					}
				}
			}
			if len(be.xKey) != 0 {
				t.Errorf("X-Api-Key reached the backend: %q", be.xKey)
			}
		})
		t.Run(tc.engine.Name()+"/without key", func(t *testing.T) {
			be := &keyedBackend{key: key}
			url := be.start(t)
			client := &http.Client{Transport: agent.BackendTransport(nil, "")}
			if _, err := tc.engine.DiscoverModels(context.Background(), url, client); err == nil {
				t.Error("DiscoverModels succeeded against a key-protected backend without the key")
			}
		})
	}
}
