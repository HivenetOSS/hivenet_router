// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
			client := &http.Client{Transport: agent.BackendTransport(nil, tc.apiKey, srv.URL)}
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
			client := &http.Client{Transport: agent.BackendTransport(nil, tc.apiKey, srv.URL)}
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
			client := &http.Client{Transport: agent.BackendTransport(nil, key, url)}

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
			client := &http.Client{Transport: agent.BackendTransport(nil, "", url)}
			if _, err := tc.engine.DiscoverModels(context.Background(), url, client); err == nil {
				t.Error("DiscoverModels succeeded against a key-protected backend without the key")
			}
		})
	}
}

// credentialsSeen records the credentials of every request a stub receives.
type credentialsSeen struct {
	mu   sync.Mutex
	reqs []string // "Authorization|X-Api-Key"
}

func (c *credentialsSeen) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, strings.Join(r.Header.Values("Authorization"), ",")+"|"+r.Header.Get("X-Api-Key"))
}

func (c *credentialsSeen) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.reqs...)
}

// TestBackendTransport_RedirectToOtherHost: when the backend redirects to
// another host, neither the backend key nor the client's credentials follow.
// Both stubs listen on 127.0.0.1 with different ports, which Go's own
// redirect rule treats as the same host (it ignores ports), so this also
// proves the transport compares host and port.
func TestBackendTransport_RedirectToOtherHost(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		status     int
		clientAuth bool // the client sent Authorization and X-Api-Key
	}{
		// GET 302, as a health check or model discovery would follow.
		{"302 GET without client credentials", http.MethodGet, http.StatusFound, false},
		{"302 GET with client credentials", http.MethodGet, http.StatusFound, true},
		// 307 keeps the method and body, as a forwarded chat request would.
		{"307 POST with client credentials", http.MethodPost, http.StatusTemporaryRedirect, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var backendSaw, otherSaw credentialsSeen
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				otherSaw.record(r)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(other.Close)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backendSaw.record(r)
				http.Redirect(w, r, other.URL+"/elsewhere", tc.status)
			}))
			t.Cleanup(backend.Close)

			req, _ := http.NewRequest(tc.method, backend.URL+"/v1/models", strings.NewReader("{}"))
			if tc.clientAuth {
				req.Header.Set("Authorization", "Bearer sk-hivenet-client")
				req.Header.Set("X-Api-Key", "sk-hivenet-client")
			}
			client := &http.Client{Transport: agent.BackendTransport(nil, "sk-backend", backend.URL)}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()

			if got := backendSaw.all(); len(got) != 1 || got[0] != "Bearer sk-backend|" {
				t.Errorf("backend saw %q, want [\"Bearer sk-backend|\"]", got)
			}
			if got := otherSaw.all(); len(got) != 1 || got[0] != "|" {
				t.Errorf("redirect target saw %q, want no credentials", got)
			}
		})
	}
}

// TestBackendTransport_HostScope: which request URLs receive the backend key,
// given the allowed URLs (the backend URL and, for the custom engine, its
// health URL). A request that does not match has any client Authorization
// removed as well, and X-Api-Key is always removed.
func TestBackendTransport_HostScope(t *testing.T) {
	tests := []struct {
		name    string
		allowed []string
		reqURL  string
		wantKey bool
	}{
		// Exact origin, any path.
		{"backend origin", []string{"http://backend:8000"}, "http://backend:8000/v1/models", true},
		// Same host name, different port: another server.
		{"other port", []string{"http://backend:8000"}, "http://backend:9000/v1/models", false},
		{"other host", []string{"http://backend:8000"}, "http://evil:8000/v1/models", false},
		// A subdomain is another host (Go's redirect rule would allow it).
		{"subdomain", []string{"http://backend:8000"}, "http://x.backend:8000/v1/models", false},
		{"other scheme", []string{"http://backend:8000"}, "https://backend:8000/v1/models", false},
		// Default ports and host name case are canonicalized.
		{"implicit http port", []string{"http://backend"}, "http://backend:80/health", true},
		{"implicit https port", []string{"https://backend:443"}, "https://backend/health", true},
		{"host name case", []string{"http://Backend:8000"}, "http://backend:8000/health", true},
		{"ipv6", []string{"http://[::1]:8000"}, "http://[::1]:8000/health", true},
		// A custom engine's --health-url on another host is allowed too.
		{"health url host", []string{"http://backend:8000", "http://monitor:9100/healthz"}, "http://monitor:9100/healthz", true},
		// Entries that are not absolute URLs match nothing (the agent refuses
		// to start with them; here the key is simply never sent).
		{"relative allowed url", []string{"backend:8000"}, "http://backend:8000/health", false},
		{"no allowed urls", nil, "http://backend:8000/health", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var seen http.Header
			base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				seen = r.Header
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
			})
			u, err := url.Parse(tc.reqURL)
			if err != nil {
				t.Fatal(err)
			}
			req := &http.Request{Method: http.MethodGet, URL: u, Header: http.Header{
				"Authorization": {"Bearer sk-hivenet-client"},
				"X-Api-Key":     {"sk-hivenet-client"},
			}}
			resp, err := agent.BackendTransport(base, "sk-backend", tc.allowed...).RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			want := ""
			if tc.wantKey {
				want = "Bearer sk-backend"
			}
			if got := seen.Get("Authorization"); got != want {
				t.Errorf("Authorization = %q, want %q", got, want)
			}
			if x := seen.Get("X-Api-Key"); x != "" {
				t.Errorf("X-Api-Key = %q, want none", x)
			}
			if req.Header.Get("Authorization") != "Bearer sk-hivenet-client" {
				t.Error("the caller's request was modified")
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
