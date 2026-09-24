// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"hivenet_router/internal/agent"
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
