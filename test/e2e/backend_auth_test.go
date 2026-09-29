// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
)

const backendKey = "sk-backend-secret"

// keyedBackend mimics an engine started with --api-key: /health is open and
// every other route (/v1/*, Ollama's /api/tags) answers 401 unless
// "Authorization: Bearer <backendKey>" is sent. It serves streaming and
// non-streaming chat, and records the credentials of each inference request
// it accepts.
type keyedBackend struct {
	mu   sync.Mutex
	seen []string // "path auth x-api-key"
}

func (b *keyedBackend) start(t *testing.T) *httptest.Server {
	t.Helper()
	guard := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+backendKey {
				http.Error(w, `{"error":"Unauthorized"}`, http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
	record := func(r *http.Request) []byte {
		body, _ := io.ReadAll(r.Body)
		b.mu.Lock()
		b.seen = append(b.seen, r.URL.Path+" "+r.Header.Get("Authorization")+" "+r.Header.Get("X-Api-Key"))
		b.mu.Unlock()
		return body
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", guard(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"object":"list","data":[{"id":%q,"object":"model"}]}`, stubModel)
	}))
	mux.HandleFunc("/api/tags", guard(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"models":[{"name":%q}]}`, stubModel)
	}))
	mux.HandleFunc("/v1/chat/completions", guard(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(record(r), &req)
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":%q,"choices":[{"index":0,"delta":{"content":"PONG"},"finish_reason":null}]}`+"\n\n", stubModel)
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"x","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	}))
	mux.HandleFunc("/v1/messages", guard(func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"m","type":"message","role":"assistant","model":%q,"content":[{"type":"text","text":"PONG"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`, stubModel)
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func (b *keyedBackend) last() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.seen) == 0 {
		return ""
	}
	return b.seen[len(b.seen)-1]
}

// TestBackendAPIKeyEndToEnd: a real router and a real agent with no --model,
// in front of a key-protected backend, for every engine that serves chat
// through an OpenAI-compatible API. The agent must pass readiness and discover
// its model with the backend key, and every inference request (chat, streaming
// chat, and /v1/messages through the generic proxy path) must reach the
// backend with the agent's key instead of the client's router credentials.
func TestBackendAPIKeyEndToEnd(t *testing.T) {
	for _, engine := range []agent.Engine{&agent.VLLMEngine{}, &agent.SGLangEngine{}, &agent.LlamaCPPEngine{}, &agent.OllamaEngine{}} {
		t.Run(engine.Name(), func(t *testing.T) {
			be := &keyedBackend{}
			backend := be.start(t)
			base, rcfg := startRouter(t)

			acfg := config.DefaultAgentConfig()
			acfg.JWTSecret = jwtSecret
			acfg.Engine = engine.Name() // Model left empty: the agent must discover it
			acfg.BackendURL = backend.URL
			acfg.BackendAPIKey = backendKey
			acfg.RouterGRPCAddr = "127.0.0.1" + rcfg.GRPCPort
			acfg.RouterP2PAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", rcfg.P2PPort)
			acfg.IdentityPath = filepath.Join(t.TempDir(), "agent.key")
			acfg.Capacity = 2
			a := agent.NewAgent(acfg, engine)
			go a.Run() //nolint:errcheck // exits via Stop; failures surface as the readiness timeout
			t.Cleanup(a.Stop)
			waitFor(t, 20*time.Second, "agent discovered its model and registered", func() bool { return modelHealthy(base) })

			for _, tc := range []struct{ name, path, body string }{
				{"chat completions", "/v1/chat/completions", `{"model":"stub-model","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`},
				{"streaming chat completions", "/v1/chat/completions", `{"model":"stub-model","messages":[{"role":"user","content":"hi"}],"max_tokens":5,"stream":true}`},
				{"anthropic messages via proxy path", "/v1/messages", `{"model":"stub-model","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req, _ := http.NewRequest(http.MethodPost, base+tc.path, bytes.NewReader([]byte(tc.body)))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer client-router-key")
					req.Header.Set("X-Api-Key", "client-x")
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					b, _ := io.ReadAll(resp.Body)
					if resp.StatusCode != http.StatusOK || !bytes.Contains(b, []byte("PONG")) {
						t.Fatalf("status %d: %s", resp.StatusCode, b)
					}
					// The backend saw the agent's key and neither client credential.
					if got, want := be.last(), tc.path+" Bearer "+backendKey+" "; got != want {
						t.Errorf("backend saw %q, want %q", got, want)
					}
				})
			}
		})
	}
}

// TestBackendAPIKeyEndToEnd_CustomHealthURLOnOtherHost: a custom engine whose
// --health-url is on a different host than --backend-url. Both hosts require
// the backend key, so the agent registers only if NewAgent allows the health
// URL's host, and chat still reaches the backend with the key.
func TestBackendAPIKeyEndToEnd_CustomHealthURLOnOtherHost(t *testing.T) {
	be := &keyedBackend{}
	backend := be.start(t)
	var healthAuth sync.Map // Authorization values seen by the health host
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthAuth.Store(r.Header.Get("Authorization"), true)
		if r.Header.Get("Authorization") != "Bearer "+backendKey {
			http.Error(w, `{"error":"Unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(health.Close)
	base, rcfg := startRouter(t)

	acfg := config.DefaultAgentConfig()
	acfg.JWTSecret = jwtSecret
	acfg.Engine = "custom"
	acfg.Model = stubModel
	acfg.BackendURL = backend.URL
	acfg.HealthURL = health.URL + "/healthz"
	acfg.BackendAPIKey = backendKey
	acfg.RouterGRPCAddr = "127.0.0.1" + rcfg.GRPCPort
	acfg.RouterP2PAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", rcfg.P2PPort)
	acfg.IdentityPath = filepath.Join(t.TempDir(), "agent.key")
	acfg.Capacity = 2
	a := agent.NewAgent(acfg, &agent.CustomEngine{HealthURL: acfg.HealthURL})
	go a.Run() //nolint:errcheck // exits via Stop; failures surface as the readiness timeout
	t.Cleanup(a.Stop)
	waitFor(t, 20*time.Second, "agent passed the keyed health check and registered", func() bool { return modelHealthy(base) })
	if _, ok := healthAuth.Load("Bearer " + backendKey); !ok {
		t.Fatal("the health host never received the backend key")
	}

	req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"stub-model","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer client-router-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if b, _ := io.ReadAll(resp.Body); resp.StatusCode != http.StatusOK || !bytes.Contains(b, []byte("PONG")) {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	if got, want := be.last(), "/v1/chat/completions Bearer "+backendKey+" "; got != want {
		t.Errorf("backend saw %q, want %q", got, want)
	}
}
