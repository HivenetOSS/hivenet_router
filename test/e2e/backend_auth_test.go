// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"bytes"
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

// keyedBackend mimics vLLM started with --api-key: /health is open and every
// /v1/* route answers 401 unless "Authorization: Bearer <backendKey>" is sent.
// It records the credentials of each inference request it accepts.
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
	record := func(r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		b.mu.Lock()
		b.seen = append(b.seen, r.URL.Path+" "+r.Header.Get("Authorization")+" "+r.Header.Get("X-Api-Key"))
		b.mu.Unlock()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", guard(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"object":"list","data":[{"id":%q,"object":"model"}]}`, stubModel)
	}))
	mux.HandleFunc("/v1/chat/completions", guard(func(w http.ResponseWriter, r *http.Request) {
		record(r)
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

// TestBackendAPIKeyEndToEnd: a real router and a real vLLM-engine agent with no
// --model, in front of a key-protected backend. The agent must discover its
// model with the backend key, and every inference request must reach the
// backend with the agent's key instead of the client's router credentials.
func TestBackendAPIKeyEndToEnd(t *testing.T) {
	be := &keyedBackend{}
	backend := be.start(t)
	base, rcfg := startRouter(t)

	acfg := config.DefaultAgentConfig()
	acfg.JWTSecret = jwtSecret
	acfg.Engine = "vllm" // Model left empty: the agent must discover it
	acfg.BackendURL = backend.URL
	acfg.BackendAPIKey = backendKey
	acfg.RouterGRPCAddr = "127.0.0.1" + rcfg.GRPCPort
	acfg.RouterP2PAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", rcfg.P2PPort)
	acfg.IdentityPath = filepath.Join(t.TempDir(), "agent.key")
	acfg.Capacity = 2
	a := agent.NewAgent(acfg, &agent.VLLMEngine{})
	go a.Run() //nolint:errcheck // exits via Stop; failures surface as the readiness timeout
	t.Cleanup(a.Stop)
	waitFor(t, 20*time.Second, "agent discovered its model and registered", func() bool { return modelHealthy(base) })

	for _, tc := range []struct{ name, path, body string }{
		{"chat completions", "/v1/chat/completions", `{"model":"stub-model","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`},
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
			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d: %s", resp.StatusCode, b)
			}
			// The backend saw the agent's key and neither client credential.
			if got, want := be.last(), tc.path+" Bearer "+backendKey+" "; got != want {
				t.Errorf("backend saw %q, want %q", got, want)
			}
		})
	}
}
