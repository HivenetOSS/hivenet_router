// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package e2e wires a real router and a real agent together in one process
// and drives inference through the router's public OpenAI-compatible API.
//
// The model backend is a stub HTTP server — the point is not inference
// quality but the plumbing between the components: gRPC auth, libp2p
// registration, request forwarding, streaming, and reconnection.
//
// These tests lock in the single-connection architecture: the agent
// registers WITHOUT advertising any dial address, and the router forwards
// inference back over the libp2p connection the agent itself opened. If a
// change reintroduces a router→agent dial requirement, the happy-path test
// fails because the agent never becomes reachable.
package e2e

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

// jwtSecret is shared by router and agent; must be ≥32 bytes (HS256 minimum).
const jwtSecret = "e2e-integration-secret-0123456789abcdef"

const stubModel = "stub-model"

// freePort asks the kernel for an unused TCP port on 127.0.0.1.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startStubBackend serves an OpenAI-compatible chat endpoint. Non-streaming
// requests get a fixed "PONG" completion; streaming requests get SSE chunks
// followed by [DONE].
func startStubBackend(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(body, &req)

		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			// Flush after every chunk so the client receives incremental SSE
			// frames like a real backend would, not one buffered burst.
			fl, _ := w.(http.Flusher)
			for _, tok := range []string{"P", "O", "N", "G"} {
				fmt.Fprintf(w,
					`data: {"id":"chatcmpl-stub","object":"chat.completion.chunk","created":1,"model":%q,"choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`+"\n\n",
					stubModel, tok)
				if fl != nil {
					fl.Flush()
				}
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			if fl != nil {
				fl.Flush()
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w,
			`{"id":"chatcmpl-stub","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
			stubModel)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// routerOpts customises startRouterWith. The zero value is the default
// no-auth stack (insecure admin allowed) every older test uses.
type routerOpts struct {
	// AuthYAML, when set, is written to a temp auth.yaml and loaded. Use
	// writeAuthYAML to build one with real key hashes.
	AuthYAML string
	// AdminKey, when set, is the raw admin key (HIVENET_ROUTER_ADMIN_API_KEYS)
	// and the insecure-admin opt-in is not set.
	AdminKey string
	// Mutate, when set, edits the config after the defaults are applied.
	Mutate func(*config.Config)
}

// startRouter builds and starts a router on ephemeral ports and returns its
// HTTP base URL plus the config (for the agent to derive addresses from).
func startRouter(t *testing.T) (baseURL string, cfg *config.Config) {
	t.Helper()
	return startRouterWith(t, routerOpts{})
}

// startRouterWith is startRouter with auth and config options.
func startRouterWith(t *testing.T, opts routerOpts) (baseURL string, cfg *config.Config) {
	t.Helper()
	baseURL, cfg, _ = startRouterHandle(t, opts)
	return baseURL, cfg
}

// routerConfig builds the config startRouterWith uses, without starting a
// router: ephemeral ports, a temp Badger dir, and the auth options.
func routerConfig(t *testing.T, opts routerOpts) *config.Config {
	t.Helper()
	if opts.AdminKey != "" {
		t.Setenv("HIVENET_ROUTER_ADMIN_API_KEYS", opts.AdminKey)
	} else {
		// These tests exercise the inference path, not admin auth, so allow
		// the default no-auth admin surface instead of configuring admin keys.
		t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")
	}
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.P2PListenAddr = "127.0.0.1"
	cfg.JWTSecret = jwtSecret
	cfg.DiskDBPath = filepath.Join(t.TempDir(), "badger")
	if opts.AuthYAML != "" {
		p := filepath.Join(t.TempDir(), "auth.yaml")
		if err := os.WriteFile(p, []byte(opts.AuthYAML), 0o600); err != nil {
			t.Fatalf("write auth.yaml: %v", err)
		}
		cfg.AuthConfigFile = p
	}
	if opts.Mutate != nil {
		opts.Mutate(cfg)
	}
	return cfg
}

// startRouterHandle is startRouterWith that also returns the router.
func startRouterHandle(t *testing.T, opts routerOpts) (baseURL string, cfg *config.Config, r *router.Router) {
	t.Helper()
	// freePort releases its port before the router binds it, so another
	// test process can take it in between. A router whose listener lost that
	// race logs the error and never becomes ready (or another process answers
	// on its port), so retry with fresh ports; a router that never starts
	// still fails after the last attempt.
	const attempts = 3
	for attempt := 1; ; attempt++ {
		cfg = routerConfig(t, opts)
		r, err := router.New(cfg)
		if err != nil {
			if attempt < attempts && strings.Contains(err.Error(), "address already in use") {
				continue
			}
			t.Fatalf("router.New: %v", err)
		}
		started := make(chan error, 1)
		go func() { started <- r.Start() }() // blocks until Close
		baseURL = "http://127.0.0.1" + cfg.HTTPPort
		// /health is public, so readiness does not depend on the auth mode.
		startErr, ready := error(nil), false
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			select {
			case startErr = <-started:
			default:
			}
			if startErr != nil {
				break
			}
			if resp, err := http.Get(baseURL + "/health"); err == nil {
				resp.Body.Close()
				if ready = resp.StatusCode == http.StatusOK; ready {
					break
				}
			}
		}
		if ready {
			t.Cleanup(func() { r.Close() }) //nolint:errcheck
			return baseURL, cfg, r
		}
		r.Close() //nolint:errcheck
		if attempt < attempts {
			t.Logf("router not ready on attempt %d (start error: %v); retrying on fresh ports", attempt, startErr)
			continue
		}
		t.Fatalf("router HTTP API not ready after %d attempts (last start error: %v)", attempts, startErr)
	}
}

// testKey is one static auth.yaml key: the raw token a client presents, the
// tenant (metadata.owner) it belongs to, and optional flat quota limits
// (0 = unlimited).
type testKey struct {
	Token, Owner, Name string
	RPM, TPD           int
}

// writeAuthYAML returns an api-key auth.yaml for the given keys, hashing each
// token the way the router does (hex SHA-256). Admin auth is api-key, so pair
// it with routerOpts.AdminKey.
func writeAuthYAML(keys ...testKey) string {
	var b strings.Builder
	b.WriteString("api:\n  mode: api-key\n  keys:\n")
	for _, k := range keys {
		h := sha256.Sum256([]byte(k.Token))
		fmt.Fprintf(&b, "    - key_hash: %q\n      metadata:\n        name: %q\n        owner: %q\n",
			hex.EncodeToString(h[:]), k.Name, k.Owner)
		if k.RPM > 0 || k.TPD > 0 {
			fmt.Fprintf(&b, "      quota:\n        requests_per_minute: %d\n        tokens_per_day: %d\n", k.RPM, k.TPD)
		}
	}
	b.WriteString("admin:\n  mode: api-key\n")
	return b.String()
}

// startAgent builds and runs an agent against the given router and stub
// backend. Note: no announce address, no listen port, no address config of
// any kind — agents are outbound-only.
func startAgent(t *testing.T, routerCfg *config.Config, backendURL, identityPath string) *agent.Agent {
	t.Helper()
	return startAgentWith(t, routerCfg, backendURL, identityPath, nil)
}

// startAgentWith is startAgent with a hook to edit the agent config.
func startAgentWith(t *testing.T, routerCfg *config.Config, backendURL, identityPath string, mutate func(*config.AgentConfig)) *agent.Agent {
	t.Helper()
	return startAgentWithEngine(t, routerCfg, backendURL, identityPath, nil, mutate)
}

// startAgentWithEngine is startAgentWith for a given engine (nil = the
// custom engine on backendURL/health).
func startAgentWithEngine(t *testing.T, routerCfg *config.Config, backendURL, identityPath string, engine agent.Engine, mutate func(*config.AgentConfig)) *agent.Agent {
	t.Helper()
	cfg := config.DefaultAgentConfig()
	cfg.JWTSecret = jwtSecret
	cfg.Model = stubModel
	cfg.Engine = "custom"
	cfg.BackendURL = backendURL
	cfg.HealthURL = backendURL + "/health"
	cfg.RouterGRPCAddr = "127.0.0.1" + routerCfg.GRPCPort
	cfg.RouterP2PAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", routerCfg.P2PPort)
	cfg.IdentityPath = identityPath
	cfg.Capacity = 2
	cfg.Region = "e2e"
	if engine == nil {
		engine = &agent.CustomEngine{HealthURL: cfg.HealthURL}
	}
	cfg.Engine = engine.Name()
	if mutate != nil {
		mutate(cfg)
	}

	a := agent.NewAgent(cfg, engine)
	go a.Run() //nolint:errcheck // exits via Stop; failures surface as readiness timeout in callers
	t.Cleanup(a.Stop)
	return a
}

// waitFor polls cond until it returns true or the deadline expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func postChat(baseURL string, stream bool) (*http.Response, error) {
	payload := fmt.Sprintf(
		`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":10,"stream":%t}`,
		stubModel, stream)
	return http.Post(baseURL+"/v1/chat/completions", "application/json",
		bytes.NewReader([]byte(payload)))
}

// modelHealthy reports whether /v1/models lists stubModel with ≥1 healthy agent.
func modelHealthy(baseURL string) bool {
	return modelHealthyAs(baseURL, "", stubModel)
}

// modelHealthyAs is modelHealthy for any model, presenting the given API key
// ("" sends none, for the no-auth stack).
func modelHealthyAs(baseURL, apiKey, model string) bool {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/models", nil)
	if err != nil {
		return false
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct {
			ID     string `json:"id"`
			Agents struct {
				Healthy int `json:"healthy"`
			} `json:"agents"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false
	}
	for _, m := range out.Data {
		if m.ID == model && m.Agents.Healthy >= 1 {
			return true
		}
	}
	return false
}

// TestRouterAgentEndToEnd runs one router+agent stack and drives it through
// the full lifecycle in ordered subtests (they share state deliberately:
// each stage depends on the previous one, like a real deployment).
func TestRouterAgentEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}

	baseURL, routerCfg := startRouter(t)
	backend := startStubBackend(t)
	identityPath := filepath.Join(t.TempDir(), "agent_identity.key")

	a := startAgent(t, routerCfg, backend.URL, identityPath)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthy(baseURL)
	})

	t.Run("completion", func(t *testing.T) {
		resp, err := postChat(baseURL, false)
		if err != nil {
			t.Fatalf("POST /v1/chat/completions: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", resp.StatusCode, body)
		}
		var out struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode response: %v; body: %s", err, body)
		}
		if len(out.Choices) == 0 || out.Choices[0].Message.Content != "PONG" {
			t.Fatalf("unexpected completion body: %s", body)
		}
	})

	t.Run("streaming", func(t *testing.T) {
		resp, err := postChat(baseURL, true)
		if err != nil {
			t.Fatalf("POST stream: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("status = %d, want 200; body: %s", resp.StatusCode, body)
		}
		var dataLines int
		var sawDone bool
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			if strings.TrimPrefix(line, "data: ") == "[DONE]" {
				sawDone = true
				break
			}
			dataLines++
		}
		if dataLines == 0 {
			t.Fatal("no SSE data chunks received")
		}
		if !sawDone {
			t.Fatal("stream ended without [DONE]")
		}
	})

	// The recovery contract of the single-connection architecture: when the
	// agent dies, requests fail (the router has no address to dial — recovery
	// is agent-initiated only), and a restarted agent re-registers over a
	// fresh outbound connection and serves again.
	t.Run("agent death and recovery", func(t *testing.T) {
		a.Stop()
		waitFor(t, 15*time.Second, "requests to fail after agent stop", func() bool {
			resp, err := postChat(baseURL, false)
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			return resp.StatusCode >= http.StatusInternalServerError
		})

		// Same identity → same peer ID → exercises the re-registration path.
		startAgent(t, routerCfg, backend.URL, identityPath)
		waitFor(t, 30*time.Second, "restarted agent to serve again", func() bool {
			resp, err := postChat(baseURL, false)
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			return resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte("PONG"))
		})
	})
}
