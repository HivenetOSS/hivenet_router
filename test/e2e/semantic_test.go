// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
	"hivenet_router/internal/semantic"
)

const semanticAliasDoc = `
models: ["auto"]
alias:
  default_route: general
  routes:
    - name: software
      signals: [ { feature: stack_trace, weight: 1 } ]
      candidates: [ { model: stub-model } ]
    - name: general
      candidates: [ { model: stub-model } ]
`

// backendCall is what the recording backend saw for one request.
type backendCall struct {
	path string
	body []byte
}

// recordingBackend answers chat completions and Anthropic messages, recording
// each request's path and body.
type recordingBackend struct {
	mu    sync.Mutex
	calls []backendCall
}

func (b *recordingBackend) last() backendCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.calls) == 0 {
		return backendCall{}
	}
	return b.calls[len(b.calls)-1]
}

func (b *recordingBackend) start(t *testing.T) *httptest.Server {
	t.Helper()
	record := func(r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		b.mu.Lock()
		b.calls = append(b.calls, backendCall{path: r.URL.Path, body: body})
		b.mu.Unlock()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"x","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	})
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"m","type":"message","role":"assistant","model":%q,"content":[{"type":"text","text":"PONG"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`, stubModel)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestSemanticAliasEndToEnd: a real router with an "auto" alias and a real
// agent, against a recording backend.
func TestSemanticAliasEndToEnd(t *testing.T) {
	be := &recordingBackend{}
	backend := be.start(t)

	dir := t.TempDir()
	policyDir := filepath.Join(dir, "policies")
	if err := os.MkdirAll(policyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(policyDir, "auto.yaml"), []byte(semanticAliasDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	decisions := filepath.Join(dir, "decisions.jsonl")

	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.P2PListenAddr = "127.0.0.1"
	cfg.JWTSecret = jwtSecret
	cfg.DiskDBPath = filepath.Join(dir, "badger")
	cfg.PolicyModelDir = policyDir
	cfg.SemanticDecisionLog = decisions
	r, err := router.New(cfg)
	if err != nil {
		t.Fatalf("router.New: %v", err)
	}
	go r.Start() //nolint:errcheck
	closed := false
	closeRouter := func() {
		if !closed {
			closed = true
			r.Close() //nolint:errcheck
		}
	}
	t.Cleanup(closeRouter)
	base := "http://127.0.0.1" + cfg.HTTPPort

	acfg := config.DefaultAgentConfig()
	acfg.JWTSecret = jwtSecret
	acfg.Model = stubModel
	acfg.Engine = "custom"
	acfg.BackendURL = backend.URL
	acfg.HealthURL = backend.URL + "/health"
	acfg.RouterGRPCAddr = "127.0.0.1" + cfg.GRPCPort
	acfg.RouterP2PAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", cfg.P2PPort)
	acfg.IdentityPath = filepath.Join(dir, "agent.key")
	acfg.Capacity = 2
	a := agent.NewAgent(acfg, &agent.CustomEngine{HealthURL: acfg.HealthURL})
	go a.Run() //nolint:errcheck
	t.Cleanup(a.Stop)
	waitFor(t, 20*time.Second, "agent healthy", func() bool { return modelHealthy(base) })

	send := func(path, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	t.Run("alias resolved and model rewritten", func(t *testing.T) {
		body := `{"model":"auto","messages":[{"role":"user","content":"Traceback (most recent call last):"}],"max_tokens":5}`
		resp := send("/v1/chat/completions", body)
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status %d: %s", resp.StatusCode, b)
		}
		if got := resp.Header.Get("X-Hivenet-Routed-Model"); got != stubModel {
			t.Errorf("routed-model header = %q", got)
		}
		if got := resp.Header.Get("X-Hivenet-Route"); got != "software" {
			t.Errorf("route header = %q", got)
		}
		call := be.last()
		if want := strings.Replace(body, `"auto"`, `"`+stubModel+`"`, 1); string(call.body) != want {
			t.Errorf("backend body:\n got %s\nwant %s", call.body, want)
		}
	})

	t.Run("concrete model body forwarded byte-identical", func(t *testing.T) {
		body := `{"model":"stub-model",  "messages":[{"role":"user","content":"hi"}],"max_tokens":5}`
		resp := send("/v1/chat/completions", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if resp.Header.Get("X-Hivenet-Routed-Model") != "" {
			t.Error("concrete model must not get routing headers")
		}
		if call := be.last(); string(call.body) != body {
			t.Errorf("backend body changed:\n got %s\nwant %s", call.body, body)
		}
	})

	t.Run("anthropic messages via proxy path", func(t *testing.T) {
		resp := send("/v1/messages", `{"model":"auto","max_tokens":5,"messages":[{"role":"user","content":"hello"}]}`)
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status %d: %s", resp.StatusCode, b)
		}
		call := be.last()
		if call.path != "/v1/messages" {
			t.Errorf("backend call path=%s", call.path)
		}
		if !strings.Contains(string(call.body), `"model":"stub-model"`) {
			t.Errorf("backend body %s", call.body)
		}
	})

	t.Run("decision log records the decisions", func(t *testing.T) {
		closeRouter() // flushes the decision log
		f, err := os.Open(decisions)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var recs []semantic.DecisionRecord
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var rec semantic.DecisionRecord
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				t.Fatalf("bad line: %v", err)
			}
			recs = append(recs, rec)
		}
		if len(recs) != 2 {
			t.Fatalf("got %d decision records, want 2 (alias requests only)", len(recs))
		}
		if recs[0].Route != "software" || recs[0].ResolvedModel != stubModel || recs[0].Features["stack_trace"] != 1 {
			t.Errorf("first record = %+v", recs[0])
		}
	})
}
