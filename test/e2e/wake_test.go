// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"bytes"
	"fmt"
	"io"
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

// TestHoldAtZeroEndToEnd runs the real router with no agent for a model that
// the sibling map places on this site with scale_from_zero. A streaming
// request is held and counted in hivenet_pending_demand, receives keepalive
// comments, and is served as soon as an agent for the model registers.
func TestHoldAtZeroEndToEnd(t *testing.T) {
	backend := startStubBackend(t)

	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.P2PListenAddr = "127.0.0.1"
	cfg.JWTSecret = jwtSecret
	cfg.DiskDBPath = filepath.Join(t.TempDir(), "badger")
	cfg.SiteID = "site-a"
	cfg.RedirectSecrets = "k1:0123456789abcdef0123456789abcdef"
	cfg.RedirectAllowedDomains = ".example.com"
	cfg.WakeKeepAlive = 200 * time.Millisecond
	r, err := router.New(cfg)
	if err != nil {
		t.Fatalf("router.New: %v", err)
	}
	go r.Start()                    //nolint:errcheck
	t.Cleanup(func() { r.Close() }) //nolint:errcheck
	base := "http://127.0.0.1" + cfg.HTTPPort
	metricsURL := "http://127.0.0.1" + cfg.MetricsPort + "/metrics"
	waitFor(t, 15*time.Second, "router HTTP API ready", func() bool {
		resp, err := http.Get(base + "/v1/models")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	siblingMap := fmt.Sprintf(`{"schema":3,"version":1,"region":"eu",
		"sites":[{"id":"site-a","url":"https://site-a.example.com","country":"FR","eu":true}],
		"placements":[{"model":%q,"kind":"chat","site":"site-a","scale_from_zero":true,"hold_budget":"60s"}]}`, stubModel)
	resp, err := http.Post(base+"/admin/sibling-map", "application/json", strings.NewReader(siblingMap))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sibling map push: status %d", resp.StatusCode)
	}

	demand := fmt.Sprintf(`hivenet_pending_demand{model=%q} `, stubModel)
	waitFor(t, 5*time.Second, "pending demand 0 before any request", func() bool {
		return metricLine(metricsURL, demand) == demand+"0"
	})

	type result struct {
		status int
		ct     string
		body   string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		body := fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}`, stubModel)
		resp, err := http.Post(base+"/v1/chat/completions", "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		done <- result{resp.StatusCode, resp.Header.Get("Content-Type"), string(b), err}
	}()

	waitFor(t, 5*time.Second, "the request held and counted", func() bool {
		return metricLine(metricsURL, demand) == demand+"1"
	})
	time.Sleep(500 * time.Millisecond) // a few keepalives while the model is at zero

	a := startAgent(t, cfg, backend.URL, filepath.Join(t.TempDir(), "agent.key"))

	var res result
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the held request was not served after the agent registered")
	}
	if res.err != nil {
		t.Fatal(res.err)
	}
	if res.status != http.StatusOK || res.ct != "text/event-stream" {
		t.Fatalf("status %d content type %q, want 200 text/event-stream", res.status, res.ct)
	}
	if !strings.HasPrefix(res.body, ": keep-alive\n\n") || !strings.Contains(res.body, `"content":"P"`) || !strings.HasSuffix(res.body, "data: [DONE]\n\n") {
		t.Fatalf("body %q, want keepalive comments, then the whole stream", res.body)
	}
	if got := metricLine(metricsURL, demand); got != demand+"0" {
		t.Errorf("after the release: %q, want %s0", got, demand)
	}
	woken := fmt.Sprintf(`hivenet_wake_holds_total{model=%q,outcome="woken"} `, stubModel)
	if got := metricLine(metricsURL, woken); got != woken+"1" {
		t.Errorf("holds: %q, want %s1", got, woken)
	}

	// Scaled back to zero: the agent goes away. A request right after must be
	// held again, not routed to the agent that left, even before its
	// missed-heartbeat timeout.
	a.Stop()
	waitFor(t, 5*time.Second, "the agent out of the routing pool", func() bool { return !modelHealthy(base) })
	again := make(chan result, 1)
	go func() {
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}`, stubModel)
		resp, err := http.Post(base+"/v1/chat/completions", "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			again <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		again <- result{resp.StatusCode, resp.Header.Get("Content-Type"), string(b), err}
	}()
	waitFor(t, 5*time.Second, "the request after the scale-down held", func() bool {
		return metricLine(metricsURL, demand) == demand+"1"
	})
	startAgent(t, cfg, backend.URL, filepath.Join(t.TempDir(), "agent2.key"))
	select {
	case res = <-again:
	case <-time.After(30 * time.Second):
		t.Fatal("the request held after the scale-down was not served")
	}
	if res.err != nil || res.status != http.StatusOK || !strings.Contains(res.body, "PONG") {
		t.Fatalf("after the second wake: %d %q %v, want 200 with the answer", res.status, res.body, res.err)
	}
}

// metricLine returns the exposition line starting with prefix, or "".
func metricLine(url, prefix string) string {
	resp, err := http.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	return ""
}

// TestHoldAtZeroBurstLinesUp wakes a model with more held requests than the
// woken capacity: an agent with one slot, a capacity queue of depth 1 and a
// backend that takes 300 ms per request. The released requests must line up
// for the slot and succeed, not be refused by the admission gate (429) or the
// full queue (503). A request still waiting for room when its hold budget
// ends gets model_waking, as a hold that expires does.
func TestHoldAtZeroBurstLinesUp(t *testing.T) {
	cases := []struct {
		name       string
		policy     string // global policy file
		holdBudget string
		n          int
		want       map[string]int // outcome → count
	}{
		{"admission limit max_inflight 1", "routing_policy:\n  strategy: least-loaded\nmax_inflight: 1\n", "60s", 6, map[string]int{"200": 6}},
		{"no admission limit, queue depth 1", "routing_policy:\n  strategy: least-loaded\n", "60s", 6, map[string]int{"200": 6}},
		// 300 ms per request and a 1 s budget: the first ~3 fit, the rest
		// are still waiting for room when the budget ends.
		{"hold budget ends while waiting", "routing_policy:\n  strategy: least-loaded\nmax_inflight: 1\n", "1s", 8, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := burst(t, tc.policy, tc.holdBudget, tc.n)
			if tc.want != nil {
				for k, v := range tc.want {
					if got[k] != v {
						t.Fatalf("outcomes %v, want %v", got, tc.want)
					}
				}
				return
			}
			if got["200"] == 0 || got["503 model_waking"] == 0 || got["200"]+got["503 model_waking"] != tc.n {
				t.Fatalf("outcomes %v, want some 200 and the rest 503 model_waking, never 429", got)
			}
		})
	}
}

// burst holds n requests for a model at zero, then starts an agent with one
// slot behind a 300 ms backend, and returns how the requests ended.
func burst(t *testing.T, policy, holdBudget string, n int) map[string]int {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"x","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	})
	backend := httptest.NewServer(mux)
	t.Cleanup(backend.Close)

	dir := t.TempDir()
	policyFile := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policyFile, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.P2PListenAddr = "127.0.0.1"
	cfg.JWTSecret = jwtSecret
	cfg.DiskDBPath = filepath.Join(dir, "badger")
	cfg.PolicyFile = policyFile
	cfg.QueueDepth = 1
	cfg.SiteID = "site-a"
	cfg.RedirectSecrets = "k1:0123456789abcdef0123456789abcdef"
	cfg.RedirectAllowedDomains = ".example.com"
	r, err := router.New(cfg)
	if err != nil {
		t.Fatalf("router.New: %v", err)
	}
	go r.Start()                    //nolint:errcheck
	t.Cleanup(func() { r.Close() }) //nolint:errcheck
	base := "http://127.0.0.1" + cfg.HTTPPort
	waitFor(t, 15*time.Second, "router HTTP API ready", func() bool {
		resp, err := http.Get(base + "/v1/models")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	siblingMap := fmt.Sprintf(`{"schema":3,"version":1,"region":"eu",
		"sites":[{"id":"site-a","url":"https://site-a.example.com","country":"FR","eu":true}],
		"placements":[{"model":%q,"kind":"chat","site":"site-a","scale_from_zero":true,"hold_budget":%q}]}`, stubModel, holdBudget)
	resp, err := http.Post(base+"/admin/sibling-map", "application/json", strings.NewReader(siblingMap))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	outcomes := make(chan string, n)
	for range n {
		go func() {
			body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}`, stubModel)
			resp, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(body))
			if err != nil {
				outcomes <- err.Error()
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			switch {
			case resp.StatusCode == http.StatusOK:
				outcomes <- "200"
			case resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(b), `"model_waking"`) && resp.Header.Get("Retry-After") == "30":
				outcomes <- "503 model_waking"
			default:
				outcomes <- fmt.Sprintf("%d %s", resp.StatusCode, b)
			}
		}()
	}
	demand := fmt.Sprintf(`hivenet_pending_demand{model=%q} `, stubModel)
	waitFor(t, 5*time.Second, "all requests held", func() bool {
		return metricLine("http://127.0.0.1"+cfg.MetricsPort+"/metrics", demand) == fmt.Sprintf("%s%d", demand, n)
	})

	acfg := config.DefaultAgentConfig()
	acfg.JWTSecret = jwtSecret
	acfg.Model = stubModel
	acfg.Engine = "custom"
	acfg.BackendURL = backend.URL
	acfg.HealthURL = backend.URL + "/health"
	acfg.RouterGRPCAddr = "127.0.0.1" + cfg.GRPCPort
	acfg.RouterP2PAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", cfg.P2PPort)
	acfg.IdentityPath = filepath.Join(dir, "agent.key")
	acfg.Capacity = 1
	a := agent.NewAgent(acfg, &agent.CustomEngine{HealthURL: acfg.HealthURL})
	go a.Run() //nolint:errcheck
	t.Cleanup(a.Stop)

	got := map[string]int{}
	for i := range n {
		select {
		case o := <-outcomes:
			got[o]++
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d of %d requests finished: %v", i, n, got)
		}
	}
	return got
}
