// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

// TestHoldAtZeroEndToEnd runs the real router with no agent for a model that
// the sibling map places on this policloud with scale_from_zero. A streaming
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
	cfg.SiteID = "pc-a"
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

	siblingMap := fmt.Sprintf(`{"schema":2,"version":1,"region":"eu",
		"policlouds":[{"id":"pc-a","url":"https://pc-a.example.com","country":"FR","eu":true}],
		"placements":[{"model":%q,"kind":"chat","policloud":"pc-a","scale_from_zero":true,"hold_budget":"60s"}]}`, stubModel)
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

	startAgent(t, cfg, backend.URL, filepath.Join(t.TempDir(), "agent.key"))

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
