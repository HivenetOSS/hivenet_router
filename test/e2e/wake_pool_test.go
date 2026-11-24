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

// TestHoldAtZeroIgnoresReservedAgents: a model whose shared pool is at zero
// but which has a dedicated replica up. A shared request is held (it may not
// use the reserved replica) and stays held, counted in the pending demand,
// until a shared agent registers: the reserved agent neither releases it nor
// wakes it. The shared pool reads 0 in hivenet_pool_healthy_agents.
func TestHoldAtZeroIgnoresReservedAgents(t *testing.T) {
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

	startAgentWith(t, cfg, backend.URL, filepath.Join(t.TempDir(), "reserved.key"), func(c *config.AgentConfig) { c.ReservedFor = "acme" })
	reserved := fmt.Sprintf(`hivenet_pool_healthy_agents{model=%q,pool="acme"} `, stubModel)
	shared := fmt.Sprintf(`hivenet_pool_healthy_agents{model=%q,pool=""} `, stubModel)
	waitFor(t, 15*time.Second, "the reserved agent registered", func() bool {
		return metricLine(metricsURL, reserved) == reserved+"1"
	})
	if got := metricLine(metricsURL, shared); got != shared+"0" {
		t.Errorf("shared pool gauge %q, want %s0", got, shared)
	}

	type result struct {
		status int
		body   string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}`, stubModel)
		resp, err := http.Post(base+"/v1/chat/completions", "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		done <- result{resp.StatusCode, string(b), err}
	}()
	demand := fmt.Sprintf(`hivenet_pending_demand{model=%q} `, stubModel)
	waitFor(t, 5*time.Second, "the shared request held", func() bool {
		return metricLine(metricsURL, demand) == demand+"1"
	})
	select {
	case res := <-done:
		t.Fatalf("the shared request ended while only a reserved agent is up: %d %s", res.status, res.body)
	case <-time.After(2 * time.Second): // heartbeats of the reserved agent came and went
	}
	if got := metricLine(metricsURL, demand); got != demand+"1" {
		t.Fatalf("pending demand %q, want %s1 while held", got, demand)
	}

	startAgent(t, cfg, backend.URL, filepath.Join(t.TempDir(), "shared.key"))
	select {
	case res := <-done:
		if res.err != nil || res.status != http.StatusOK || !strings.Contains(res.body, "PONG") {
			t.Fatalf("after the shared agent registered: %d %q %v, want 200 with the answer", res.status, res.body, res.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the held request was not served after a shared agent registered")
	}
}
