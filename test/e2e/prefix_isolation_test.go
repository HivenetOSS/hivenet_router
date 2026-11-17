// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/config"
)

// TestIsolation_PrefixIndexPerTenant: with the cache salt on, prefix-aware
// routing keys its index per tenant. A prompt tenant A taught the router
// matches for A's next request, never for tenant B's identical one, as B
// could not use A's cached blocks anyway. With the salt off, tenants share
// the engine's cache and so the index (the control that shows the test can
// see a cross-tenant match).
func TestIsolation_PrefixIndexPerTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	for _, salt := range []bool{true, false} {
		t.Run(fmt.Sprintf("salt=%v", salt), func(t *testing.T) {
			policyFile := filepath.Join(t.TempDir(), "policy.yaml")
			if err := os.WriteFile(policyFile, []byte("routing_policy:\n  strategy: prefix-aware\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			authYAML := writeAuthYAML(
				testKey{Token: isoKeyA, Owner: "tenant-a", Name: "a"},
				testKey{Token: isoKeyB, Owner: "tenant-b", Name: "b"},
			)
			base, rcfg := startRouterWith(t, routerOpts{AuthYAML: authYAML, AdminKey: isoAdminKey, Mutate: func(c *config.Config) {
				c.PolicyFile = policyFile
				c.CacheSalt = salt
				if salt {
					c.CacheSaltSecret = "e2e-cache-salt-secret-0123456789abcdef"
				}
			}})
			r := &siteRouter{baseURL: base, cfg: rcfg}
			for range 2 {
				startPrefixAgent(t, r, startPrefixBackend(t).URL)
			}
			waitFor(t, 20*time.Second, "2 agents registered", func() bool { return modelHealthyAs(base, isoKeyA, stubModel) })
			time.Sleep(time.Second) // both agents, not just the first

			msgs := []map[string]string{
				{"role": "system", "content": strings.Repeat("You are a careful assistant. ", 30)},
				{"role": "user", "content": "the same question from both tenants " + strings.Repeat("details ", 30)},
			}
			metricsURL := "http://127.0.0.1" + rcfg.MetricsPort + "/metrics"
			hits := func() int { return prefixOutcome(t, metricsURL, "hit") }

			streamChatAs(t, base, isoKeyA, msgs)
			// Let the agent report the prompt (every 100 ms) and the
			// pending note expire into the memo's answer.
			time.Sleep(500 * time.Millisecond)
			streamChatAs(t, base, isoKeyA, msgs)
			if hits() != 1 {
				t.Fatalf("tenant A's repeat: %d hits, want 1", hits())
			}

			streamChatAs(t, base, isoKeyB, msgs)
			wantB := 1
			if !salt {
				wantB = 2
			}
			if got := hits(); got != wantB {
				t.Fatalf("tenant B's identical prompt: hits %d, want %d", got, wantB)
			}
			streamChatAs(t, base, isoKeyA, msgs)
			if got := hits(); got != wantB+1 {
				t.Fatalf("tenant A again: hits %d, want %d", got, wantB+1)
			}
		})
	}
}

// streamChatAs is streamChat with an API key.
func streamChatAs(t *testing.T, baseURL, key string, msgs []map[string]string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"model": stubModel, "stream": true, "messages": msgs})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, out)
	}
}

// prefixOutcome reads hivenet_prefix_routing_total for stubModel and outcome
// (0 when the series does not exist yet).
func prefixOutcome(t *testing.T, metricsURL, outcome string) int {
	t.Helper()
	prefix := `hivenet_prefix_routing_total{model="` + stubModel + `",outcome="` + outcome + `"} `
	for _, line := range strings.Split(getMetrics(t, metricsURL), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("%s: %v", line, err)
			}
			return int(n)
		}
	}
	return 0
}
