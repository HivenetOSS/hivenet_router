// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
)

// TestEngineVersionFollowsBackendRestart: a vLLM agent reports its engine
// version, and the router counts /v1/messages as a prefix-cache gap below
// vLLM 0.27.0 (which ignores cache_salt there). When the engine is restarted
// with a newer build while the agent keeps running, the agent reads the
// version again once the backend answers its health check again, registers
// anew, and the gap clears without restarting the agent.
func TestEngineVersionFollowsBackendRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	var version atomic.Value
	version.Store("0.26.0")
	var down atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"version":%q}`, version.Load())
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"object":"list","data":[{"id":"chat-vllm","object":"model"}]}`)
	})
	backend := httptest.NewServer(mux)
	t.Cleanup(backend.Close)

	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey, Mutate: func(c *config.Config) {
		c.CacheSalt, c.CacheSaltSecret = true, "e2e-cache-salt-secret-0123456789abcdef"
	}})
	startAgentWithEngine(t, rcfg, backend.URL, filepath.Join(t.TempDir(), "agent.key"), &agent.VLLMEngine{}, func(c *config.AgentConfig) {
		c.Model, c.Capability = "chat-vllm", domain.CapabilityLLM
		c.HardwareSampleInterval = 100 * time.Millisecond // the backend health check
	})
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool { return modelHealthyAs(base, isoKeyA, "chat-vllm") })

	gap := `hivenet_cache_isolation_gaps{engine="vllm",model="chat-vllm"} 1`
	metrics := func() string {
		resp, err := http.Get("http://127.0.0.1" + rcfg.MetricsPort + "/metrics")
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	waitFor(t, 10*time.Second, "vLLM 0.26.0 counted as a /v1/messages gap", func() bool { return strings.Contains(metrics(), gap) })

	// The engine restarts with 0.31.0: down for a few health checks, then up.
	down.Store(true)
	time.Sleep(500 * time.Millisecond)
	version.Store("0.31.0")
	down.Store(false)
	waitFor(t, 15*time.Second, "the gap to clear after the restart", func() bool {
		m := metrics()
		return m != "" && !strings.Contains(m, `hivenet_cache_isolation_gaps{engine="vllm",model="chat-vllm"}`)
	})
}
