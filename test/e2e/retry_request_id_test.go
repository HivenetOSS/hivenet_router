// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestRetrySendsItsOwnRequestID: a retry reaches the backend with its own ID,
// <id>-2, so a backend that keys requests by ID (vLLM) never sees the same ID
// twice. The client still gets the router's request ID.
func TestRetrySendsItsOwnRequestID(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		seen = append(seen, r.Header.Get("X-Request-ID"))
		first := len(seen) == 1
		mu.Unlock()
		if first { // the first attempt fails, so the router retries
			http.Error(w, "backend failure", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"x","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	})
	backend := httptest.NewServer(mux)
	t.Cleanup(backend.Close)

	base, rcfg := startRouter(t)
	// Two agents, so the retry has another agent to go to.
	startAgent(t, rcfg, backend.URL, filepath.Join(t.TempDir(), "a1.key"))
	startAgent(t, rcfg, backend.URL, filepath.Join(t.TempDir(), "a2.key"))
	waitFor(t, 20*time.Second, "two healthy agents", func() bool { return healthyAgents(base) >= 2 })

	resp, err := postChat(base, false)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	id := resp.Header.Get("X-Request-ID")

	mu.Lock()
	defer mu.Unlock()
	want := []string{id, id + "-2"}
	if id == "" || fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Errorf("backend saw request IDs %q, want %q", seen, want)
	}
}

// healthyAgents returns how many healthy agents /v1/models lists for stubModel.
func healthyAgents(baseURL string) int {
	resp, err := http.Get(baseURL + "/v1/models")
	if err != nil {
		return 0
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
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return 0
	}
	for _, m := range out.Data {
		if m.ID == stubModel {
			return m.Agents.Healthy
		}
	}
	return 0
}
