// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

// Tenant-isolation checklist (test/isolation/CHECKLIST.md), end to end: a
// real router with two static-key tenants and a real agent.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/config"
)

const (
	isoAdminKey = "sk-e2e-admin"
	isoKeyA     = "sk-e2e-tenant-a"
	isoKeyB     = "sk-e2e-tenant-b"
)

// isoAuthYAML: tenant-a and tenant-b, one key each.
var isoAuthYAML = writeAuthYAML(
	testKey{Token: isoKeyA, Owner: "tenant-a", Name: "a"},
	testKey{Token: isoKeyB, Owner: "tenant-b", Name: "b"},
)

var markerRe = regexp.MustCompile(`marker-[a-z]+-\d+`)

// startEchoBackend answers chat completions with the marker found in the
// request (marker-<tenant>-<n>), streaming or not, after a short delay so
// that concurrent requests overlap inside the router and agent.
func startEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(body, &req)
		marker := markerRe.FindString(string(body))
		time.Sleep(20 * time.Millisecond)
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fl, _ := w.(http.Flusher)
			for _, part := range []string{marker[:len(marker)/2], marker[len(marker)/2:]} {
				fmt.Fprintf(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":%q,"choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`+"\n\n", stubModel, part)
				if fl != nil {
					fl.Flush()
				}
				time.Sleep(5 * time.Millisecond)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"c","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel, marker)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// chatAs sends one chat completion with the given key and returns the
// assistant text (joined across SSE chunks when streaming).
func chatAs(base, key, content string, stream bool) (int, string, error) {
	payload := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":10,"stream":%t}`,
		stubModel, content, stream)
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(payload))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if !stream {
		var out struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		raw, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(raw, &out); err != nil || len(out.Choices) == 0 {
			return resp.StatusCode, string(raw), nil
		}
		return resp.StatusCode, out.Choices[0].Message.Content, nil
	}
	var text strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || line == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(line), &chunk) == nil && len(chunk.Choices) > 0 {
			text.WriteString(chunk.Choices[0].Delta.Content)
		}
	}
	return resp.StatusCode, text.String(), sc.Err()
}

// startIsoStack starts a router with the two-tenant auth.yaml and one agent
// on the given backend, and waits until tenant-a can see the model.
func startIsoStack(t *testing.T, backendURL string, capacity int) string {
	t.Helper()
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, backendURL, filepath.Join(t.TempDir(), "agent.key"), func(c *config.AgentConfig) {
		c.Capacity = capacity
	})
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, isoKeyA, stubModel)
	})
	return base
}

// TestIsolation_ConcurrentResponsesNotCrossWired: two tenants send many
// concurrent requests, streaming and not, through one router and one agent.
// Every response carries its own request's marker, never another request's
// (in particular never the other tenant's).
func TestIsolation_ConcurrentResponsesNotCrossWired(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base := startIsoStack(t, startEchoBackend(t).URL, 64)

	const perTenant = 25
	var wg sync.WaitGroup
	errs := make(chan string, 2*perTenant)
	for _, tenant := range []struct{ name, key string }{{"a", isoKeyA}, {"b", isoKeyB}} {
		for i := range perTenant {
			wg.Add(1)
			go func() {
				defer wg.Done()
				marker := fmt.Sprintf("marker-%s-%d", tenant.name, i)
				status, got, err := chatAs(base, tenant.key, "echo "+marker, i%2 == 0)
				switch {
				case err != nil:
					errs <- fmt.Sprintf("%s: %v", marker, err)
				case status != http.StatusOK:
					errs <- fmt.Sprintf("%s: status %d body %q", marker, status, got)
				case got != marker:
					errs <- fmt.Sprintf("%s: got response %q (another request's answer)", marker, got)
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
