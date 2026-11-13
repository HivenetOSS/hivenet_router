// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
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

	"hivenet_router/internal/config"
)

// conversationBackend is a stub backend that records which conversations
// (first user message) it served.
type conversationBackend struct {
	*httptest.Server
	mu   sync.Mutex
	seen map[string]int
}

func startConversationBackend(t *testing.T) *conversationBackend {
	t.Helper()
	b := &conversationBackend{seen: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.Unmarshal(body, &req)
		for _, m := range req.Messages {
			if m.Role == "user" {
				b.mu.Lock()
				b.seen[m.Content]++
				b.mu.Unlock()
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"c","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	})
	b.Server = httptest.NewServer(mux)
	t.Cleanup(b.Close)
	return b
}

func (b *conversationBackend) served(conv string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seen[conv]
}

// postTurn sends turn n of conversation conv: system prompt, the first user
// message, then n-1 earlier exchanges.
func postTurn(t *testing.T, baseURL, conv string, n int) {
	t.Helper()
	msgs := []map[string]string{
		{"role": "system", "content": "You are a helpful assistant."},
		{"role": "user", "content": conv},
	}
	for i := 1; i < n; i++ {
		msgs = append(msgs,
			map[string]string{"role": "assistant", "content": "ok"},
			map[string]string{"role": "user", "content": fmt.Sprintf("follow-up %d", i)})
	}
	body, _ := json.Marshal(map[string]any{"model": stubModel, "messages": msgs})
	resp, err := http.Post(baseURL+"/v1/chat/completions", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("turn %d of %q: %v", n, conv, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("turn %d of %q: HTTP %d: %s", n, conv, resp.StatusCode, b)
	}
}

// With strategy prefix-aware, every turn of a conversation reaches the same
// agent, whichever router of the site it goes through: the routers compute
// the same ring without sharing anything.
func TestPrefixAwareKeepsConversationOnOneAgentAcrossRouters(t *testing.T) {
	policyFile := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyFile, []byte("routing_policy:\n  strategy: prefix-aware\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	newRouter := func() *siteRouter {
		cfg := config.DefaultConfig()
		cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
		cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
		cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
		cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
		cfg.PolicyFile = policyFile
		return startSiteRouterOn(t, cfg)
	}
	routers := []*siteRouter{newRouter(), newRouter()}

	backends := make([]*conversationBackend, 3)
	for i := range backends {
		backends[i] = startConversationBackend(t)
		startMultiHomeAgent(t, routers, backends[i].URL, 0)
	}
	for i, r := range routers {
		waitFor(t, 20*time.Second, fmt.Sprintf("3 agents on router %d", i), func() bool {
			return healthyAgents(r.baseURL) == len(backends)
		})
	}

	// 12 conversations: least-loaded would send them all to one idle agent;
	// the ring spreads them (all on one agent: probability 3^-11).
	convs := make([]string, 12)
	for i := range convs {
		convs[i] = fmt.Sprintf("conversation %d", i)
	}
	const turns = 4
	for n := 1; n <= turns; n++ {
		for i, conv := range convs {
			postTurn(t, routers[(n+i)%len(routers)].baseURL, conv, n) // alternate routers
		}
	}

	used := map[int]bool{}
	for _, conv := range convs {
		var homes []int
		for i, b := range backends {
			if c := b.served(conv); c > 0 {
				homes = append(homes, i)
				if c != turns {
					t.Errorf("%q: agent %d served %d of %d turns", conv, i, c, turns)
				}
			}
		}
		if len(homes) != 1 {
			t.Errorf("%q: served by agents %v, want exactly one", conv, homes)
		}
		for _, h := range homes {
			used[h] = true
		}
	}
	if len(used) < 2 {
		t.Errorf("all conversations on agents %v: the ring should spread them", used)
	}
}
