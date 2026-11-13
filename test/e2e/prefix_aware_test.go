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

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"

	logging "github.com/ipfs/go-log/v2"
)

// prefixBackend is a fake vLLM: it renders a chat as tokens (one per byte,
// each message opened by token 7, a generation prompt at the end), returns
// those prompt tokens and per-chunk token IDs when asked (return_token_ids),
// and records which conversations it served.
type prefixBackend struct {
	*httptest.Server
	mu         sync.Mutex
	served     map[string]int
	askedForID int
}

func (b *prefixBackend) count(conv string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.served[conv]
}

func startPrefixBackend(t *testing.T) *prefixBackend {
	t.Helper()
	b := &prefixBackend{served: map[string]int{}}
	renderMsgs := func(msgs []map[string]string, gen bool) []int {
		var toks []int
		for _, m := range msgs {
			toks = append(toks, 7, len(m["role"]))
			for _, c := range []byte(m["content"]) {
				toks = append(toks, 100+int(c))
			}
			toks = append(toks, 8)
		}
		if gen {
			toks = append(toks, 7, 99)
		}
		return toks
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"object":"list","data":[{"id":%q,"object":"model"}]}`, stubModel)
	})
	mux.HandleFunc("/tokenize", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages            []map[string]string `json:"messages"`
			AddGenerationPrompt bool                `json:"add_generation_prompt"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		json.NewEncoder(w).Encode(map[string]any{"tokens": renderMsgs(req.Messages, req.AddGenerationPrompt)}) //nolint:errcheck
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages       []map[string]string `json:"messages"`
			ReturnTokenIDs bool                `json:"return_token_ids"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		b.mu.Lock()
		for _, m := range req.Messages {
			if m["role"] == "user" {
				b.served[m["content"]]++
				break
			}
		}
		if req.ReturnTokenIDs {
			b.askedForID++
		}
		b.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		first := `{"id":"c","object":"chat.completion.chunk","model":"` + stubModel + `","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"}`
		if req.ReturnTokenIDs {
			ids, _ := json.Marshal(renderMsgs(req.Messages, true))
			first += `,"token_ids":[1]}],"prompt_token_ids":` + string(ids) + `}`
		} else {
			first += `}]}`
		}
		fmt.Fprintf(w, "data: %s\n\n", first)
		fmt.Fprintf(w, `data: {"id":"c","object":"chat.completion.chunk","model":"%s","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}}`+"\n\n", stubModel)
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	b.Server = httptest.NewServer(mux)
	t.Cleanup(b.Close)
	return b
}

func startPrefixAgent(t *testing.T, r *siteRouter, backendURL string) {
	t.Helper()
	cfg := config.DefaultAgentConfig()
	cfg.JWTSecret = jwtSecret
	cfg.Engine = "vllm"
	cfg.BackendURL = backendURL
	cfg.Routers = []string{"127.0.0.1" + r.cfg.GRPCPort}
	cfg.IdentityPath = filepath.Join(t.TempDir(), "agent.key")
	cfg.Region = "e2e"
	a := agent.NewAgent(cfg, &agent.VLLMEngine{})
	go a.Run() //nolint:errcheck
	t.Cleanup(a.Stop)
}

func streamChat(t *testing.T, baseURL string, msgs []map[string]string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"model": stubModel, "stream": true, "messages": msgs})
	resp, err := http.Post(baseURL+"/v1/chat/completions", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, out)
	}
	return string(out)
}

// With strategy prefix-aware on vLLM agents, the turns of a conversation stay
// on the agent that served the first one (the router learns from the prompt
// tokens the agent reports), conversations spread, and token IDs never reach
// the client.
func TestPrefixAwareRoutesByWhatAgentsServed(t *testing.T) {
	policyFile := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyFile, []byte("routing_policy:\n  strategy: prefix-aware\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.PolicyFile = policyFile
	r := startSiteRouterOn(t, cfg)

	backends := []*prefixBackend{startPrefixBackend(t), startPrefixBackend(t), startPrefixBackend(t)}
	for _, b := range backends {
		startPrefixAgent(t, r, b.URL)
	}
	waitFor(t, 20*time.Second, "3 agents registered", func() bool { return healthyAgents(r.baseURL) == len(backends) })

	// 4 applications (system prompts) x 3 conversations each.
	systems := make([]string, 4)
	for i := range systems {
		systems[i] = fmt.Sprintf("App %d. ", i) + strings.Repeat("You are a careful assistant. ", 20)
	}
	convs := make([]string, 12)
	for i := range convs {
		convs[i] = fmt.Sprintf("conversation %d: %s", i, strings.Repeat("details ", 20))
	}
	const turns = 3
	for turn := range turns {
		for i, c := range convs {
			msgs := []map[string]string{{"role": "system", "content": systems[i%len(systems)]}, {"role": "user", "content": c}}
			for k := range turn {
				msgs = append(msgs, map[string]string{"role": "assistant", "content": "ok"}, map[string]string{"role": "user", "content": fmt.Sprintf("follow-up %d", k)})
			}
			out := streamChat(t, r.baseURL, msgs)
			if strings.Contains(out, "token_ids") {
				t.Fatalf("token IDs reached the client: %s", out)
			}
			// Let the agent report the prompt (every 100 ms) before the next turn.
		}
		time.Sleep(300 * time.Millisecond)
	}

	asked := 0
	for _, b := range backends {
		asked += b.askedForID
	}
	if asked == 0 {
		t.Fatal("no agent asked its engine for the prompt tokens")
	}
	used := map[int]bool{}
	for _, c := range convs {
		var homes []int
		for i, b := range backends {
			if b.count(c) > 0 {
				homes = append(homes, i)
			}
		}
		if len(homes) != 1 {
			t.Errorf("%.20q served by agents %v, want one", c, homes)
		}
		for _, h := range homes {
			used[h] = true
		}
	}
	if len(used) < 2 {
		t.Errorf("all applications on agents %v: different system prompts should spread", used)
	}
	metrics := getMetrics(t, "http://127.0.0.1"+cfg.MetricsPort+"/metrics")
	if !strings.Contains(metrics, `hivenet_prefix_routing_total{model="`+stubModel+`",outcome="hit"}`) {
		t.Errorf("no hit counted:\n%s", grepLines(metrics, "hivenet_prefix_routing_total"))
	}
}

func getMetrics(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// Prefix-aware routing never logs prompt content or token IDs, even at debug
// level: the prompts carry a marker that must not appear in any log line.
func TestPrefixAwareLeavesNoContentInLogs(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "debug.log")
	old := logging.GetConfig()
	logging.SetupLogging(logging.Config{Format: logging.JSONOutput, Level: logging.LevelDebug, File: logFile})
	logging.SetAllLoggers(logging.LevelDebug)
	t.Cleanup(func() { logging.SetupLogging(old) })

	policyFile := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyFile, []byte("routing_policy:\n  strategy: prefix-aware\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.PolicyFile = policyFile
	r := startSiteRouterOn(t, cfg)
	b := startPrefixBackend(t)
	startPrefixAgent(t, r, b.URL)
	waitFor(t, 20*time.Second, "agent registered", func() bool { return healthyAgents(r.baseURL) == 1 })

	const marker = "ZZPRIVATEMARKERZZ"
	system := marker + " " + strings.Repeat("system rules ", 30)
	for turn := range 3 {
		msgs := []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": marker + " question " + strings.Repeat("x", 200)}}
		for k := range turn {
			msgs = append(msgs, map[string]string{"role": "assistant", "content": "ok"}, map[string]string{"role": "user", "content": fmt.Sprintf("%s follow-up %d", marker, k)})
		}
		streamChat(t, r.baseURL, msgs)
		time.Sleep(200 * time.Millisecond)
	}
	logs, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) == 0 {
		t.Fatal("no debug log captured: the check would prove nothing")
	}
	for _, leak := range []string{marker, "prompt_token_ids", "token_ids"} {
		if strings.Contains(string(logs), leak) {
			t.Errorf("logs contain %q", leak)
		}
	}
}
