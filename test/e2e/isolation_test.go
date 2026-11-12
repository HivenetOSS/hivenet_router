// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

// Tenant-isolation checklist (test/isolation/CHECKLIST.md), end to end: a
// real router with two static-key tenants and a real agent.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
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

// postAs sends one raw chat-completions body with the given key and returns
// the status and response body.
func postAs(t *testing.T, base, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// TestIsolation_TwoTenantsQuotaE2E: one router, one agent, two tenants with
// two keys each. A key exhausting its request rate or its daily token budget
// is refused with 429; the other key of the same tenant and the other
// tenant's keys keep being served.
func TestIsolation_TwoTenantsQuotaE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	const (
		rpmA1, rpmA2, rpmB1 = "sk-q-a1", "sk-q-a2", "sk-q-b1"
		tpdA, tpdB          = "sk-q-a-tpd", "sk-q-b-tpd"
	)
	auth := writeAuthYAML(
		testKey{Token: rpmA1, Owner: "tenant-a", Name: "a1", RPM: 2},
		testKey{Token: rpmA2, Owner: "tenant-a", Name: "a2", RPM: 2},
		testKey{Token: rpmB1, Owner: "tenant-b", Name: "b1", RPM: 2},
		// 170 tokens/day: one request reserving 150 output tokens on a ~14
		// token prompt fits; after it is charged (prompt + 1 output token)
		// the next identical request no longer does.
		testKey{Token: tpdA, Owner: "tenant-a", Name: "a-tpd", TPD: 170},
		testKey{Token: tpdB, Owner: "tenant-b", Name: "b-tpd", TPD: 170},
	)
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: auth, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, startStubBackend(t).URL, filepath.Join(t.TempDir(), "agent.key"), nil)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, rpmB1, stubModel)
	})
	small := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":5}`, stubModel)

	t.Run("requests per minute", func(t *testing.T) {
		for i := range 2 {
			if code, body := postAs(t, base, rpmA1, small); code != http.StatusOK {
				t.Fatalf("a1 request %d: %d %s", i, code, body)
			}
		}
		if code, _ := postAs(t, base, rpmA1, small); code != http.StatusTooManyRequests {
			t.Fatalf("a1 third request: %d, want 429", code)
		}
		if code, body := postAs(t, base, rpmA2, small); code != http.StatusOK {
			t.Errorf("a2 (same tenant, own bucket) after a1 hit its limit: %d %s", code, body)
		}
		if code, body := postAs(t, base, rpmB1, small); code != http.StatusOK {
			t.Errorf("b1 (other tenant) after a1 hit its limit: %d %s", code, body)
		}
	})

	t.Run("tokens per day", func(t *testing.T) {
		big := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":150}`,
			stubModel, strings.Repeat("x", 40))
		if code, body := postAs(t, base, tpdA, big); code != http.StatusOK {
			t.Fatalf("a-tpd first request: %d %s", code, body)
		}
		if code, body := postAs(t, base, tpdA, big); code != http.StatusTooManyRequests || !strings.Contains(body, "token") {
			t.Fatalf("a-tpd second request: %d %s, want 429 (daily budget spent)", code, body)
		}
		if code, body := postAs(t, base, tpdB, big); code != http.StatusOK {
			t.Errorf("b-tpd (other tenant) after a-tpd spent its budget: %d %s", code, body)
		}
		if code, body := postAs(t, base, rpmA2, small); code != http.StatusOK {
			t.Errorf("a2 (same tenant, no daily limit) after a-tpd spent its budget: %d %s", code, body)
		}
	})
}

// tenantSeries sums a Prometheus counter's samples per tenant_id and lists
// the key_id values seen per tenant.
func tenantSeries(t *testing.T, metricsURL, name string) (sum map[string]float64, keys map[string]map[string]bool) {
	t.Helper()
	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sum, keys = map[string]float64{}, map[string]map[string]bool{}
	label := func(line, l string) string {
		m := regexp.MustCompile(l + `="([^"]*)"`).FindStringSubmatch(line)
		if m == nil {
			return ""
		}
		return m[1]
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		var v float64
		if _, err := fmt.Sscan(line[strings.LastIndex(line, " ")+1:], &v); err != nil {
			continue
		}
		tenant, key := label(line, "tenant_id"), label(line, "key_id")
		sum[tenant] += v
		if keys[tenant] == nil {
			keys[tenant] = map[string]bool{}
		}
		keys[tenant][key] = true
	}
	return sum, keys
}

// TestIsolation_TenantMetricsAttribution: a tenant's requests move only that
// tenant's hivenet_tenant_* series, labelled with its own key ID (a static
// key's derived ID, not "anonymous").
func TestIsolation_TenantMetricsAttribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, startStubBackend(t).URL, filepath.Join(t.TempDir(), "agent.key"), nil)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, isoKeyA, stubModel)
	})
	metricsURL := "http://127.0.0.1" + rcfg.MetricsPort + "/metrics"
	const success = "hivenet_tenant_requests_success_total"
	small := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":5}`, stubModel)

	for range 3 {
		if code, body := postAs(t, base, isoKeyA, small); code != http.StatusOK {
			t.Fatalf("tenant-a request: %d %s", code, body)
		}
	}
	waitFor(t, 5*time.Second, "tenant-a success counter at 3", func() bool {
		sum, _ := tenantSeries(t, metricsURL, success)
		return sum["tenant-a"] == 3
	})
	if sum, _ := tenantSeries(t, metricsURL, success); sum["tenant-b"] != 0 {
		t.Errorf("tenant-b success %v after only tenant-a sent requests, want 0", sum["tenant-b"])
	}

	if code, body := postAs(t, base, isoKeyB, small); code != http.StatusOK {
		t.Fatalf("tenant-b request: %d %s", code, body)
	}
	waitFor(t, 5*time.Second, "tenant-b success counter at 1", func() bool {
		sum, _ := tenantSeries(t, metricsURL, success)
		return sum["tenant-b"] == 1
	})
	sum, keys := tenantSeries(t, metricsURL, success)
	if sum["tenant-a"] != 3 {
		t.Errorf("tenant-a success %v after tenant-b's request, want still 3", sum["tenant-a"])
	}
	for tenant, token := range map[string]string{"tenant-a": isoKeyA, "tenant-b": isoKeyB} {
		want := derivedStaticKeyID(token)
		if !keys[tenant][want] || keys[tenant]["anonymous"] {
			t.Errorf("%s key_id labels %v, want only its own %q", tenant, keys[tenant], want)
		}
	}
}

// derivedStaticKeyID is the key ID the router derives for a static key.
func derivedStaticKeyID(token string) string {
	h := sha256.Sum256([]byte(token))
	id := sha256.Sum256([]byte("hivenet-router/key-id/v1\x00" + hex.EncodeToString(h[:])))
	return "static-" + hex.EncodeToString(id[:8])
}

// bodyRecorder is a backend for every engine path the router forwards to. It
// records each request body by path and answers with a minimal valid reply
// (streaming for chat when asked).
type bodyRecorder struct {
	mu    sync.Mutex
	calls []backendCall
}

func (b *bodyRecorder) start(t *testing.T, models ...string) *httptest.Server {
	t.Helper()
	record := func(r *http.Request) []byte {
		body, _ := io.ReadAll(r.Body)
		b.mu.Lock()
		b.calls = append(b.calls, backendCall{path: r.URL.Path, body: body})
		b.mu.Unlock()
		return body
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		data := make([]string, len(models))
		for i, m := range models {
			data[i] = fmt.Sprintf(`{"id":%q,"object":"model"}`, m)
		}
		fmt.Fprintf(w, `{"object":"list","data":[%s]}`, strings.Join(data, ","))
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(record(r), &req)
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"PONG"},"finish_reason":null}]}`+"\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"x","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	})
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"m","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":"PONG"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	})
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1]}],"model":"m","usage":{"prompt_tokens":1,"total_tokens":1}}`)
	})
	mux.HandleFunc("/v1/rerank", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"results":[{"index":0,"relevance_score":0.5}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func (b *bodyRecorder) last() backendCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.calls) == 0 {
		return backendCall{}
	}
	return b.calls[len(b.calls)-1]
}

// startEngineAgent runs an agent with the given engine, serving model with
// the given capability, against backendURL.
func startEngineAgent(t *testing.T, rcfg *config.Config, backendURL string, engine agent.Engine, model, capability string) {
	t.Helper()
	startAgentWithEngine(t, rcfg, backendURL, filepath.Join(t.TempDir(), model+".key"), engine, func(c *config.AgentConfig) {
		c.Model = model
		c.Capability = capability
	})
}

// TestIsolation_CacheSaltPerTenant: with --cache-salt, every request the
// router forwards to a vLLM agent carries exactly one cache_salt, the
// tenant's: chat (streaming and not), Anthropic messages, embeddings and
// rerank. Two keys of one tenant share the salt; two tenants never do. A
// cache_salt sent by the client (here twice, to try both of the backend's
// duplicate-key rules) is replaced, and the rest of the body is unchanged.
// An agent whose engine has no salt support gets no cache_salt at all.
func TestIsolation_CacheSaltPerTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	const keyA2 = "sk-e2e-tenant-a-2"
	authYAML := writeAuthYAML(
		testKey{Token: isoKeyA, Owner: "tenant-a", Name: "a1"},
		testKey{Token: keyA2, Owner: "tenant-a", Name: "a2"},
		testKey{Token: isoKeyB, Owner: "tenant-b", Name: "b"},
	)
	saltSecret := "e2e-cache-salt-secret-0123456789abcdef"
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: authYAML, AdminKey: isoAdminKey, Mutate: func(c *config.Config) {
		c.CacheSalt = true
		c.CacheSaltSecret = saltSecret
	}})
	be := &bodyRecorder{}
	backend := be.start(t, "chat-vllm", "embed-vllm", "rerank-vllm", "chat-plain", "chat-sglang", "chat-llama")
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "chat-vllm", domain.CapabilityLLM)
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "embed-vllm", domain.CapabilityEmbedding)
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "rerank-vllm", domain.CapabilityReranker)
	startEngineAgent(t, rcfg, backend.URL, &agent.CustomEngine{HealthURL: backend.URL + "/health"}, "chat-plain", domain.CapabilityLLM)
	startEngineAgent(t, rcfg, backend.URL, &agent.SGLangEngine{}, "chat-sglang", domain.CapabilityLLM)
	startEngineAgent(t, rcfg, backend.URL, &agent.LlamaCPPEngine{}, "chat-llama", domain.CapabilityLLM)
	for _, m := range []string{"chat-vllm", "embed-vllm", "rerank-vllm", "chat-plain", "chat-sglang", "chat-llama"} {
		waitFor(t, 30*time.Second, m+" registered and healthy", func() bool { return modelHealthyAs(base, isoKeyA, m) })
	}

	type call struct{ name, path, body string }
	vllmCalls := []call{
		{"chat", "/v1/chat/completions", `{"model":"chat-vllm","cache_salt":"guess-1","messages":[{"role":"user","content":"hi"}],"max_tokens":5,"cache_salt":"guess-2"}`},
		{"streaming chat", "/v1/chat/completions", `{"model":"chat-vllm","messages":[{"role":"user","content":"hi"}],"max_tokens":5,"stream":true,"cache_salt":"guess"}`},
		{"messages", "/v1/messages", `{"model":"chat-vllm","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}`},
		{"embeddings", "/v1/embeddings", `{"model":"embed-vllm","input":"x","cache_salt":"guess"}`},
		{"rerank", "/v1/rerank", `{"model":"rerank-vllm","query":"q","documents":["d"]}`},
	}
	// send posts c with key and returns the body the backend received,
	// decoded, plus how many top-level cache_salt members it had.
	send := func(t *testing.T, key string, c call) (map[string]any, int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, base+c.path, strings.NewReader(c.body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d body %s", c.name, resp.StatusCode, raw)
		}
		got := be.last()
		if got.path != c.path {
			t.Fatalf("%s: backend saw path %q, want %q", c.name, got.path, c.path)
		}
		var m map[string]any
		if err := json.Unmarshal(got.body, &m); err != nil {
			t.Fatalf("%s: backend body %q: %v", c.name, got.body, err)
		}
		return m, strings.Count(string(got.body), `"cache_salt"`)
	}
	saltFmt := regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

	for _, c := range vllmCalls {
		t.Run(c.name, func(t *testing.T) {
			salts := map[string]any{}
			for name, key := range map[string]string{"a1": isoKeyA, "a2": keyA2, "b": isoKeyB} {
				got, n := send(t, key, c)
				salt, _ := got["cache_salt"].(string)
				if n != 1 || !saltFmt.MatchString(salt) || strings.HasPrefix(salt, "guess") {
					t.Fatalf("%s: backend got %d cache_salt members, value %q; want exactly one router salt", name, n, salt)
				}
				salts[name] = salt
				var sent map[string]any
				_ = json.Unmarshal([]byte(c.body), &sent)
				delete(sent, "cache_salt")
				delete(got, "cache_salt")
				if !reflect.DeepEqual(got, sent) {
					t.Errorf("%s: body changed beyond cache_salt:\n got  %v\n sent %v", name, got, sent)
				}
			}
			if salts["a1"] != salts["a2"] {
				t.Errorf("keys of one tenant got different salts %v / %v", salts["a1"], salts["a2"])
			}
			if salts["a1"] == salts["b"] {
				t.Errorf("tenant-a and tenant-b share salt %v", salts["a1"])
			}
		})
	}

	t.Run("engine without salt support", func(t *testing.T) {
		got, n := send(t, isoKeyA, call{"plain chat", "/v1/chat/completions",
			`{"model":"chat-plain","cache_salt":"guess","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`})
		if n != 0 {
			t.Errorf("custom-engine backend got %d cache_salt members (%v), want none", n, got["cache_salt"])
		}
	})

	t.Run("sglang chat is salted", func(t *testing.T) {
		got, n := send(t, isoKeyA, call{"sglang chat", "/v1/chat/completions",
			`{"model":"chat-sglang","cache_salt":"guess","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`})
		if salt, _ := got["cache_salt"].(string); n != 1 || !saltFmt.MatchString(salt) {
			t.Errorf("sglang backend got %d cache_salt members, value %q; want the router salt", n, salt)
		}
	})

	t.Run("llama.cpp chat skips its prompt cache", func(t *testing.T) {
		got, n := send(t, isoKeyA, call{"llama.cpp chat", "/v1/chat/completions",
			`{"model":"chat-llama","cache_salt":"guess","cache_prompt":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}`})
		if n != 0 || got["cache_prompt"] != false {
			t.Errorf("llama.cpp backend got %d cache_salt members and cache_prompt %v; want none and false", n, got["cache_prompt"])
		}
	})

	// The router counts the LLM agents left with a gap: custom (no
	// isolation), SGLang and llama.cpp (no isolation on /v1/messages); the
	// vLLM agents have none.
	t.Run("agents without isolation are counted", func(t *testing.T) {
		resp, err := http.Get("http://127.0.0.1" + rcfg.MetricsPort + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		for _, want := range []string{
			`hivenet_cache_isolation_gaps{engine="custom",model="chat-plain"} 1`,
			`hivenet_cache_isolation_gaps{engine="sglang",model="chat-sglang"} 1`,
			`hivenet_cache_isolation_gaps{engine="llamacpp",model="chat-llama"} 1`,
		} {
			if !strings.Contains(string(raw), want) {
				t.Errorf("metrics lack %s", want)
			}
		}
		if strings.Contains(string(raw), `hivenet_cache_isolation_gaps{engine="vllm"`) {
			t.Error("a vLLM agent is counted as having a gap")
		}
	})
}

// postFull posts body to path with key and returns the status and the
// router's X-Request-ID.
func postFull(t *testing.T, base, key, path, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("X-Request-ID")
}

// auditLines returns the audit log's records keyed by request_id.
func auditLines(t *testing.T) map[string]map[string]any {
	t.Helper()
	b, err := os.ReadFile(auditLogPath())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]any{}
	for _, line := range strings.Split(string(b), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil {
			if id, _ := rec["request_id"].(string); id != "" {
				out[id] = rec
			}
		}
	}
	return out
}

// TestIsolation_LogsAttributable: every authenticated request's audit line
// names the caller's tenant and its own key ID (static keys' derived ID,
// never empty), so a customer's activity can be selected from a shared log.
func TestIsolation_LogsAttributable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, (&bodyRecorder{}).start(t, stubModel).URL, filepath.Join(t.TempDir(), "agent.key"), nil)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, isoKeyA, stubModel)
	})
	small := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":5}`, stubModel)
	want := map[string][2]string{} // request ID → tenant, key ID
	for tenant, key := range map[string]string{"tenant-a": isoKeyA, "tenant-b": isoKeyB} {
		for _, path := range []string{"/v1/chat/completions", "/v1/messages"} {
			code, id := postFull(t, base, key, path, small)
			if code != http.StatusOK || id == "" {
				t.Fatalf("%s %s: status %d request ID %q", tenant, path, code, id)
			}
			want[id] = [2]string{tenant, derivedStaticKeyID(key)}
		}
	}
	waitFor(t, 5*time.Second, "audit lines written", func() bool {
		lines := auditLines(t)
		for id := range want {
			if lines[id] == nil {
				return false
			}
		}
		return true
	})
	lines := auditLines(t)
	for id, w := range want {
		if got := lines[id]; got["tenant_id"] != w[0] || got["key_id"] != w[1] {
			t.Errorf("audit line %s: tenant_id %v key_id %v, want %s / %s", id, got["tenant_id"], got["key_id"], w[0], w[1])
		}
	}
}

// TestIsolation_AuditFileMode: the audit log, which names every tenant and
// key, is readable by the router's user only, even when the file already
// existed with a wider mode.
func TestIsolation_AuditFileMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base, _ := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	postFull(t, base, isoKeyA, "/v1/chat/completions", `{"model":"none"}`) // any audited request opens the file
	fi, err := os.Stat(auditLogPath())
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("audit log mode %o, want 600", perm)
	}
}

// TestIsolation_LogsContentFree: no log the router or agent writes (audit,
// application at debug level, access) contains a prompt, a completion, an
// API key, a client-sent cache_salt, the salt secret or a derived salt.
// This guards doc-18 G7 (content-free logging is a convention) by test.
func TestIsolation_LogsContentFree(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	const (
		promptCanary = "canary-prompt-7f3a9c"
		clientSalt   = "canary-client-salt-41d2"
		badKey       = "sk-canary-not-a-key-90b1"
		saltSecret   = "canary-salt-secret-5e8b0123456789abcdef"
	)
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey, Mutate: func(c *config.Config) {
		c.CacheSalt = true
		c.CacheSaltSecret = saltSecret
	}})
	be := &bodyRecorder{}
	backend := be.start(t, "chat-vllm")
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "chat-vllm", domain.CapabilityLLM)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool { return modelHealthyAs(base, isoKeyA, "chat-vllm") })

	for _, body := range []string{
		fmt.Sprintf(`{"model":"chat-vllm","cache_salt":%q,"messages":[{"role":"user","content":%q}],"max_tokens":5}`, clientSalt, promptCanary),
		fmt.Sprintf(`{"model":"chat-vllm","stream":true,"messages":[{"role":"user","content":%q}],"max_tokens":5}`, promptCanary),
	} {
		if code, _ := postFull(t, base, isoKeyA, "/v1/chat/completions", body); code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
	}
	if code, _ := postFull(t, base, isoKeyA, "/v1/messages", fmt.Sprintf(`{"model":"chat-vllm","max_tokens":5,"messages":[{"role":"user","content":%q}]}`, promptCanary)); code != http.StatusOK {
		t.Fatalf("messages status %d", code)
	}
	if code, _ := postFull(t, base, badKey, "/v1/chat/completions", `{"model":"chat-vllm"}`); code != http.StatusUnauthorized {
		t.Fatalf("bad key status %d", code)
	}
	var derived map[string]any
	_ = json.Unmarshal(be.last().body, &derived)
	salt, _ := derived["cache_salt"].(string)
	if salt == "" {
		t.Fatal("backend saw no salt; the test would not check the derived salt")
	}

	time.Sleep(200 * time.Millisecond) // let the last log lines land
	secrets := map[string]string{
		"prompt": promptCanary, "completion": "PONG", "API key": isoKeyA, "rejected key": badKey,
		"client salt": clientSalt, "salt secret": saltSecret, "derived salt": salt,
	}
	for _, path := range []string{auditLogPath(), appLogPath(), accessLogPath()} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 {
			t.Errorf("%s is empty; the check would prove nothing", filepath.Base(path))
		}
		for what, s := range secrets {
			if strings.Contains(string(b), s) {
				t.Errorf("%s contains the %s", filepath.Base(path), what)
			}
		}
	}
}
