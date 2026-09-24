// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/test/testutil"

	"github.com/gin-gonic/gin"
)

const aliasDocYAML = `
models: ["auto"]
alias:
  default_route: general
  abstain_below: 0.3
  routes:
    - name: software
      signals:
        - { feature: stack_trace, weight: 1 }
      candidates: [ { model: coder }, { model: chat } ]
    - name: general
      candidates: [ { model: chat }, { model: coder } ]
`

// aliasStorage lists one healthy agent per model for /v1/models.
type aliasStorage struct {
	testutil.NoopStorage
	models []string
}

func (s aliasStorage) ListAgents() ([]*domain.AgentRegistration, error) {
	out := make([]*domain.AgentRegistration, 0, len(s.models))
	for _, m := range s.models {
		out = append(out, &domain.AgentRegistration{PeerID: "p-" + m, Model: m, IsHealthy: true, Capacity: 1,
			Metadata: domain.AgentMetadata{Capability: domain.CapabilityLLM}})
	}
	return out, nil
}

func newAliasHandlers(t *testing.T, healthy map[string]int) *api.Handlers {
	t.Helper()
	exec := policy.NewExecutor(nil, nil, policy.Default(), 3, 0)
	doc, err := policy.LoadModelDocBytes([]byte(aliasDocYAML))
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.SetNamedPolicy("auto", doc); err != nil {
		t.Fatal(err)
	}
	var healthyFn func(string) int
	if healthy != nil {
		healthyFn = func(m string) int { return healthy[m] }
	}
	return api.NewHandlers(aliasStorage{models: []string{"coder", "chat"}}, nil, make(chan *domain.PendingRequest, 1),
		time.Second, exec, nil, nil, nil, nil, nil, nil, nil, nil, nil, healthyFn, nil, nil, nil, nil, nil, nil, nil)
}

// seen captures what the downstream handler observes after AliasMiddleware.
type seen struct {
	called bool
	body   []byte
}

func aliasRouter(h *api.Handlers, auth gin.HandlerFunc, out *seen) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	g := r.Group("/v1", auth, h.AliasMiddleware())
	handler := func(c *gin.Context) {
		out.called = true
		if cb, ok := c.Get(gin.BodyBytesKey); ok {
			out.body, _ = cb.([]byte)
		} else {
			out.body = nil
		}
		c.Status(http.StatusOK)
	}
	for _, p := range []string{"/chat/completions", "/messages", "/messages/count_tokens", "/embeddings"} {
		g.POST(p, handler)
	}
	return r
}

func noAuth(c *gin.Context) { c.Next() }

func allowOnly(models ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("key_id", "k1")
		c.Set("allowed_models", models)
		c.Next()
	}
}

func post(r http.Handler, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func modelOf(t *testing.T, body []byte) string {
	t.Helper()
	var m struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("body %q: %v", body, err)
	}
	return m.Model
}

// TestAliasMiddleware_ConcreteModelUntouched: a concrete model passes through
// with byte-identical body and no routing headers (PRD N1).
func TestAliasMiddleware_ConcreteModelUntouched(t *testing.T) {
	var out seen
	r := aliasRouter(newAliasHandlers(t, nil), noAuth, &out)
	body := `{"model":"coder",  "messages":[{"role":"user","content":"Traceback (most recent call last):"}]}`
	w := post(r, "/v1/chat/completions", body)
	if !out.called || w.Code != http.StatusOK {
		t.Fatalf("handler not reached: %d", w.Code)
	}
	if string(out.body) != body {
		t.Errorf("body changed:\n got %s\nwant %s", out.body, body)
	}
	if w.Header().Get(api.HeaderRoutedModel) != "" {
		t.Error("concrete model must not get routing headers")
	}
}

// TestAliasMiddleware_Resolves: an alias is rewritten to the chosen model,
// every other byte is preserved, and the decision is exposed in headers.
func TestAliasMiddleware_Resolves(t *testing.T) {
	tests := []struct {
		name, path, body, wantModel, wantRoute, wantSource string
	}{
		{"stack trace → software → coder", "/v1/chat/completions",
			`{"messages":[{"role":"user","content":"Traceback (most recent call last):\n  File \"a.py\", line 1"}], "model" : "auto","stream":true}`,
			"coder", "software", "scored"},
		{"plain chat → default → chat", "/v1/chat/completions",
			`{"model":"auto","messages":[{"role":"user","content":"hello"}]}`, "chat", "general", "default"},
		{"anthropic messages", "/v1/messages",
			`{"model":"auto","max_tokens":10,"messages":[{"role":"user","content":"panic: runtime error\ngoroutine 1 [running]:"}]}`,
			"coder", "software", "scored"},
		{"count_tokens uses default route", "/v1/messages/count_tokens",
			`{"model":"auto","messages":[{"role":"user","content":"Traceback (most recent call last):"}]}`,
			"chat", "general", "count"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out seen
			r := aliasRouter(newAliasHandlers(t, nil), noAuth, &out)
			w := post(r, tc.path, tc.body)
			if w.Code != http.StatusOK || !out.called {
				t.Fatalf("status %d body %s", w.Code, w.Body.String())
			}
			if got := modelOf(t, out.body); got != tc.wantModel {
				t.Errorf("forwarded model = %q, want %q", got, tc.wantModel)
			}
			// Only the model value changes.
			if want := strings.Replace(tc.body, `"auto"`, `"`+tc.wantModel+`"`, 1); string(out.body) != want {
				t.Errorf("body not preserved:\n got %s\nwant %s", out.body, want)
			}
			if w.Header().Get(api.HeaderRoutedModel) != tc.wantModel || w.Header().Get(api.HeaderRoute) != tc.wantRoute ||
				w.Header().Get(api.HeaderRouteSource) != tc.wantSource {
				t.Errorf("headers = %v", w.Header())
			}
		})
	}
}

// TestAliasMiddleware_Allowlist: the key's allowlist bounds the choice, and a
// key that may use none of the candidates gets 403.
func TestAliasMiddleware_Allowlist(t *testing.T) {
	body := `{"model":"auto","messages":[{"role":"user","content":"hello"}]}`
	var out seen
	r := aliasRouter(newAliasHandlers(t, nil), allowOnly("coder"), &out)
	if w := post(r, "/v1/chat/completions", body); w.Code != http.StatusOK || modelOf(t, out.body) != "coder" {
		t.Errorf("allowlisted key: status %d model %s", w.Code, out.body)
	}

	out = seen{}
	r = aliasRouter(newAliasHandlers(t, nil), allowOnly("other"), &out)
	w := post(r, "/v1/chat/completions", body)
	if w.Code != http.StatusForbidden || out.called {
		t.Errorf("no allowed candidate: status %d called %v body %s", w.Code, out.called, w.Body.String())
	}
}

// TestAliasMiddleware_NoHealthyCandidate: all candidates down → 503.
func TestAliasMiddleware_NoHealthyCandidate(t *testing.T) {
	var out seen
	r := aliasRouter(newAliasHandlers(t, map[string]int{}), noAuth, &out)
	w := post(r, "/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`)
	if w.Code != http.StatusServiceUnavailable || out.called {
		t.Errorf("status %d called %v", w.Code, out.called)
	}
}

// TestAliasMiddleware_NonChatPathUntouched: aliases apply only to chat paths.
func TestAliasMiddleware_NonChatPathUntouched(t *testing.T) {
	var out seen
	r := aliasRouter(newAliasHandlers(t, nil), noAuth, &out)
	body := `{"model":"auto","input":"x"}`
	post(r, "/v1/embeddings", body)
	if string(out.body) != body {
		t.Errorf("embeddings body changed: %s", out.body)
	}
}

// TestAliasMiddleware_QuotaSeesResolvedModel: with strict per-model quotas the
// quota bucket charged is the resolved model's, and a candidate without a
// quota entry is never chosen (no 429 after the rewrite).
func TestAliasMiddleware_QuotaSeesResolvedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lim := &recLimiter{allowRPM: true, remaining: 10}
	limits := auth.QuotaLimits{PerModel: map[string]auth.PerModelQuotaLimits{"coder": {RequestsPerMinutePerReplica: 10}}}
	h := newAliasHandlers(t, nil)
	r := gin.New()
	var hit bool
	r.POST("/v1/chat/completions", stamp("tenant-a", limits), h.AliasMiddleware(),
		api.QuotaMiddleware(lim, metrics.NewRouterMetrics(), nil), func(c *gin.Context) { hit = true; c.Status(200) })
	// Default route prefers chat, but the key only has quota for coder.
	w := post(r, "/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"hello"}]}`)
	if w.Code != http.StatusOK || !hit {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	if len(lim.requests) != 1 || lim.requests[0].model != "coder" {
		t.Errorf("quota calls = %+v, want one on coder", lim.requests)
	}
}

// TestListModels_IncludesAlias: /v1/models lists the alias for keys that may
// use at least one healthy candidate, and hides it otherwise.
func TestListModels_IncludesAlias(t *testing.T) {
	gin.SetMode(gin.TestMode)
	list := func(authMW gin.HandlerFunc) []string {
		h := newAliasHandlers(t, nil)
		r := gin.New()
		r.GET("/v1/models", authMW, h.ListModels)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
		var resp struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		_ = json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&resp)
		ids := make([]string, 0, len(resp.Data))
		for _, d := range resp.Data {
			ids = append(ids, d.ID)
		}
		return ids
	}
	contains := func(ids []string, id string) bool {
		for _, x := range ids {
			if x == id {
				return true
			}
		}
		return false
	}
	if ids := list(noAuth); !contains(ids, "auto") {
		t.Errorf("unrestricted key should see auto: %v", ids)
	}
	if ids := list(allowOnly("coder")); !contains(ids, "auto") || contains(ids, "chat") {
		t.Errorf("coder-only key: %v", ids)
	}
	if ids := list(allowOnly("other")); contains(ids, "auto") {
		t.Errorf("key without candidates must not see auto: %v", ids)
	}
}

// TestAliasMiddleware_ModelKeyPlacement: the rewrite finds "model" wherever it
// sits, honours escaped key spellings, and with duplicate keys rewrites the
// last one — the one encoding/json (and therefore quota and routing) uses.
func TestAliasMiddleware_ModelKeyPlacement(t *testing.T) {
	tests := []struct{ name, body string }{
		{"model after messages", `{"messages":[{"role":"user","content":"hi \"model\": {x}"}],"stream":false,"model":"auto"}`},
		{"escaped key", `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`},
		{"duplicate keys, alias last", `{"model":"coder","messages":[{"role":"user","content":"hi"}],"model":"auto"}`},
		{"whitespace everywhere", "{ \"model\" :\n \"auto\" ,\t\"messages\" : [ {\"role\":\"user\",\"content\":\"hi\"} ] }"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out seen
			r := aliasRouter(newAliasHandlers(t, nil), noAuth, &out)
			w := post(r, "/v1/chat/completions", tc.body)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			routed := w.Header().Get(api.HeaderRoutedModel)
			if got := modelOf(t, out.body); got != routed || routed == "" {
				t.Errorf("decoded model %q, routed %q; body %s", got, routed, out.body)
			}
			if len(out.body)-len(tc.body) != len(routed)-len("auto") && !strings.Contains(tc.body, `e`) {
				t.Errorf("more than the model value changed: %s", out.body)
			}
		})
	}
}

// BenchmarkAliasMiddleware_Agentic measures everything an alias adds inside
// the router (peek, parse, resolve, body rewrite) on a ~190 KB agentic
// Anthropic body whose "model" key is serialised after "messages".
func BenchmarkAliasMiddleware_Agentic(b *testing.B) {
	tools := make([]map[string]any, 30)
	for i := range tools {
		tools[i] = map[string]any{"name": fmt.Sprintf("Tool%d", i), "input_schema": map[string]any{"type": "object", "description": strings.Repeat("schema text ", 60)}}
	}
	msgs := []map[string]any{{"role": "user", "content": "Refactor the storage layer and add tests"}}
	for len(msgs) < 120 {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "t", "name": "Bash", "input": map[string]any{"cmd": "go test ./..."}}}},
			map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "t", "content": strings.Repeat("ok  \tpkg/x\t0.1s\n", 110)}}})
	}
	body, _ := json.Marshal(map[string]any{"model": "auto", "system": strings.Repeat("You are an agent. ", 1500), "tools": tools, "messages": msgs})
	exec := policy.NewExecutor(nil, nil, policy.Default(), 3, 0)
	doc, err := policy.LoadModelDocBytes([]byte(aliasDocYAML))
	if err != nil {
		b.Fatal(err)
	}
	if err := exec.SetNamedPolicy("auto", doc); err != nil {
		b.Fatal(err)
	}
	h := api.NewHandlers(nil, nil, nil, time.Second, exec, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/messages", h.AliasMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body)))
		if w.Code != http.StatusOK {
			b.Fatalf("status %d", w.Code)
		}
	}
}

// BenchmarkConcreteModelPeek is the baseline a concrete-model request pays in
// the same position (the body read + model peek QuotaMiddleware already did
// before semantic routing existed), for comparison with the alias path.
func BenchmarkConcreteModelPeek(b *testing.B) {
	msgs := []map[string]any{{"role": "user", "content": strings.Repeat("hello ", 30000)}}
	body, _ := json.Marshal(map[string]any{"model": "coder", "messages": msgs})
	h := newAliasHandlers(&testing.T{}, nil)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/chat/completions", h.AliasMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body)))
	}
}
