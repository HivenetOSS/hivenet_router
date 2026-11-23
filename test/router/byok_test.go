// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/provider"
	"hivenet_router/internal/router"
)

// byokProvider is an OpenAI-compatible endpoint that records the bearer it got.
func byokProvider(t *testing.T, gotAuth *atomic.Value, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "x", "object": "chat.completion", "model": "gpt-client",
			"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "hi"}, "finish_reason": "stop"}},
			"usage":   map[string]any{"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// When every local step is exhausted, a key with an external fallback is
// served by its own provider account, with its own credential — before the
// policy's (deprecated) fallback_provider — and never when the request is
// bound to a scope.
func TestBYOK_Fallback(t *testing.T) {
	var gotAuth atomic.Value
	var calls atomic.Int32
	srv := byokProvider(t, &gotAuth, &calls)

	base := srv.URL
	run := func(t *testing.T, scope domain.Scope, fb *domain.ExternalFallback, edit ...func(*domain.PendingRequest)) (*domain.ChatResponse, int32, error) {
		lister := &stubAgentLister{agents: map[string][]*domain.Agent{}}
		stor := &stubStorage{}
		m := metrics.NewRouterMetrics()
		counters := metrics.NewUniversalCounterStore(stor, m)
		pol := &policy.Policy{
			RoutingPolicy:    policy.PolicyStep{Strategy: "least-loaded", MaxTries: 1},
			FallbackProvider: &policy.FallbackProvider{Engine: "openai", Model: "gpt-operator"},
		}
		exec := policy.NewExecutor(lister, policy.NewEvaluator(stor, counters), pol, 3, 0)
		operator := &fakeProvider{}
		queue := make(chan *domain.PendingRequest, 1)
		p := router.NewRequestProcessor(queue, exec, nil, m, counters, 10, map[string]provider.Provider{"openai": operator}, nil,
			router.WithProviderBaseURLs(map[string]string{"openai": base}))
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go p.Start(ctx)
		pending := domain.NewPendingRequest("req", &domain.ChatRequest{Model: testModel, Messages: []domain.ChatCompletionMessage{{Role: "user", Content: "hi"}}}, time.Minute)
		pending.Capability = domain.CapabilityLLM
		pending.Ctx = context.Background()
		pending.Scope = scope
		pending.ExternalFallback = fb
		for _, e := range edit {
			e(pending)
		}
		queue <- pending
		select {
		case r := <-pending.Response:
			return r, operator.calls.Load(), nil
		case err := <-pending.Error:
			return nil, operator.calls.Load(), err
		case <-time.After(5 * time.Second):
			t.Fatal("no answer")
		}
		return nil, 0, nil
	}
	byok := &domain.ExternalFallback{Provider: "openai", Model: "gpt-client", APIKey: domain.NewSecret("sk-client-0123456789")}

	t.Run("BYOK first, with the client's key", func(t *testing.T) {
		calls.Store(0)
		resp, op, err := run(t, "", byok)
		if err != nil || resp == nil || resp.ProcessedBy != "byok:openai" || calls.Load() != 1 || op != 0 {
			t.Fatalf("resp %+v err %v byok calls %d operator calls %d", resp, err, calls.Load(), op)
		}
		if a, _ := gotAuth.Load().(string); a != "Bearer sk-client-0123456789" {
			t.Errorf("provider got Authorization %q, want the client's key", a)
		}
		if !resp.ServedByProvider() {
			t.Error("a BYOK response must count as served off-box")
		}
		var body struct {
			Choices []struct {
				Message struct{ Content string } `json:"message"`
			} `json:"choices"`
			ProcessedBy string `json:"processed_by"`
		}
		if err := json.Unmarshal(resp.RawBytes, &body); err != nil || len(body.Choices) != 1 || body.Choices[0].Message.Content != "hi" || body.ProcessedBy != "" {
			t.Errorf("body %s: want the provider's answer as OpenAI JSON, without processed_by", resp.RawBytes)
		}
	})
	t.Run("scope all still allows it", func(t *testing.T) {
		calls.Store(0)
		if resp, _, err := run(t, domain.ScopeAll, byok); err != nil || resp.ProcessedBy != "byok:openai" {
			t.Fatalf("resp %+v err %v", resp, err)
		}
	})
	t.Run("a scoped request never leaves: no BYOK, no operator provider", func(t *testing.T) {
		calls.Store(0)
		resp, op, err := run(t, "country:FR", byok)
		if err == nil || resp != nil || calls.Load() != 0 || op != 0 {
			t.Fatalf("resp %+v err %v byok calls %d operator calls %d, want an error and no provider call", resp, err, calls.Load(), op)
		}
	})
	t.Run("no BYOK: the deprecated policy fallback_provider still applies", func(t *testing.T) {
		calls.Store(0)
		resp, op, err := run(t, "", nil)
		if err != nil || resp.ProcessedBy != "provider:openai" || op != 1 || calls.Load() != 0 {
			t.Fatalf("resp %+v err %v operator calls %d byok calls %d", resp, err, op, calls.Load())
		}
		if len(resp.RawBytes) == 0 {
			t.Error("the operator's provider answer must carry a body too")
		}
	})
	// Only a non-streaming chat completion goes out: the providers' clients
	// send and decode only that shape.
	for _, path := range []string{"/v1/messages", "/v1/messages/count_tokens", "/v1/completions"} {
		t.Run("not for "+path, func(t *testing.T) {
			calls.Store(0)
			resp, op, err := run(t, "", byok, func(p *domain.PendingRequest) { p.Path = path })
			if err == nil || resp != nil || calls.Load() != 0 || op != 0 {
				t.Fatalf("resp %+v err %v byok calls %d operator calls %d, want an error and no provider call", resp, err, calls.Load(), op)
			}
		})
	}
	t.Run("chat completions path explicitly", func(t *testing.T) {
		calls.Store(0)
		if resp, _, err := run(t, "", byok, func(p *domain.PendingRequest) { p.Path = "/v1/chat/completions" }); err != nil || resp.ProcessedBy != "byok:openai" {
			t.Fatalf("resp %+v err %v", resp, err)
		}
	})
	t.Run("not for a streaming request, and the error says why", func(t *testing.T) {
		calls.Store(0)
		resp, op, err := run(t, "", byok, func(p *domain.PendingRequest) { p.Request.Stream = true })
		if resp != nil || calls.Load() != 0 || op != 0 {
			t.Fatalf("resp %+v byok calls %d operator calls %d, want no provider call", resp, calls.Load(), op)
		}
		var re *domain.RouterError
		if !errors.As(err, &re) || !strings.Contains(re.Message, "only non-streaming") {
			t.Fatalf("err %v, want a message about non-streaming requests", err)
		}
	})
	t.Run("the provider's explanation reaches the client", func(t *testing.T) {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"Incorrect API key provided","type":"invalid_request_error"}}`))
		}))
		t.Cleanup(bad.Close)
		base = bad.URL
		t.Cleanup(func() { base = srv.URL })
		_, _, err := run(t, "", byok)
		var re *domain.RouterError
		if !errors.As(err, &re) || !strings.Contains(re.Message, "Incorrect API key provided") || re.Source != domain.SourceBackend {
			t.Fatalf("err %v, want the provider's message, from the backend", err)
		}
	})
}
