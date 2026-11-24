// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/domain"
)

// startPoolingBackend answers embeddings with usage.prompt_tokens =
// embedTokens and reranks the way SGLang does: an array whose results carry
// meta_info.prompt_tokens, summing to rerankTokens.
func startPoolingBackend(t *testing.T, embedTokens, rerankTokens int, models ...string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		data := make([]string, len(models))
		for i, m := range models {
			data[i] = fmt.Sprintf(`{"id":%q,"object":"model"}`, m)
		}
		fmt.Fprintf(w, `{"object":"list","data":[%s]}`, strings.Join(data, ","))
	})
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"object":"list","data":[{"index":0,"embedding":[0.1]}],"usage":{"prompt_tokens":%d,"total_tokens":%d}}`, embedTokens, embedTokens)
	})
	mux.HandleFunc("/v1/rerank", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `[{"score":0.9,"index":0,"meta_info":{"prompt_tokens":%d}},{"score":0.1,"index":1,"meta_info":{"prompt_tokens":%d}}]`,
			rerankTokens/2, rerankTokens-rerankTokens/2)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// postPooling sends an embeddings or rerank request and returns the status,
// the remaining daily tokens the router reports, and the error code if any.
func postPooling(t *testing.T, base, key, path, body string) (status, remaining int, code string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var env domain.RouterErrorResponse
	if json.Unmarshal(raw, &env) == nil && env.Error != nil {
		code = string(env.Error.Code)
	}
	remaining = -1
	if h := resp.Header.Get("X-RateLimit-Remaining-Tokens"); h != "" {
		remaining, _ = strconv.Atoi(h)
	}
	return resp.StatusCode, remaining, code
}

// TestPoolingMetering_DailyBudget: embeddings and rerank spend the key's daily
// token budget by the input tokens the backend reports (an SGLang rerank's
// per-result counts summed). The request that runs the budget out is still
// answered; the next one is refused with 429 token_limit_exceeded.
func TestPoolingMetering_DailyBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	const key = "sk-pooling-metering-key-0123456789"
	base, rcfg := startRouterWith(t, routerOpts{
		AuthYAML: writeAuthYAML(testKey{Token: key, Owner: "t", Name: "k", RPM: 1000, TPD: 1000}),
		AdminKey: "admin-pooling-metering-0123456789abcdef",
	})
	backend := startPoolingBackend(t, 400, 300, "embed-m", "rerank-m")
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "embed-m", domain.CapabilityEmbedding)
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "rerank-m", domain.CapabilityReranker)
	for _, m := range []string{"embed-m", "rerank-m"} {
		waitFor(t, 30*time.Second, m+" registered and healthy", func() bool { return modelHealthyAs(base, key, m) })
	}

	embed := `{"model":"embed-m","input":"hello world"}`
	rerank := `{"model":"rerank-m","query":"q","documents":["a","b"]}`
	for i, step := range []struct {
		path, body string
		status     int
		remaining  int
		code       string
	}{
		{"/v1/embeddings", embed, http.StatusOK, 600, ""},
		{"/v1/rerank", rerank, http.StatusOK, 300, ""},
		{"/v1/embeddings", embed, http.StatusOK, 0, ""}, // 400 > 300 left: answered, budget spent
		{"/v1/embeddings", embed, http.StatusTooManyRequests, 0, string(domain.ErrCodeTokenLimitExceeded)},
	} {
		status, remaining, code := postPooling(t, base, key, step.path, step.body)
		if status != step.status || remaining != step.remaining || code != step.code {
			t.Errorf("step %d %s: status %d remaining %d code %q; want %d %d %q",
				i+1, step.path, status, remaining, code, step.status, step.remaining, step.code)
		}
	}
}

// TestPoolingMetering_InfinityEstimated: Infinity reports characters in
// usage, so the router keeps its own token estimate for an Infinity agent
// instead of charging the character count.
func TestPoolingMetering_InfinityEstimated(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	const key = "sk-pooling-infinity-key-0123456789"
	base, rcfg := startRouterWith(t, routerOpts{
		AuthYAML: writeAuthYAML(testKey{Token: key, Owner: "t", Name: "k", RPM: 1000, TPD: 1000}),
		AdminKey: "admin-pooling-infinity-0123456789abcdef",
	})
	backend := startPoolingBackend(t, 900, 0, "embed-inf") // 900 "tokens" = characters
	startEngineAgent(t, rcfg, backend.URL, &agent.InfinityEngine{}, "embed-inf", domain.CapabilityEmbedding)
	waitFor(t, 30*time.Second, "embed-inf registered and healthy", func() bool { return modelHealthyAs(base, key, "embed-inf") })

	status, remaining, _ := postPooling(t, base, key, "/v1/embeddings", `{"model":"embed-inf","input":"hello world"}`)
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	// The estimate of 11 bytes plus the special tokens is far below 900.
	if remaining < 950 || remaining >= 1000 {
		t.Errorf("remaining %d: want the estimate charged (950..999), not Infinity's 900 characters", remaining)
	}
}
