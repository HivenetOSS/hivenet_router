// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"hivenet_router/internal/domain"
)

// LlamaCPPEngine implements Engine for llama.cpp inference servers.
//
// llama.cpp exposes a fully OpenAI-compatible REST API:
//
//	Health check:    GET /health
//	Model discovery: GET /v1/models  (returns the alias set via -a at launch)
//	Chat forwarding: POST /v1/chat/completions
//	Prometheus:      GET /metrics    (requires --metrics at launch)
//
// Launch example:
//
//	./llama-server -m model.gguf -a "ModelAlias" --host 0.0.0.0 --port 8888 --metrics
type LlamaCPPEngine struct{}

func (e *LlamaCPPEngine) Name() string { return "llamacpp" }

func (e *LlamaCPPEngine) WaitForReady(_ context.Context, baseURL string, httpClient *http.Client) error {
	resp, err := httpClient.Get(baseURL + "/health")
	if err != nil {
		return fmt.Errorf("cannot reach llama.cpp server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("llama.cpp health check failed: status %d", resp.StatusCode)
	}
	return nil
}

func (e *LlamaCPPEngine) DiscoverModels(_ context.Context, baseURL string, httpClient *http.Client) ([]string, error) {
	resp, err := httpClient.Get(baseURL + "/v1/models")
	if err != nil {
		return nil, fmt.Errorf("failed to query llama.cpp models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llama.cpp models endpoint returned status %d", resp.StatusCode)
	}

	var modelsResp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return nil, fmt.Errorf("failed to decode llama.cpp models response: %w", err)
	}

	if len(modelsResp.Data) == 0 {
		return nil, fmt.Errorf("no models found on llama.cpp instance")
	}

	models := make([]string, len(modelsResp.Data))
	for i, m := range modelsResp.Data {
		models[i] = m.ID
	}
	return models, nil
}

// ScrapeMetrics implements MetricsProvider by fetching llama.cpp's /metrics endpoint.
// Requires --metrics at launch. A 5-second timeout is applied via the caller-supplied
// context. Scrape failures are non-fatal: the heartbeat proceeds without backend metrics.
//
// Reuses parsePrometheusTextFull, histogramQuantile, histogramAvg, and metricVal from
// engine_vllm.go (same package). PreemptionsTotal is always nil — llama.cpp has no
// preemption mechanism.
func (e *LlamaCPPEngine) ScrapeMetrics(ctx context.Context, baseURL string, httpClient *http.Client) (*domain.BackendMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("llamacpp ScrapeMetrics: build request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("llamacpp ScrapeMetrics: GET /metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llamacpp ScrapeMetrics: /metrics returned HTTP %d", resp.StatusCode)
	}

	scalars, buckets, _ := parsePrometheusTextFull(resp.Body)

	m := &domain.BackendMetrics{}
	m.KVCacheUtilization = metricVal(scalars, "llamacpp:kv_cache_usage_ratio")
	m.RunningRequests = metricVal(scalars, "llamacpp:requests_processing")
	m.WaitingRequests = metricVal(scalars, "llamacpp:requests_deferred")
	// PreemptionsTotal left nil — llama.cpp has no preemption mechanism.
	m.AvgTTFTSeconds = histogramAvg(scalars,
		"llamacpp:time_to_first_token_seconds_sum",
		"llamacpp:time_to_first_token_seconds_count")
	m.P90TTFTSeconds = histogramQuantile(buckets["llamacpp:time_to_first_token_seconds"], 0.9)
	m.AvgITLSeconds = histogramAvg(scalars,
		"llamacpp:time_per_output_token_seconds_sum",
		"llamacpp:time_per_output_token_seconds_count")
	m.P90ITLSeconds = histogramQuantile(buckets["llamacpp:time_per_output_token_seconds"], 0.9)
	// Token throughput gauges — llama.cpp only.
	m.PredictedTokensPerSecond = metricVal(scalars, "llamacpp:predicted_tokens_seconds")
	m.PromptTokensPerSecond = metricVal(scalars, "llamacpp:prompt_tokens_seconds")
	return m, nil
}

func (e *LlamaCPPEngine) ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error) {
	return forwardOpenAIChatCompletion(ctx, baseURL, httpClient, model, req.RawBytes, e.Name(), options...)
}
