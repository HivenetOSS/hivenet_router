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

// SGLangEngine implements Engine for SGLang inference servers.
//
// SGLang exposes a fully OpenAI-compatible REST API:
//
//	Health check:    GET /health
//	Model discovery: GET /v1/models
//	Chat forwarding: POST /v1/chat/completions
//
// SGLang-specific endpoints (for future use):
//
//	Detailed health: GET /health_generate  (includes memory stats, KV cache usage)
//	Prometheus:      GET /metrics          (requires --enable-metrics at launch)
//	Native generate: POST /generate        (SGLang low-level generation API)
type SGLangEngine struct{}

func (e *SGLangEngine) Name() string { return "sglang" }

func (e *SGLangEngine) WaitForReady(_ context.Context, baseURL string, httpClient *http.Client) error {
	resp, err := httpClient.Get(baseURL + "/health")
	if err != nil {
		return fmt.Errorf("cannot reach SGLang server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SGLang health check failed: status %d", resp.StatusCode)
	}
	return nil
}

func (e *SGLangEngine) DiscoverModels(_ context.Context, baseURL string, httpClient *http.Client) ([]string, error) {
	resp, err := httpClient.Get(baseURL + "/v1/models")
	if err != nil {
		return nil, fmt.Errorf("failed to query SGLang models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("SGLang models endpoint returned status %d", resp.StatusCode)
	}

	var modelsResp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return nil, fmt.Errorf("failed to decode SGLang models response: %w", err)
	}

	if len(modelsResp.Data) == 0 {
		return nil, fmt.Errorf("no models found on SGLang instance")
	}

	models := make([]string, len(modelsResp.Data))
	for i, m := range modelsResp.Data {
		models[i] = m.ID
	}
	return models, nil
}

// ScrapeMetrics implements MetricsProvider by fetching SGLang's /metrics endpoint
// and extracting routing-relevant metrics. Requires --enable-metrics at SGLang launch.
// A 5-second timeout is applied via the caller-supplied context. Scrape failures are
// non-fatal: the returned error is logged by the poller and the heartbeat proceeds
// without backend metrics.
//
// Reuses parsePrometheusTextFull, histogramQuantile, histogramAvg, and metricVal from
// engine_vllm.go (same package). PreemptionsTotal is always nil — SGLang has no
// preemption mechanism.
func (e *SGLangEngine) ScrapeMetrics(ctx context.Context, baseURL string, httpClient *http.Client) (*domain.BackendMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("sglang ScrapeMetrics: build request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sglang ScrapeMetrics: GET /metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sglang ScrapeMetrics: /metrics returned HTTP %d", resp.StatusCode)
	}

	scalars, buckets, _ := parsePrometheusTextFull(resp.Body)

	m := &domain.BackendMetrics{}
	// sglang:token_usage is the fraction of the KV/token cache in use (0.0–1.0).
	m.KVCacheUtilization = metricVal(scalars, "sglang:token_usage")
	m.RunningRequests = metricVal(scalars, "sglang:num_running_reqs")
	// SGLang uses num_queue_reqs for the scheduler waiting queue.
	m.WaitingRequests = metricVal(scalars, "sglang:num_queue_reqs")
	// PreemptionsTotal left nil — SGLang has no preemption mechanism.
	// AvgITLSeconds / P90ITLSeconds left nil — SGLang does not expose an ITL histogram.
	m.AvgTTFTSeconds = histogramAvg(scalars,
		"sglang:time_to_first_token_seconds_sum",
		"sglang:time_to_first_token_seconds_count")
	m.P90TTFTSeconds = histogramQuantile(buckets["sglang:time_to_first_token_seconds"], 0.9)
	return m, nil
}

func (e *SGLangEngine) ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error) {
	return forwardOpenAIChatCompletion(ctx, baseURL, httpClient, model, req.RawBytes, e.Name(), options...)
}
