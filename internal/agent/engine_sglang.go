// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

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
type SGLangEngine struct {
	// decodeMu guards the decode-seconds floor below. ScrapeMetrics derives
	// decode seconds from two sums that SGLang updates at different times, so
	// the agent keeps the value from going down between scrapes.
	decodeMu sync.Mutex
	// decodeSecondsFloor is the last decode-seconds value shipped, and
	// decodeRequestsSeen the num_requests_total it was shipped with. A drop in
	// num_requests_total means the engine restarted, and the floor is reset.
	decodeSecondsFloor float64
	decodeRequestsSeen float64
}

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
// Reuses parsePrometheusTextFull, histogramQuantile, histogramAvg, metricVal,
// metricSum, mergeBucketVariants, and bucketsToSnapshot from engine_vllm.go
// (same package). PreemptionsTotal is always nil — SGLang has no preemption
// mechanism.
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

	scalars, buckets, _, sums := parsePrometheusTextFull(resp.Body)

	m := &domain.BackendMetrics{}
	// sglang:token_usage is the fraction of the KV/token cache in use (0.0–1.0).
	m.KVCacheUtilization = metricVal(scalars, "sglang:token_usage")
	m.KVCapacityTokens = metricVal(scalars, "sglang:max_total_num_tokens")
	m.RunningRequests = metricVal(scalars, "sglang:num_running_reqs")
	// SGLang uses num_queue_reqs for the scheduler waiting queue.
	m.WaitingRequests = metricVal(scalars, "sglang:num_queue_reqs")
	// PreemptionsTotal left nil — SGLang has no preemption mechanism.
	//
	// SGLang splits its latency histograms across is_streaming label variants
	// (streaming and non-streaming requests), so every bound carries one point
	// per variant. Merge the variants at each le, and read the _sum/_count
	// series from the parser's variant sums — the first-occurrence scalar map
	// would hold only the first variant's value.
	ttftPts := mergeBucketVariants(buckets["sglang:time_to_first_token_seconds"])
	m.AvgTTFTSeconds = histogramAvg(sums,
		"sglang:time_to_first_token_seconds_sum",
		"sglang:time_to_first_token_seconds_count")
	m.P90TTFTSeconds = histogramQuantile(ttftPts, 0.9)
	m.TTFTHistogram = bucketsToSnapshot(ttftPts,
		metricSum(sums, "sglang:time_to_first_token_seconds_sum"),
		metricSum(sums, "sglang:time_to_first_token_seconds_count"))

	// ITL: sglang:inter_token_latency_seconds is observed once per generated
	// token, like vllm:inter_token_latency_seconds, so the two engines feed
	// the same distribution. (sglang:request_time_per_output_token_seconds is
	// a per-request average and would mean something else.) It carries no
	// is_streaming label; merging is a no-op then.
	itlPts := mergeBucketVariants(buckets["sglang:inter_token_latency_seconds"])
	m.AvgITLSeconds = histogramAvg(sums,
		"sglang:inter_token_latency_seconds_sum",
		"sglang:inter_token_latency_seconds_count")
	m.P90ITLSeconds = histogramQuantile(itlPts, 0.9)
	m.ITLHistogram = bucketsToSnapshot(itlPts,
		metricSum(sums, "sglang:inter_token_latency_seconds_sum"),
		metricSum(sums, "sglang:inter_token_latency_seconds_count"))

	// Per-request prompt and generation lengths, observed for every finished
	// request. No is_streaming label.
	m.PromptTokensHistogram = bucketsToSnapshot(
		mergeBucketVariants(buckets["sglang:prompt_tokens_histogram"]),
		metricSum(sums, "sglang:prompt_tokens_histogram_sum"),
		metricSum(sums, "sglang:prompt_tokens_histogram_count"))
	m.GenerationTokensHistogram = bucketsToSnapshot(
		mergeBucketVariants(buckets["sglang:generation_tokens_histogram"]),
		metricSum(sums, "sglang:generation_tokens_histogram_sum"),
		metricSum(sums, "sglang:generation_tokens_histogram_count"))

	// Decode-phase counters for the universal output-speed formula. The first
	// token is produced by prefill, so each request contributes one fewer
	// decoded token than the generation total, and its decode time is the e2e
	// latency minus the TTFT. Both are summed across the is_streaming
	// variants via the parser's variant sums.
	if gen := metricSum(sums, "sglang:generation_tokens_total"); gen != nil {
		if reqs := metricSum(sums, "sglang:num_requests_total"); reqs != nil {
			decodeTokens := *gen - *reqs
			if decodeTokens < 0 {
				decodeTokens = 0
			}
			m.DecodeTokensTotal = &decodeTokens
		}
	}
	if e2e := metricSum(sums, "sglang:e2e_request_latency_seconds_sum"); e2e != nil {
		if ttft := metricSum(sums, "sglang:time_to_first_token_seconds_sum"); ttft != nil {
			m.DecodeSecondsTotal = e.monotonicDecodeSeconds(*e2e-*ttft, metricSum(sums, "sglang:num_requests_total"))
		}
	}
	return m, nil
}

// monotonicDecodeSeconds keeps the decode-seconds counter from going down.
//
// SGLang observes a request's TTFT at its first token but its e2e latency only
// when it finishes, so e2e sum − TTFT sum drops whenever a request reaches its
// first token while none finishes. The router exports the value as a
// Prometheus counter, and Prometheus reads any drop as a reset: rate() would
// add the whole value and the measured output speed would collapse. The value
// is therefore held at its highest point until the running requests finish
// and it climbs past it again. It may go down only when the engine restarted,
// which num_requests_total going down reveals.
//
// The result is a lower bound: an aborted request adds its TTFT but never an
// e2e latency, so the decode seconds read slightly low.
func (e *SGLangEngine) monotonicDecodeSeconds(raw float64, requests *float64) *float64 {
	if raw < 0 {
		raw = 0
	}
	e.decodeMu.Lock()
	defer e.decodeMu.Unlock()
	if requests != nil {
		if *requests < e.decodeRequestsSeen {
			e.decodeSecondsFloor = 0 // engine restarted
		}
		e.decodeRequestsSeen = *requests
	}
	if raw < e.decodeSecondsFloor {
		raw = e.decodeSecondsFloor
	}
	e.decodeSecondsFloor = raw
	return &raw
}

func (e *SGLangEngine) ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error) {
	return forwardOpenAIChatCompletion(ctx, baseURL, httpClient, model, req.RawBytes, e.Name(), options...)
}

// Warm runs a first generation: SGLang reports healthy before it has
// compiled the kernels its first request needs, which can take a minute or
// more.
func (e *SGLangEngine) Warm(ctx context.Context, baseURL string, httpClient *http.Client, model string) error {
	return warmChat(ctx, baseURL, httpClient, model)
}

// MessageStartToken implements messageStartProber with SGLang's /v1/tokenize
// (which applies the chat template to messages).
func (e *SGLangEngine) MessageStartToken(ctx context.Context, baseURL string, httpClient *http.Client, model string) (int, error) {
	return probeMessageStart(func(msgs []map[string]string) ([]int, error) {
		return postTokens(ctx, httpClient, baseURL+"/v1/tokenize", map[string]any{"model": model, "messages": msgs})
	})
}

// TokenizeChat implements chatTokenizer through SGLang's /v1/tokenize.
func (e *SGLangEngine) TokenizeChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, body []byte) ([]int, error) {
	return tokenizeChatBody(ctx, httpClient, baseURL+"/v1/tokenize", model, body)
}
