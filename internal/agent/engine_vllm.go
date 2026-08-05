// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"hivenet_router/internal/domain"
)

// VLLMEngine implements Engine for vLLM inference servers.
//
// Health check: GET /health
// Model discovery: GET /v1/models
// Request forwarding: POST /v1/chat/completions
type VLLMEngine struct{}

func (e *VLLMEngine) Name() string { return "vllm" }

func (e *VLLMEngine) WaitForReady(_ context.Context, baseURL string, httpClient *http.Client) error {
	resp, err := httpClient.Get(baseURL + "/health")
	if err != nil {
		return fmt.Errorf("cannot reach vLLM server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("vLLM health check failed: status %d", resp.StatusCode)
	}
	return nil
}

func (e *VLLMEngine) DiscoverModels(_ context.Context, baseURL string, httpClient *http.Client) ([]string, error) {
	resp, err := httpClient.Get(baseURL + "/v1/models")
	if err != nil {
		return nil, fmt.Errorf("failed to query vLLM models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vLLM models endpoint returned status %d", resp.StatusCode)
	}

	var modelsResp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return nil, fmt.Errorf("failed to decode vLLM models response: %w", err)
	}

	if len(modelsResp.Data) == 0 {
		return nil, fmt.Errorf("no models found on vLLM instance")
	}

	models := make([]string, len(modelsResp.Data))
	for i, m := range modelsResp.Data {
		models[i] = m.ID
	}
	return models, nil
}

// ScrapeMetrics implements MetricsProvider by fetching vLLM's /metrics endpoint
// and extracting routing-relevant metrics. A 5-second timeout is applied via the
// caller-supplied context. Scrape failures are non-fatal: the returned error is
// logged by the poller and the heartbeat proceeds without backend metrics.
//
// Parsing uses a simple line scanner + bucket collector to avoid the
// prometheus/common v0.60+ global model.NameValidationScheme requirement.
func (e *VLLMEngine) ScrapeMetrics(ctx context.Context, baseURL string, httpClient *http.Client) (*domain.BackendMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("vllm ScrapeMetrics: build request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vllm ScrapeMetrics: GET /metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vllm ScrapeMetrics: /metrics returned HTTP %d", resp.StatusCode)
	}

	scalars, buckets, finishReasons := parsePrometheusTextFull(resp.Body)

	m := &domain.BackendMetrics{}
	m.KVCacheUtilization = metricVal(scalars, "vllm:kv_cache_usage_perc")
	m.RunningRequests = metricVal(scalars, "vllm:num_requests_running")
	m.WaitingRequests = metricVal(scalars, "vllm:num_requests_waiting")
	m.PreemptionsTotal = metricVal(scalars, "vllm:num_preemptions_total")

	// TTFT: keep scalar avg + p90 for routing-policy evaluator (reads in-process)
	// and ship the raw histogram snapshot for correct fleet-wide quantiles + heatmap.
	ttftBuckets := buckets["vllm:time_to_first_token_seconds"]
	ttftSum := metricVal(scalars, "vllm:time_to_first_token_seconds_sum")
	ttftCount := metricVal(scalars, "vllm:time_to_first_token_seconds_count")
	m.AvgTTFTSeconds = histogramAvg(scalars,
		"vllm:time_to_first_token_seconds_sum",
		"vllm:time_to_first_token_seconds_count")
	m.P90TTFTSeconds = histogramQuantile(ttftBuckets, 0.9)
	m.TTFTHistogram = bucketsToSnapshot(ttftBuckets, ttftSum, ttftCount)

	// ITL: same pattern.
	itlBuckets := buckets["vllm:inter_token_latency_seconds"]
	itlSum := metricVal(scalars, "vllm:inter_token_latency_seconds_sum")
	itlCount := metricVal(scalars, "vllm:inter_token_latency_seconds_count")
	m.AvgITLSeconds = histogramAvg(scalars,
		"vllm:inter_token_latency_seconds_sum",
		"vllm:inter_token_latency_seconds_count")
	m.P90ITLSeconds = histogramQuantile(itlBuckets, 0.9)
	m.ITLHistogram = bucketsToSnapshot(itlBuckets, itlSum, itlCount)

	// Per-request prompt and generation length distributions — drive the
	// workload-shape heatmaps on the Inference Engine dashboard.
	promptSum := metricVal(scalars, "vllm:request_prompt_tokens_sum")
	promptCount := metricVal(scalars, "vllm:request_prompt_tokens_count")
	m.PromptTokensHistogram = bucketsToSnapshot(buckets["vllm:request_prompt_tokens"], promptSum, promptCount)

	genSum := metricVal(scalars, "vllm:request_generation_tokens_sum")
	genCount := metricVal(scalars, "vllm:request_generation_tokens_count")
	m.GenerationTokensHistogram = bucketsToSnapshot(buckets["vllm:request_generation_tokens"], genSum, genCount)

	// Finish-reason breakdown. parsePrometheusTextFull captures this from
	// vllm:request_success_total{finished_reason=...} label variants — the
	// default scalar map only retains one series per metric name, so a
	// dedicated return is required.
	if len(finishReasons) > 0 {
		m.FinishReasonCounts = finishReasons
	}

	return m, nil
}

// bucketsToSnapshot converts sorted bucketPoints (as returned by
// parsePrometheusTextFull) into a HistogramSnapshot suitable for re-export
// via prometheus.NewConstHistogram.
//
// The +Inf catch-all bucket is deliberately dropped from Buckets: its
// cumulative count is identical to HistogramSnapshot.Count (already captured
// on the struct), and an Le value of math.Inf(1) cannot be round-tripped
// through encoding/json, which would break the heartbeat payload marshal.
// The router's collector treats +Inf as implicit via the Count parameter of
// NewConstHistogram, so dropping it here is lossless.
//
// Returns nil when the histogram carries no observations (count missing or
// zero) — matches the existing "nil = metric unavailable" convention so the
// router leaves any prior series unchanged.
func bucketsToSnapshot(pts []bucketPoint, sum, count *float64) *domain.HistogramSnapshot {
	if len(pts) == 0 || count == nil || *count == 0 {
		return nil
	}
	snap := &domain.HistogramSnapshot{
		Buckets: make([]domain.HistogramBucket, 0, len(pts)),
		Count:   uint64(*count),
	}
	if sum != nil && !math.IsInf(*sum, 0) && !math.IsNaN(*sum) {
		snap.Sum = *sum
	}
	for _, p := range pts {
		if math.IsInf(p.le, 1) {
			continue
		}
		snap.Buckets = append(snap.Buckets, domain.HistogramBucket{
			Le:    p.le,
			Count: uint64(p.count),
		})
	}
	return snap
}

// bucketPoint is a single bucket observation from a Prometheus histogram.
// le is the upper bound (math.Inf(1) for the +Inf catch-all bucket) and
// count is the cumulative observation count up to that bound.
type bucketPoint struct {
	le    float64
	count float64
}

// parsePrometheusTextFull reads a Prometheus text-format exposition body and returns:
//   - scalars: metric name → first observed float value
//   - buckets: histogram base name → cumulative bucket slice sorted by le ascending
//   - finishReasons: vllm:request_success_total finished_reason label value → cumulative count
//
// Lines that are skipped: comment lines (#), parse errors, and lines whose metric
// value is NaN or ±Inf (math.IsInf sign=0 checks both directions). Note that
// histogram le="+Inf" bucket lines are NOT skipped — their value is a finite count
// and they are collected normally via the bucket path.
//
// Lines with the _bucket suffix are collected into buckets keyed by the base
// name (without _bucket). vllm:request_success_total lines are collected into
// finishReasons keyed by the finished_reason label (the default scalar map
// uses first-occurrence semantics, so it cannot hold multiple label variants).
// All other lines go into scalars with first-occurrence semantics. A 1 MB
// scanner buffer is used — enough for any /metrics response.
func parsePrometheusTextFull(r io.Reader) (
	scalars map[string]float64,
	buckets map[string][]bucketPoint,
	finishReasons map[string]uint64,
) {
	scalars = make(map[string]float64)
	buckets = make(map[string][]bucketPoint)
	finishReasons = make(map[string]uint64)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}

		// Extract metric name — ends at '{' (label set) or ' ' (no labels).
		nameEnd := strings.IndexAny(line, "{ ")
		if nameEnd <= 0 {
			continue
		}
		name := line[:nameEnd]
		rest := line[nameEnd:]

		// Capture label set and skip past it.
		var labelSet string
		if rest[0] == '{' {
			close := strings.Index(rest, "} ")
			if close < 0 {
				continue
			}
			labelSet = rest[1:close]
			rest = rest[close+2:]
		} else {
			rest = rest[1:] // strip leading space (no-label metric)
		}

		// Value is the first whitespace-delimited token; optional timestamp follows.
		if i := strings.IndexByte(rest, ' '); i >= 0 {
			rest = rest[:i]
		}

		v, err := strconv.ParseFloat(rest, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}

		// Histogram buckets: collect all le values under the base metric name.
		if strings.HasSuffix(name, "_bucket") {
			le, ok := extractLe(labelSet)
			if !ok {
				continue
			}
			base := name[:len(name)-len("_bucket")]
			buckets[base] = append(buckets[base], bucketPoint{le: le, count: v})
			continue
		}

		// Finish-reason counter: one series per reason variant, collected into
		// a dedicated map so all variants are preserved (scalar first-occurrence
		// would drop all but the first).
		if name == "vllm:request_success_total" {
			if reason, ok := extractFinishedReason(labelSet); ok {
				if v >= 0 {
					finishReasons[reason] = uint64(v)
				}
				continue
			}
		}

		// Scalar metric: keep only the first occurrence per name.
		if _, seen := scalars[name]; !seen {
			scalars[name] = v
		}
	}

	// Sort each bucket slice by le ascending so histogramQuantile can interpolate.
	for base := range buckets {
		pts := buckets[base]
		sort.Slice(pts, func(i, j int) bool { return pts[i].le < pts[j].le })
		buckets[base] = pts
	}
	return
}

// extractFinishedReason extracts the finished_reason label value from a
// Prometheus label set (e.g. `model="foo",finished_reason="stop"`). Returns
// ("", false) when the label is absent.
func extractFinishedReason(labelSet string) (string, bool) {
	const prefix = `finished_reason="`
	idx := strings.Index(labelSet, prefix)
	if idx < 0 {
		return "", false
	}
	start := idx + len(prefix)
	end := strings.Index(labelSet[start:], `"`)
	if end < 0 {
		return "", false
	}
	return labelSet[start : start+end], true
}

// extractLe extracts the le label value from a Prometheus histogram bucket label set.
// Returns (math.Inf(1), true) for le="+Inf". Returns (0, false) when le is absent.
func extractLe(labelSet string) (float64, bool) {
	const prefix = `le="`
	idx := strings.Index(labelSet, prefix)
	if idx < 0 {
		return 0, false
	}
	start := idx + len(prefix)
	end := strings.Index(labelSet[start:], `"`)
	if end < 0 {
		return 0, false
	}
	leStr := labelSet[start : start+end]
	if leStr == "+Inf" {
		return math.Inf(1), true
	}
	v, err := strconv.ParseFloat(leStr, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// histogramQuantile estimates the quantile q ∈ (0,1] from a sorted bucket slice
// as returned by parsePrometheusTextFull. The slice must include the +Inf bucket
// which provides the total observation count. Uses linear interpolation within
// each bucket, matching the semantics of PromQL histogram_quantile().
// Returns nil when data is unavailable or the total observation count is zero.
func histogramQuantile(pts []bucketPoint, q float64) *float64 {
	if len(pts) == 0 {
		return nil
	}
	// The +Inf bucket (last after sorting) holds the total count.
	total := pts[len(pts)-1].count
	if total == 0 {
		return nil
	}
	rank := q * total

	var prevLe, prevCount float64
	for _, p := range pts {
		if math.IsInf(p.le, 1) {
			break
		}
		if p.count >= rank {
			if p.count == prevCount {
				result := prevLe
				return &result
			}
			result := prevLe + (p.le-prevLe)*(rank-prevCount)/(p.count-prevCount)
			return &result
		}
		prevLe = p.le
		prevCount = p.count
	}
	// Rank falls beyond the last finite bucket; return its upper bound.
	for i := len(pts) - 2; i >= 0; i-- {
		if !math.IsInf(pts[i].le, 1) {
			result := pts[i].le
			return &result
		}
	}
	return nil
}

// metricVal returns a pointer to the value for name, or nil if absent.
func metricVal(vals map[string]float64, name string) *float64 {
	v, ok := vals[name]
	if !ok {
		return nil
	}
	return &v
}

// histogramAvg computes sum/count for a Prometheus histogram.
// Returns nil when either series is absent or count is zero (no requests yet).
func histogramAvg(vals map[string]float64, sumName, countName string) *float64 {
	sum, okS := vals[sumName]
	count, okC := vals[countName]
	if !okS || !okC || count == 0 {
		return nil
	}
	avg := sum / count
	return &avg
}

func (e *VLLMEngine) ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error) {
	return forwardOpenAIChatCompletion(ctx, baseURL, httpClient, model, req.RawBytes, e.Name(), options...)
}
