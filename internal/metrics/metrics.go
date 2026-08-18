// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics

import (
	"net/http"
	"strconv"
	"time"

	"hivenet_router/internal/domain"

	logging "github.com/ipfs/go-log/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

var log = logging.Logger("metrics")

// agentLabels are the Prometheus label names carried on every routing-table
// metric that is scoped to a single agent.
var agentLabels = []string{"peer_id", "region", "engine", "model", "capacity", "organization", "machine"}

// RouterMetrics holds all Prometheus collectors for the routing table.
// It is safe for concurrent use after construction.
type RouterMetrics struct {
	// registry is the private Prometheus registry used by this instance.
	// Using a private registry (instead of the global default) allows multiple
	// RouterMetrics instances to coexist in the same process (e.g. in tests)
	// without MustRegister panicking on duplicate metric names.
	registry *prometheus.Registry

	// agentInfo is set to 1 while an agent is registered in the routing table.
	// Labels: peer_id, region, engine, model, capacity.
	agentInfo *prometheus.GaugeVec

	// agentHealthy tracks the health status of each registered agent.
	// Value is 1 when healthy, 0 when the health monitor marks it unhealthy.
	// Labels: peer_id, region, engine, model, capacity.
	agentHealthy *prometheus.GaugeVec

	// agentLastSeen is the Unix timestamp of the last heartbeat received from
	// the agent. Updated on registration and on every successful heartbeat.
	// Labels: peer_id, region, engine, model, capacity.
	agentLastSeen *prometheus.GaugeVec

	// requestsRouted counts every request successfully forwarded to an agent.
	// Labels: region, engine, model (peer_id omitted — selector may pick
	// different agents per request, keeping cardinality bounded).
	requestsRouted *prometheus.CounterVec

	// requestsFailed counts every request that could not be forwarded
	// (no healthy agent, forwarding error, etc.).
	// Labels: region, engine, model.
	requestsFailed *prometheus.CounterVec

	// --- Universal per-agent metrics (Ticket 2) ---
	// Labels: peer_id, model, engine

	// agentSuccessTotal counts lifetime successful requests per agent (HTTP 200).
	agentSuccessTotal *prometheus.CounterVec

	// agentFailedTotal counts lifetime failed requests per agent.
	agentFailedTotal *prometheus.CounterVec

	// agentSuccessRate is the derived success rate (0–1.0) per agent.
	agentSuccessRate *prometheus.GaugeVec

	// agentCapacityUtil is the real-time ActiveRequests/Capacity ratio per agent.
	agentCapacityUtil *prometheus.GaugeVec

	// agentInputTokens counts lifetime prompt tokens consumed per agent.
	agentInputTokens *prometheus.CounterVec

	// agentOutputTokens counts lifetime completion tokens produced per agent.
	agentOutputTokens *prometheus.CounterVec

	// agentRejectedTotal counts times TryAcquireSlot returned false per agent.
	agentRejectedTotal *prometheus.CounterVec

	// agentDisconnections counts lifetime disconnections per agent.
	agentDisconnections *prometheus.CounterVec

	// agentConnectionResets counts stale-connection evictions performed by the
	// dispatcher when a forward fails on a dead libp2p connection. Labels: model, reason.
	agentConnectionResets *prometheus.CounterVec

	// agentFailureTotal counts times the health monitor marked an agent unhealthy
	// due to missed heartbeats (network/process death).
	// Labels: peer_id, model, engine, organization, machine.
	agentFailureTotal *prometheus.CounterVec

	// modelBackendFailureTotal counts times an agent reported its backend
	// (vLLM/SGLang/Ollama/etc.) as unhealthy via heartbeat health check.
	// Labels: peer_id, model, engine, organization, machine.
	modelBackendFailureTotal *prometheus.CounterVec

	// --- Per-tenant billing metrics ---
	// Labels: tenant_id, model

	// tenantInputTokens counts lifetime prompt tokens per tenant.
	tenantInputTokens *prometheus.CounterVec

	// tenantOutputTokens counts lifetime completion tokens per tenant.
	tenantOutputTokens *prometheus.CounterVec

	// tenantRequestSuccess counts successfully served requests per tenant.
	tenantRequestSuccess *prometheus.CounterVec

	// tenantRequestFailed counts failed/rejected requests per tenant.
	tenantRequestFailed *prometheus.CounterVec

	// tenantRateLimited counts requests rejected due to RPM quota per tenant.
	tenantRateLimited *prometheus.CounterVec

	// tenantQuotaRPMLimit exposes the configured RPM limit per tenant (0 = unlimited).
	// Populated only for keys on the legacy flat quota shape. Per-model keys
	// leave this series at its seeded 0 value — see tenantPerModelQuotaRPMLimit
	// for the per-(tenant, model) ceiling.
	tenantQuotaRPMLimit *prometheus.GaugeVec

	// tenantQuotaTPDLimit exposes the configured tokens-per-day limit per tenant
	// (0 = unlimited). Same flat-shape-only semantics as tenantQuotaRPMLimit.
	tenantQuotaTPDLimit *prometheus.GaugeVec

	// tenantPerModelQuotaRPMLimit exposes the effective per-minute RPM ceiling
	// for a (tenant, model) pair. For per-model keys this is
	// requests_per_minute_per_replica × live healthy replicas, refreshed on
	// every admission so the gauge tracks fleet scaling. Unused (and never
	// touched) by the legacy flat path. 0 = unlimited.
	tenantPerModelQuotaRPMLimit *prometheus.GaugeVec

	// tenantPerModelQuotaTPDLimit exposes the configured tokens-per-day budget
	// for a (tenant, model) pair. 0 = unlimited. Unused by the legacy flat path.
	tenantPerModelQuotaTPDLimit *prometheus.GaugeVec

	// tenantTokensUsedToday tracks how many tokens the tenant has consumed so far
	// today (UTC calendar day). Updated on every successful AllowInputTokens /
	// AllowOutputTokens deduction and reset to the new value on UTC midnight rollover.
	// Label: tenant_id.
	tenantTokensUsedToday *prometheus.GaugeVec

	// tenantTokenLimited counts requests rejected due to the daily token budget.
	// Labels: tenant_id, phase (input|output).
	tenantTokenLimited *prometheus.CounterVec

	// tenantRequestDuration is an end-to-end histogram of request duration
	// (from handler entry to response written) per tenant and model.
	// Labels: tenant_id, model.
	tenantRequestDuration *prometheus.HistogramVec

	// requestDuration is an end-to-end histogram of request duration
	// (from handler entry to response written) per tenant, agent, model, and status.
	// Labels: tenant_id, peer_id, model, status_code.
	requestDuration *prometheus.HistogramVec

	// tenantLastRequestTimestamp is the Unix timestamp (seconds) of the most
	// recent request received from the tenant (set at handler entry).
	// Labels: tenant_id.
	tenantLastRequestTimestamp *prometheus.GaugeVec

	// quotaBackendErrors counts Redis quota backend call failures (fail-open events).
	quotaBackendErrors prometheus.Counter

	// --- Admission-control metrics ---

	// admissionRejections counts requests rejected by an admission gate, labelled
	// by the gate that rejected it. reason ∈ {b1, b2, b3, b4_occupancy, b4_itpm,
	// b4_otpm}; the existing per-tenant RPM path reports via tenantRateLimited.
	// Labels: reason, model.
	admissionRejections *prometheus.CounterVec

	// admissionOccupancyTokens is the current weighted in-flight token sum (Σw)
	// for a model — the numerator of the occupancy-vs-budget utilization.
	admissionOccupancyTokens *prometheus.GaugeVec

	// admissionBudgetTokens is the effective occupancy admit budget (admit_fraction
	// × admit_budget_tokens) for a model — the denominator of the utilization.
	admissionBudgetTokens *prometheus.GaugeVec

	// admissionInflightRequests is the current in-flight request count for a model,
	// against the max_inflight backstop. Label: model.
	admissionInflightRequests *prometheus.GaugeVec

	// admissionMaxInflight is the configured max_inflight backstop for a model —
	// the denominator for the concurrency-utilization panel. 0 = no backstop.
	admissionMaxInflight *prometheus.GaugeVec

	// --- Policy routing metrics ---

	// policyPrimaryRouted counts requests served by the primary routing_policy step.
	// Label: model.
	policyPrimaryRouted *prometheus.CounterVec

	// policyFallbackRouted counts requests served by any fallback chain step.
	// Label: model.
	policyFallbackRouted *prometheus.CounterVec

	// policyExhausted counts requests where all policy steps were exhausted
	// and the client received a 503. Label: model.
	policyExhausted *prometheus.CounterVec

	// policyProviderFallbackRouted counts requests served by the closed-source
	// provider fallback (after all local steps were exhausted). Label: model.
	policyProviderFallbackRouted *prometheus.CounterVec

	// --- Per-model wait queue metrics ---

	// queueDepth is the instantaneous number of requests waiting for a
	// concurrency slot per model. Label: model.
	queueDepth *prometheus.GaugeVec

	// queueWaitSeconds is the time each request spends in the per-model wait
	// queue before a slot becomes available and it is dispatched.
	// Label: model.
	queueWaitSeconds *prometheus.HistogramVec

	// policyReloadTotal counts policy reload attempts.
	// Labels: trigger (api|sighup), result (success|error).
	policyReloadTotal *prometheus.CounterVec

	// --- HTTP server RED metrics (Layer 1) ---

	// httpRequestDuration is the per-endpoint request duration histogram.
	// Labels: method, route, status_code.
	httpRequestDuration *prometheus.HistogramVec

	// httpActiveRequests is the number of in-flight HTTP requests.
	// Labels: method, route.
	httpActiveRequests *prometheus.GaugeVec

	// --- HTTP client metrics (Layer 1, provider fallback) ---

	// httpClientDuration is the per-provider outbound call duration histogram.
	// Labels: provider, status_code.
	httpClientDuration *prometheus.HistogramVec

	// agentSRTT is the RFC 6298 smoothed RTT (ms) per agent.
	agentSRTT *prometheus.GaugeVec

	// agentRTTVAR is the RFC 6298 RTT variance (ms) per agent.
	agentRTTVAR *prometheus.GaugeVec

	// --- Hardware metrics (HAI-65) ---
	// GPU labels: peer_id, region, model, engine, gpu_index
	// CPU/memory labels: peer_id, region, model, engine

	// gpuUtil is GPU compute utilization percent per device.
	gpuUtil *prometheus.GaugeVec

	// gpuVRAMUsed is VRAM currently in use per device (bytes).
	gpuVRAMUsed *prometheus.GaugeVec

	// gpuVRAMFree is VRAM available per device (bytes).
	gpuVRAMFree *prometheus.GaugeVec

	// gpuVRAMTotal is total VRAM capacity per device (bytes).
	gpuVRAMTotal *prometheus.GaugeVec

	// gpuTemp is GPU die temperature in Celsius per device.
	gpuTemp *prometheus.GaugeVec

	// gpuPower is GPU power draw in watts per device.
	gpuPower *prometheus.GaugeVec

	// cpuUsage is node-level CPU utilization percent.
	cpuUsage *prometheus.GaugeVec

	// memUsedPct is system memory used as a percentage.
	memUsedPct *prometheus.GaugeVec

	// memAvailable is system memory available in bytes.
	memAvailable *prometheus.GaugeVec

	// memTotal is total system memory in bytes.
	memTotal *prometheus.GaugeVec

	// --- Engine punctual metrics (HAI-60) ---
	// Labels: peer_id, model, engine
	// All pointer-typed in BackendMetrics; nil fields leave the gauge unchanged.

	// engineKVCache is the fraction of GPU KV-cache in use (0.0–1.0).
	// Source: vllm:kv_cache_usage_perc (vLLM) / sglang:token_usage (SGLang).
	engineKVCache *prometheus.GaugeVec

	// engineRunning is the number of requests currently being processed.
	// Source: vllm:num_requests_running (vLLM) / sglang:num_running_reqs (SGLang).
	engineRunning *prometheus.GaugeVec

	// engineWaiting is the number of requests queued in the engine scheduler.
	// Source: vllm:num_requests_waiting (vLLM) / sglang:num_queue_reqs (SGLang).
	engineWaiting *prometheus.GaugeVec

	// enginePreemptions is the cumulative count of requests preempted by the engine.
	// Source: vllm:num_preemptions_total (vLLM only — SGLang has no preemption).
	enginePreemptions *prometheus.GaugeVec

	// engineAvgTTFT is the running average time-to-first-token in seconds.
	// Source: vllm:time_to_first_token_seconds (vLLM) / sglang:time_to_first_token_seconds (SGLang) histogram sum/count.
	engineAvgTTFT *prometheus.GaugeVec

	// engineP90TTFT is the 90th-percentile TTFT. Kept as a scalar gauge
	// alongside the hivenet_agent_engine_ttft_seconds histogram because the
	// Inference Engine dashboard's per-peer status-history panels read it
	// directly (one value per peer, no aggregation needed). For fleet-wide
	// percentiles use histogram_quantile() on the histogram instead.
	engineP90TTFT *prometheus.GaugeVec

	// engineAvgITL is the running average inter-token latency in seconds.
	// Source: vllm:inter_token_latency_seconds (vLLM only — SGLang does not expose an ITL histogram).
	engineAvgITL *prometheus.GaugeVec

	// engineP90ITL is the 90th-percentile ITL. See engineP90TTFT for why
	// this scalar is kept alongside the ITL histogram.
	engineP90ITL *prometheus.GaugeVec

	// engineHistograms re-exports the raw TTFT / ITL / prompt-tokens /
	// generation-tokens histograms from the engine with the router's label
	// set. Implemented as a custom prometheus.Collector because the agent
	// ships pre-aggregated bucket counts, not individual observations.
	engineHistograms *engineHistogramCollector

	// engineFinishReason counts requests completed on the engine, partitioned
	// by their vLLM finished_reason (stop, length, abort, etc.). Incremented
	// per scrape from cumulative-count deltas tracked in engineFinishState.
	engineFinishReason *prometheus.CounterVec

	// engineFinishState tracks last-seen cumulative finish-reason counts per
	// peer so increments can be computed across scrapes and fed to
	// engineFinishReason. Pod restarts are handled as counter resets.
	engineFinishState *engineFinishReasonTracker

	// enginePredictedTPS is the token generation throughput in tokens/sec.
	// Source: llamacpp:predicted_tokens_seconds (llama.cpp only).
	enginePredictedTPS *prometheus.GaugeVec

	// enginePromptTPS is the prompt ingestion throughput in tokens/sec.
	// Source: llamacpp:prompt_tokens_seconds (llama.cpp only).
	enginePromptTPS *prometheus.GaugeVec
}

// NewRouterMetrics creates and registers all routing-table Prometheus collectors.
// Each call returns an independent instance backed by its own private registry,
// so multiple instances can coexist in the same process without panicking.
func NewRouterMetrics() *RouterMetrics {
	m := &RouterMetrics{
		registry: prometheus.NewRegistry(),
		agentInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_routing_agent_info",
				Help: "Constant 1 for each agent currently registered in the routing table.",
			},
			agentLabels,
		),
		agentHealthy: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_routing_agent_healthy",
				Help: "1 if the agent is healthy, 0 if the health monitor has marked it unhealthy.",
			},
			agentLabels,
		),
		agentLastSeen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_routing_agent_last_seen_timestamp",
				Help: "Unix timestamp of the last heartbeat received from the agent.",
			},
			agentLabels,
		),
		requestsRouted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_routing_requests_routed_total",
				Help: "Total number of inference requests successfully forwarded to an agent.",
			},
			[]string{"region", "engine", "model", "tenant_id"},
		),
		requestsFailed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_routing_requests_failed_total",
				Help: "Total number of inference requests that could not be forwarded.",
			},
			[]string{"region", "engine", "model", "tenant_id"},
		),
		// Per-tenant billing metrics
		tenantInputTokens: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_tenant_input_tokens_total",
				Help: "Lifetime prompt tokens per tenant, API key, and deployment.",
			},
			[]string{"tenant_id", "key_id", "deployment_id", "model"},
		),
		tenantOutputTokens: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_tenant_output_tokens_total",
				Help: "Lifetime completion tokens per tenant, API key, and deployment.",
			},
			[]string{"tenant_id", "key_id", "deployment_id", "model"},
		),
		tenantRequestSuccess: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_tenant_requests_success_total",
				Help: "Successfully served requests per tenant, API key, and deployment.",
			},
			[]string{"tenant_id", "key_id", "deployment_id", "model"},
		),
		tenantRequestFailed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_tenant_requests_failed_total",
				Help: "Failed or rejected requests per tenant, API key, and deployment.",
			},
			[]string{"tenant_id", "key_id", "deployment_id", "model"},
		),
		tenantRateLimited: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_tenant_rate_limited_total",
				Help: "Requests rejected due to RPM quota per tenant and API key.",
			},
			[]string{"tenant_id", "key_id"},
		),
		tenantQuotaRPMLimit: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_tenant_quota_rpm_limit",
				Help: "Configured RPM limit per tenant (0 = unlimited).",
			},
			[]string{"tenant_id"},
		),
		tenantQuotaTPDLimit: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_tenant_quota_tpd_limit",
				Help: "Configured tokens-per-day limit per tenant (0 = unlimited). Flat-shape keys only — per-model keys are reported via hivenet_tenant_per_model_quota_tpd_limit.",
			},
			[]string{"tenant_id"},
		),
		tenantPerModelQuotaRPMLimit: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_tenant_per_model_quota_rpm_limit",
				Help: "Effective per-minute RPM ceiling for a (tenant, model) pair on a per-model quota key. For per-replica quotas this is per_replica × live_healthy_replicas, refreshed on admission. 0 = unlimited.",
			},
			[]string{"tenant_id", "model"},
		),
		tenantPerModelQuotaTPDLimit: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_tenant_per_model_quota_tpd_limit",
				Help: "Configured tokens-per-day budget for a (tenant, model) pair on a per-model quota key (0 = unlimited).",
			},
			[]string{"tenant_id", "model"},
		),
		tenantTokensUsedToday: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_tenant_tokens_used_today",
				Help: "Tokens consumed by the tenant so far today (UTC calendar day). Resets at midnight UTC.",
			},
			[]string{"tenant_id"},
		),
		tenantTokenLimited: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_tenant_token_limited_total",
				Help: "Requests rejected due to daily token budget per tenant, API key, and deployment, by phase (input|output).",
			},
			[]string{"tenant_id", "key_id", "deployment_id", "phase"},
		),
		quotaBackendErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "hivenet_quota_backend_errors_total",
				Help: "Redis quota backend call failures (fail-open events).",
			},
		),
		admissionRejections: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_admission_rejections_total",
				Help: "Requests rejected by an admission gate, by gate/reason (b1, b2, b3, b4_occupancy, b4_itpm, b4_otpm) and model.",
			},
			[]string{"reason", "model"},
		),
		admissionOccupancyTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_admission_occupancy_tokens",
				Help: "Current weighted in-flight token sum (Σw) per model — the occupancy-budget numerator.",
			},
			[]string{"model"},
		),
		admissionBudgetTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_admission_budget_tokens",
				Help: "Effective occupancy admit budget (admit_fraction × admit_budget_tokens) per model — the occupancy-budget denominator.",
			},
			[]string{"model"},
		),
		admissionInflightRequests: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_admission_inflight_requests",
				Help: "Current in-flight request count per model, against the max_inflight backstop.",
			},
			[]string{"model"},
		),
		admissionMaxInflight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_admission_max_inflight",
				Help: "Configured max_inflight backstop per model — the concurrency-utilization denominator. 0 = no backstop.",
			},
			[]string{"model"},
		),
		agentSuccessTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_requests_success_total",
				Help: "Lifetime successful requests (HTTP 200) per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentFailedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_requests_failed_total",
				Help: "Lifetime failed requests per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentSuccessRate: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_success_rate",
				Help: "Derived success rate (0–1.0) per agent; recomputed on every write.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentCapacityUtil: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_capacity_utilization",
				Help: "Real-time ActiveRequests/Capacity ratio per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentInputTokens: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_input_tokens_total",
				Help: "Lifetime prompt tokens consumed per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentOutputTokens: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_output_tokens_total",
				Help: "Lifetime completion tokens produced per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentRejectedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_rejected_requests_total",
				Help: "Times TryAcquireSlot returned false (agent at full capacity) per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentDisconnections: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_disconnections_total",
				Help: "Lifetime disconnection count per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentConnectionResets: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_connection_resets_total",
				Help: "Stale-connection evictions performed by the dispatcher on a connection-level forward failure.",
			},
			[]string{"model", "reason"},
		),
		agentSRTT: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_srtt_ms",
				Help: "RFC 6298 smoothed RTT in milliseconds per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentRTTVAR: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_rttvar_ms",
				Help: "RFC 6298 RTT variance in milliseconds per agent.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),

		// Hardware metrics — HAI-65
		gpuUtil: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_gpu_utilization_percent",
				Help: "GPU compute utilization percent per device.",
			},
			[]string{"peer_id", "region", "model", "engine", "gpu_index", "gpu_id", "organization", "machine"},
		),
		gpuVRAMUsed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_gpu_vram_used_bytes",
				Help: "VRAM currently in use per device (bytes).",
			},
			[]string{"peer_id", "region", "model", "engine", "gpu_index", "gpu_id", "organization", "machine"},
		),
		gpuVRAMFree: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_gpu_vram_free_bytes",
				Help: "VRAM available per device (bytes).",
			},
			[]string{"peer_id", "region", "model", "engine", "gpu_index", "gpu_id", "organization", "machine"},
		),
		gpuVRAMTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_gpu_vram_total_bytes",
				Help: "Total VRAM capacity per device (bytes).",
			},
			[]string{"peer_id", "region", "model", "engine", "gpu_index", "gpu_id", "organization", "machine"},
		),
		gpuTemp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_gpu_temperature_celsius",
				Help: "GPU die temperature in Celsius per device.",
			},
			[]string{"peer_id", "region", "model", "engine", "gpu_index", "gpu_id", "organization", "machine"},
		),
		gpuPower: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_gpu_power_watts",
				Help: "GPU power draw in watts per device.",
			},
			[]string{"peer_id", "region", "model", "engine", "gpu_index", "gpu_id", "organization", "machine"},
		),
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_cpu_usage_percent",
				Help: "Node-level CPU utilization percent.",
			},
			[]string{"peer_id", "region", "model", "engine", "organization", "machine"},
		),
		memUsedPct: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_memory_used_percent",
				Help: "System memory used as a percentage.",
			},
			[]string{"peer_id", "region", "model", "engine", "organization", "machine"},
		),
		memAvailable: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_memory_available_bytes",
				Help: "System memory available in bytes.",
			},
			[]string{"peer_id", "region", "model", "engine", "organization", "machine"},
		),
		memTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_memory_total_bytes",
				Help: "Total system memory in bytes.",
			},
			[]string{"peer_id", "region", "model", "engine", "organization", "machine"},
		),

		// Engine punctual metrics — HAI-60
		engineKVCache: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_kv_cache_utilization",
				Help: "Fraction of pre-allocated GPU KV-cache in use (0.0–1.0). Near 1.0 signals imminent preemptions.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_running_requests",
				Help: "Number of requests currently being processed by the inference engine.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineWaiting: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_waiting_requests",
				Help: "Number of requests queued in the engine scheduler. Non-zero means the scheduler is saturated.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		enginePreemptions: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_preemptions_total",
				Help: "Cumulative count of requests preempted by the engine. Use rate() in Prometheus to detect KV-cache thrashing.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineAvgTTFT: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_avg_ttft_seconds",
				Help: "Running average time-to-first-token in seconds (histogram sum/count). Absent when no requests have completed.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineP90TTFT: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_p90_ttft_seconds",
				Help: "P90 time-to-first-token in seconds (histogram quantile). Absent when no requests have completed.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineAvgITL: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_avg_itl_seconds",
				Help: "Running average inter-token latency in seconds (histogram sum/count). Absent when no tokens have been generated.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineP90ITL: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_p90_itl_seconds",
				Help: "P90 inter-token latency in seconds (histogram quantile). Absent when no tokens have been generated.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		engineHistograms: newEngineHistogramCollector(),
		engineFinishReason: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_engine_request_success_total",
				Help: "Requests completed on the inference engine, partitioned by vLLM finished_reason (stop|length|abort|...). Incremented from cumulative-count deltas observed across scrapes; pod restarts are handled as counter resets.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine", "finished_reason"},
		),
		engineFinishState: newEngineFinishReasonTracker(),
		enginePredictedTPS: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_predicted_tps",
				Help: "Token generation throughput in tokens/sec (last-second average). Source: llamacpp:predicted_tokens_seconds.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		enginePromptTPS: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_agent_engine_prompt_tps",
				Help: "Prompt ingestion throughput in tokens/sec (last-second average). Source: llamacpp:prompt_tokens_seconds.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		agentFailureTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_agent_failure_total",
				Help: "Total number of times an agent was marked unhealthy due to missed heartbeats (network/process death).",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		modelBackendFailureTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_model_backend_failure_total",
				Help: "Total number of times an agent reported its inference backend as unhealthy via heartbeat health check.",
			},
			[]string{"peer_id", "model", "engine", "organization", "machine"},
		),
		// HTTP server RED metrics — Layer 1
		httpRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_server_request_duration_seconds",
				Help:    "HTTP request duration in seconds per endpoint (method, route, status_code).",
				Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
			},
			[]string{"method", "route", "status_code"},
		),
		httpActiveRequests: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "http_server_active_requests",
				Help: "Number of in-flight HTTP requests (method, route).",
			},
			[]string{"method", "route"},
		),
		httpClientDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_client_request_duration_seconds",
				Help:    "Outbound HTTP request duration in seconds for provider fallback calls.",
				Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
			},
			[]string{"provider", "status_code"},
		),
		policyPrimaryRouted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_policy_primary_routed_total",
				Help: "Total requests served by the primary routing_policy step.",
			},
			[]string{"model"},
		),
		policyFallbackRouted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_policy_fallback_routed_total",
				Help: "Total requests served by a fallback chain step (primary step was skipped or exhausted).",
			},
			[]string{"model"},
		),
		policyExhausted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_policy_exhausted_total",
				Help: "Total requests where all policy steps were exhausted and the client received a 503.",
			},
			[]string{"model"},
		),
		policyProviderFallbackRouted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_policy_provider_fallback_total",
				Help: "Total requests served by the closed-source provider fallback after all local steps were exhausted.",
			},
			[]string{"model"},
		),
		queueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_queue_depth",
				Help: "Current number of requests waiting for a concurrency slot per model.",
			},
			[]string{"model"},
		),
		queueWaitSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "hivenet_queue_wait_seconds",
				Help:    "Time requests spend waiting in the per-model capacity queue before being dispatched.",
				Buckets: prometheus.ExponentialBuckets(0.01, 2, 13), // 10ms to ~40s (0.01 × 2^12 = 40.96s)
			},
			[]string{"model"},
		),
		tenantRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "hivenet_tenant_request_duration_seconds",
				Help:    "End-to-end request duration in seconds per tenant, API key, deployment, and model (handler entry to response written).",
				Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
			},
			[]string{"tenant_id", "key_id", "deployment_id", "model"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "hivenet_request_duration_seconds",
				Help:    "End-to-end request duration from handler entry to response, labeled by tenant, agent, model, and status.",
				Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
			},
			[]string{"tenant_id", "peer_id", "model", "status_code"},
		),
		tenantLastRequestTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hivenet_tenant_last_request_timestamp",
				Help: "Unix timestamp (seconds) of the most recent request per tenant, API key, and deployment. Use max() to get last-used time per key.",
			},
			[]string{"tenant_id", "key_id", "deployment_id"},
		),
		policyReloadTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "hivenet_policy_reload_total",
				Help: "Total number of routing policy reload attempts, partitioned by trigger (api|sighup) and result (success|error).",
			},
			[]string{"trigger", "result"},
		),
	}

	m.registry.MustRegister(
		m.agentInfo,
		m.agentHealthy,
		m.agentLastSeen,
		m.requestsRouted,
		m.requestsFailed,
		// Tenant billing metrics
		m.tenantInputTokens,
		m.tenantOutputTokens,
		m.tenantRequestSuccess,
		m.tenantRequestFailed,
		m.tenantRateLimited,
		m.tenantQuotaRPMLimit,
		m.tenantQuotaTPDLimit,
		m.tenantPerModelQuotaRPMLimit,
		m.tenantPerModelQuotaTPDLimit,
		m.tenantTokensUsedToday,
		m.tenantTokenLimited,
		m.tenantRequestDuration,
		m.requestDuration,
		m.tenantLastRequestTimestamp,
		m.quotaBackendErrors,
		m.admissionRejections,
		m.admissionOccupancyTokens,
		m.admissionBudgetTokens,
		m.admissionInflightRequests,
		m.admissionMaxInflight,
		m.agentSuccessTotal,
		m.agentFailedTotal,
		m.agentSuccessRate,
		m.agentCapacityUtil,
		m.agentInputTokens,
		m.agentOutputTokens,
		m.agentRejectedTotal,
		m.agentDisconnections,
		m.agentConnectionResets,
		m.agentSRTT,
		m.agentRTTVAR,
		// Hardware metrics
		m.gpuUtil,
		m.gpuVRAMUsed,
		m.gpuVRAMFree,
		m.gpuVRAMTotal,
		m.gpuTemp,
		m.gpuPower,
		m.cpuUsage,
		m.memUsedPct,
		m.memAvailable,
		m.memTotal,
		// Engine punctual metrics
		m.engineKVCache,
		m.engineRunning,
		m.engineWaiting,
		m.enginePreemptions,
		m.engineAvgTTFT,
		m.engineP90TTFT,
		m.engineAvgITL,
		m.engineP90ITL,
		m.engineHistograms,
		m.engineFinishReason,
		m.enginePredictedTPS,
		m.enginePromptTPS,
		// Failure counters
		m.agentFailureTotal,
		m.modelBackendFailureTotal,
		// HTTP server RED metrics
		m.httpRequestDuration,
		m.httpActiveRequests,
		m.httpClientDuration,
		// Policy routing counters
		m.policyPrimaryRouted,
		m.policyFallbackRouted,
		m.policyExhausted,
		m.policyProviderFallbackRouted,
		// Wait queue metrics
		m.queueDepth,
		m.queueWaitSeconds,
		// Policy management
		m.policyReloadTotal,
	)

	// Go runtime metrics: goroutine count, memory, GC, scheduler latency.
	m.registry.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(
			collectors.MetricsGC, collectors.MetricsMemory, collectors.MetricsScheduler,
		),
	))
	// Process metrics: open FDs, resident memory, CPU seconds.
	m.registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return m
}

// AgentRegistered sets the info and healthy gauges to 1 and records the
// current time as last-seen for a newly registered agent.
//
// Counter initialisation: Bootstrap() is called before AgentRegistered() in
// handleAgentRegister. On first registration Bootstrap seeds all CounterVec
// series with the historical baselines loaded from diskDB (so Grafana shows
// lifetime totals immediately). On reconnect within the same router run,
// Bootstrap skips seeding (Prometheus already holds the accumulated values).
// The Add(0) calls below are a safety net that creates the label set in
// Prometheus even when Bootstrap produced zero baselines (brand-new agent).
// Add(0) is a no-op on an already-seeded counter, so order is not sensitive.
//
// Gauge metrics (successRate, SRTT, RTTVAR) are intentionally NOT reset to 0
// here — Bootstrap already pushed the diskDB-restored values via
// AgentUniversalUpdated(). Calling Set(0) would wipe them.
//
// Called from handleAgentRegister.
func (m *RouterMetrics) AgentRegistered(peerID, region, engine, model, capacity, organization, machine string) {
	labels := prometheus.Labels{
		"peer_id":      peerID,
		"region":       region,
		"engine":       engine,
		"model":        model,
		"capacity":     capacity,
		"organization": organization,
		"machine":      machine,
	}
	m.agentInfo.With(labels).Set(1)
	m.agentHealthy.With(labels).Set(1)
	m.agentLastSeen.With(labels).Set(float64(time.Now().UnixMilli()))

	uLabels := prometheus.Labels{"peer_id": peerID, "model": model, "engine": engine, "organization": organization, "machine": machine}
	m.agentSuccessTotal.With(uLabels).Add(0)
	m.agentFailedTotal.With(uLabels).Add(0)
	m.agentInputTokens.With(uLabels).Add(0)
	m.agentOutputTokens.With(uLabels).Add(0)
	m.agentRejectedTotal.With(uLabels).Add(0)
	m.agentDisconnections.With(uLabels).Add(0)
	m.agentFailureTotal.With(uLabels).Add(0)
	m.modelBackendFailureTotal.With(uLabels).Add(0)
}

// AgentUnregistered deletes all Prometheus time series for a fully removed agent.
// This ensures that agents with a new peer ID (e.g. after docker compose down -v)
// do not leave zombie rows in Grafana tables. Historical counters are preserved in
// diskDB and will be reloaded if the agent reconnects with the same peer ID.
func (m *RouterMetrics) AgentUnregistered(peerID, region, engine, model, capacity, organization, machine string) {
	labels := prometheus.Labels{
		"peer_id":      peerID,
		"region":       region,
		"engine":       engine,
		"model":        model,
		"capacity":     capacity,
		"organization": organization,
		"machine":      machine,
	}
	m.agentInfo.Delete(labels)
	m.agentHealthy.Delete(labels)
	m.agentLastSeen.Delete(labels)

	uLabels := prometheus.Labels{"peer_id": peerID, "model": model, "engine": engine, "organization": organization, "machine": machine}
	m.agentSuccessTotal.Delete(uLabels)
	m.agentFailedTotal.Delete(uLabels)
	m.agentSuccessRate.Delete(uLabels)
	m.agentCapacityUtil.Delete(uLabels)
	m.agentInputTokens.Delete(uLabels)
	m.agentOutputTokens.Delete(uLabels)
	m.agentRejectedTotal.Delete(uLabels)
	m.agentDisconnections.Delete(uLabels)
	m.agentSRTT.Delete(uLabels)
	m.agentRTTVAR.Delete(uLabels)
	m.agentFailureTotal.Delete(uLabels)
	m.modelBackendFailureTotal.Delete(uLabels)
}

// SeedAgentFailureCounters seeds the agent and backend failure counters from diskDB history
// on the first registration of a peer after a router restart. This restores lifetime failure
// counts in Prometheus immediately, before any new failures occur.
func (m *RouterMetrics) SeedAgentFailureCounters(peerID, model, engine, organization, machine string, agentFailures, backendFailures int64) {
	labels := prometheus.Labels{"peer_id": peerID, "model": model, "engine": engine, "organization": organization, "machine": machine}
	if agentFailures > 0 {
		m.agentFailureTotal.With(labels).Add(float64(agentFailures))
	}
	if backendFailures > 0 {
		m.modelBackendFailureTotal.With(labels).Add(float64(backendFailures))
	}
}

// AgentHealthUpdated sets the healthy gauge to 1 or 0 and, when healthy,
// updates the last-seen timestamp. Called from handleAgentHeartbeat
// (healthy=true) and healthMonitor (healthy=false).
func (m *RouterMetrics) AgentHealthUpdated(peerID, region, engine, model, capacity, organization, machine string, healthy bool) {
	labels := prometheus.Labels{
		"peer_id":      peerID,
		"region":       region,
		"engine":       engine,
		"model":        model,
		"capacity":     capacity,
		"organization": organization,
		"machine":      machine,
	}
	val := 0.0
	if healthy {
		val = 1.0
		m.agentLastSeen.With(labels).Set(float64(time.Now().UnixMilli()))
	}
	m.agentHealthy.With(labels).Set(val)
}

// AgentFailure increments the agent failure counter when the health monitor marks
// an agent unhealthy due to a missed heartbeat (network/process death).
func (m *RouterMetrics) AgentFailure(peerID, model, engine, organization, machine string) {
	m.agentFailureTotal.WithLabelValues(peerID, model, engine, organization, machine).Inc()
}

// ModelBackendFailure increments the backend failure counter when an agent
// reports its inference backend as unhealthy via the heartbeat payload.
func (m *RouterMetrics) ModelBackendFailure(peerID, model, engine, organization, machine string) {
	m.modelBackendFailureTotal.WithLabelValues(peerID, model, engine, organization, machine).Inc()
}

// RequestRouted increments the routed counter for the given agent dimensions.
// Called from RequestProcessor after a successful forward.
func (m *RouterMetrics) RequestRouted(region, engine, model, tenantID string) {
	m.requestsRouted.With(prometheus.Labels{
		"region":    region,
		"engine":    engine,
		"model":     model,
		"tenant_id": tenantID,
	}).Inc()
}

// RequestFailed increments the failed counter for the given agent dimensions.
// Called from RequestProcessor when no agent is available or forwarding fails.
func (m *RouterMetrics) RequestFailed(region, engine, model, tenantID string) {
	m.requestsFailed.With(prometheus.Labels{
		"region":    region,
		"engine":    engine,
		"model":     model,
		"tenant_id": tenantID,
	}).Inc()
}

// PrimeTenantCounters materialises the tenant success/failure and token
// counters at 0 so a scrape can anchor increase() on the first non-zero
// sample. Without it, a series that first appears at value N (deployment's
// first request lands before the first scrape) is invisible to increase(),
// which then undercounts by N for the lifetime of that series — trivial for
// success/failed (N=1), painful for tokens (N=prompt/completion length).
func (m *RouterMetrics) PrimeTenantCounters(tenantID, keyID, deploymentID, model string) {
	labels := prometheus.Labels{"tenant_id": tenantID, "key_id": keyID, "deployment_id": deploymentID, "model": model}
	m.tenantRequestSuccess.With(labels).Add(0)
	m.tenantRequestFailed.With(labels).Add(0)
	m.tenantInputTokens.With(labels).Add(0)
	m.tenantOutputTokens.With(labels).Add(0)
}

// TenantRequestSucceeded increments the tenant success counter and adds token counts.
func (m *RouterMetrics) TenantRequestSucceeded(tenantID, keyID, deploymentID, model string, inputTokens, outputTokens int) {
	labels := prometheus.Labels{"tenant_id": tenantID, "key_id": keyID, "deployment_id": deploymentID, "model": model}
	m.tenantRequestSuccess.With(labels).Inc()
	if inputTokens > 0 {
		m.tenantInputTokens.With(labels).Add(float64(inputTokens))
	}
	if outputTokens > 0 {
		m.tenantOutputTokens.With(labels).Add(float64(outputTokens))
	}
}

// TenantRequestFailed increments the tenant failure counter.
func (m *RouterMetrics) TenantRequestFailed(tenantID, keyID, deploymentID, model string) {
	m.tenantRequestFailed.With(prometheus.Labels{
		"tenant_id":     tenantID,
		"key_id":        keyID,
		"deployment_id": deploymentID,
		"model":         model,
	}).Inc()
}

// TenantRateLimited increments the rate-limited counter for a tenant.
func (m *RouterMetrics) TenantRateLimited(tenantID, keyID string) {
	m.tenantRateLimited.With(prometheus.Labels{"tenant_id": tenantID, "key_id": keyID}).Inc()
}

// TenantSetQuotaLimit records the configured RPM limit for a tenant.
// This is a gauge — idempotent to call on every authenticated request.
func (m *RouterMetrics) TenantSetQuotaLimit(tenantID string, rpm int) {
	m.tenantQuotaRPMLimit.With(prometheus.Labels{"tenant_id": tenantID}).Set(float64(rpm))
}

// TenantSetTPDLimit records the configured tokens-per-day limit for a tenant.
// This is a gauge — idempotent to call on every authenticated request.
func (m *RouterMetrics) TenantSetTPDLimit(tenantID string, tpd int) {
	m.tenantQuotaTPDLimit.With(prometheus.Labels{"tenant_id": tenantID}).Set(float64(tpd))
}

// TenantSetPerModelQuotaLimit records the effective per-minute RPM ceiling for
// a (tenant, model) pair on a per-model quota key. The value already includes
// the replica multiplication (per_replica × live_healthy_replicas), so the
// gauge reflects the actual ceiling admission would apply at this moment —
// dashboards can compare it against hivenet_tenant_per_model_request_total to
// derive utilization. Idempotent to call on every request; pure overwrite.
func (m *RouterMetrics) TenantSetPerModelQuotaLimit(tenantID, model string, rpm int) {
	m.tenantPerModelQuotaRPMLimit.With(prometheus.Labels{"tenant_id": tenantID, "model": model}).Set(float64(rpm))
}

// TenantSetPerModelTPDLimit records the configured tokens-per-day budget for a
// (tenant, model) pair on a per-model quota key. Idempotent on every request.
func (m *RouterMetrics) TenantSetPerModelTPDLimit(tenantID, model string, tpd int) {
	m.tenantPerModelQuotaTPDLimit.With(prometheus.Labels{"tenant_id": tenantID, "model": model}).Set(float64(tpd))
}

// AdmissionRejected counts a request rejected by an admission gate, labelled by
// the gate/reason and model. reason ∈ {b1, b2, b3, b4_occupancy, b4_itpm,
// b4_otpm}. B3 shed rate is rate(...{reason="b3"}); "429 by reason" sums over all.
func (m *RouterMetrics) AdmissionRejected(reason, model string) {
	m.admissionRejections.With(prometheus.Labels{"reason": reason, "model": model}).Inc()
}

// SetAdmissionOccupancy publishes a model's live occupancy: the weighted
// in-flight token sum (Σw), the in-flight request count, and the effective admit
// budget. Called on every admit/release/grow, so occupancy_tokens / budget_tokens
// tracks real utilization. The budget is always written — including 0 — so a
// model with no token budget (max_inflight-only), or one whose budget was
// disabled on reload, reports the true denominator rather than a stale value.
func (m *RouterMetrics) SetAdmissionOccupancy(model string, sumW int64, count int, budget int64, maxInflight int) {
	m.admissionOccupancyTokens.With(prometheus.Labels{"model": model}).Set(float64(sumW))
	m.admissionInflightRequests.With(prometheus.Labels{"model": model}).Set(float64(count))
	m.admissionBudgetTokens.With(prometheus.Labels{"model": model}).Set(float64(budget))
	m.admissionMaxInflight.With(prometheus.Labels{"model": model}).Set(float64(maxInflight))
}

// Gather returns the current metric families from this instance's registry. It
// exposes the private registry for tests that assert exported metric values.
func (m *RouterMetrics) Gather() ([]*dto.MetricFamily, error) {
	return m.registry.Gather()
}

// TenantSetTokensUsedToday updates the gauge that tracks how many tokens the
// tenant has consumed so far today. Called after every successful token deduction
// in the rate limiter so the value reflects the current dailyBucket.used counter.
// On UTC midnight rollover the first deduction of the new day resets the gauge
// to the post-reset value automatically (the bucket resets before the callback fires).
func (m *RouterMetrics) TenantSetTokensUsedToday(tenantID string, used int) {
	m.tenantTokensUsedToday.With(prometheus.Labels{"tenant_id": tenantID}).Set(float64(used))
}

// TenantTokenLimited increments the token-limited counter for a tenant.
// phase is "input" (pre-check) or "output" (post-check).
func (m *RouterMetrics) TenantTokenLimited(tenantID, keyID, deploymentID, phase string) {
	m.tenantTokenLimited.With(prometheus.Labels{
		"tenant_id":     tenantID,
		"key_id":        keyID,
		"deployment_id": deploymentID,
		"phase":         phase,
	}).Inc()
}

// TenantObserveRequestDuration records the end-to-end duration of a completed
// request (from handler entry to response written).
func (m *RouterMetrics) TenantObserveRequestDuration(tenantID, keyID, deploymentID, model string, seconds float64) {
	m.tenantRequestDuration.With(prometheus.Labels{
		"tenant_id":     tenantID,
		"key_id":        keyID,
		"deployment_id": deploymentID,
		"model":         model,
	}).Observe(seconds)
}

// ObserveRequestDuration records the end-to-end duration of a completed
// request, labeled by tenant, agent, model, and HTTP status code.
func (m *RouterMetrics) ObserveRequestDuration(tenantID, peerID, model, statusCode string, seconds float64) {
	m.requestDuration.With(prometheus.Labels{
		"tenant_id":   tenantID,
		"peer_id":     peerID,
		"model":       model,
		"status_code": statusCode,
	}).Observe(seconds)
}

// TenantSetLastRequestTimestamp records the current time as the most recent
// request received from the tenant.
func (m *RouterMetrics) TenantSetLastRequestTimestamp(tenantID, keyID, deploymentID string) {
	m.tenantLastRequestTimestamp.With(prometheus.Labels{
		"tenant_id":     tenantID,
		"key_id":        keyID,
		"deployment_id": deploymentID,
	}).Set(float64(time.Now().Unix()))
}

// TenantInitCounters previously pre-seeded zero-value counters for a tenant at
// startup. It is now a no-op: the addition of dynamic key_id and deployment_id
// labels makes it impossible to enumerate all combinations in advance.
// Prometheus creates series lazily on first observation.
func (m *RouterMetrics) TenantInitCounters(_ string) {
	// no-op: dynamic key_id/deployment_id labels prevent pre-seeding at startup
}

// QuotaBackendError increments the quota backend error counter.
// Called on every Redis operation failure (fail-open event).
func (m *RouterMetrics) QuotaBackendError() {
	m.quotaBackendErrors.Inc()
}

// PolicyPrimaryRouted increments the counter for requests served by the primary policy step.
func (m *RouterMetrics) PolicyPrimaryRouted(model string) {
	m.policyPrimaryRouted.With(prometheus.Labels{"model": model}).Inc()
}

// PolicyFallbackRouted increments the counter for requests served by a fallback step.
func (m *RouterMetrics) PolicyFallbackRouted(model string) {
	m.policyFallbackRouted.With(prometheus.Labels{"model": model}).Inc()
}

// PolicyExhausted increments the counter for requests where all steps were exhausted (503).
func (m *RouterMetrics) PolicyExhausted(model string) {
	m.policyExhausted.With(prometheus.Labels{"model": model}).Inc()
}

// AgentConnectionReset records that the dispatcher dropped a dead libp2p connection
// to an agent after a connection-level forward failure, so the next attempt re-dials
// a fresh one. reason is the failure category (currently "forward_failure").
func (m *RouterMetrics) AgentConnectionReset(model, reason string) {
	m.agentConnectionResets.With(prometheus.Labels{"model": model, "reason": reason}).Inc()
}

// ResetAgentSeries clears every per-agent lifetime series so a metrics reset starts
// them from zero. Each series reappears at its post-reset value on the next push.
// Only the universalHistory-backed counters/gauges are reset — liveness gauges
// (agent info/health), capacity utilization, and all tenant/billing series are left
// intact. Called by UniversalCounterStore.ResetAll.
func (m *RouterMetrics) ResetAgentSeries() {
	m.agentSuccessTotal.Reset()
	m.agentFailedTotal.Reset()
	m.agentSuccessRate.Reset()
	m.agentInputTokens.Reset()
	m.agentOutputTokens.Reset()
	m.agentRejectedTotal.Reset()
	m.agentDisconnections.Reset()
	m.agentConnectionResets.Reset()
	m.agentSRTT.Reset()
	m.agentRTTVAR.Reset()
	m.agentFailureTotal.Reset()
	m.modelBackendFailureTotal.Reset()
}

// PolicyProviderFallbackRouted increments the counter for requests served by the
// closed-source provider fallback after all local policy steps were exhausted.
func (m *RouterMetrics) PolicyProviderFallbackRouted(model string) {
	m.policyProviderFallbackRouted.With(prometheus.Labels{"model": model}).Inc()
}

// AgentUniversalUpdated refreshes all universal per-agent Prometheus metrics.
// Called by UniversalCounterStore after every counter mutation.
// successDelta and failedDelta are the increments since the last call (0 or 1).
// inputDelta and outputDelta are token increments.
// rejectedDelta and disconnDelta are increments for rejected/disconnection counters.
func (m *RouterMetrics) AgentUniversalUpdated(
	peerID, model, engine, organization, machine string,
	successDelta, failedDelta int64,
	successRate, capacityUtil float64,
	inputDelta, outputDelta int64,
	rejectedDelta, disconnDelta int64,
	srtt, rttvar float64,
) {
	labels := prometheus.Labels{
		"peer_id":      peerID,
		"model":        model,
		"engine":       engine,
		"organization": organization,
		"machine":      machine,
	}
	if successDelta > 0 {
		m.agentSuccessTotal.With(labels).Add(float64(successDelta))
	}
	if failedDelta > 0 {
		m.agentFailedTotal.With(labels).Add(float64(failedDelta))
	}
	m.agentSuccessRate.With(labels).Set(successRate)
	m.agentCapacityUtil.With(labels).Set(capacityUtil)
	if inputDelta > 0 {
		m.agentInputTokens.With(labels).Add(float64(inputDelta))
	}
	if outputDelta > 0 {
		m.agentOutputTokens.With(labels).Add(float64(outputDelta))
	}
	if rejectedDelta > 0 {
		m.agentRejectedTotal.With(labels).Add(float64(rejectedDelta))
	}
	if disconnDelta > 0 {
		m.agentDisconnections.With(labels).Add(float64(disconnDelta))
	}
	m.agentSRTT.With(labels).Set(srtt)
	m.agentRTTVAR.With(labels).Set(rttvar)
}

// AgentHardwareUpdated refreshes all hardware Prometheus gauges from a snapshot.
// Called by HardwareStore on each heartbeat that carries a non-nil snapshot.
func (m *RouterMetrics) AgentHardwareUpdated(peerID, region, model, engine, organization, machine string, snap *domain.HardwareSnapshot) {
	nodeLabels := prometheus.Labels{
		"peer_id":      peerID,
		"region":       region,
		"model":        model,
		"engine":       engine,
		"organization": organization,
		"machine":      machine,
	}
	m.cpuUsage.With(nodeLabels).Set(snap.CPU.UsagePercent)
	m.memUsedPct.With(nodeLabels).Set(snap.Memory.UsedPercent)
	m.memAvailable.With(nodeLabels).Set(float64(snap.Memory.AvailableBytes))
	m.memTotal.With(nodeLabels).Set(float64(snap.Memory.TotalBytes))

	for _, gpu := range snap.GPUs {
		gpuIdx := strconv.Itoa(gpu.Index)
		gpuLabels := prometheus.Labels{
			"peer_id":      peerID,
			"region":       region,
			"model":        model,
			"engine":       engine,
			"gpu_index":    gpuIdx,
			"gpu_id":       peerID + "_" + gpuIdx,
			"organization": organization,
			"machine":      machine,
		}
		m.gpuUtil.With(gpuLabels).Set(gpu.UtilPercent)
		m.gpuVRAMUsed.With(gpuLabels).Set(float64(gpu.VRAMUsedBytes))
		m.gpuVRAMFree.With(gpuLabels).Set(float64(gpu.VRAMFreeBytes))
		m.gpuVRAMTotal.With(gpuLabels).Set(float64(gpu.VRAMTotalBytes))
		m.gpuTemp.With(gpuLabels).Set(gpu.TemperatureC)
		m.gpuPower.With(gpuLabels).Set(gpu.PowerWatts)
	}
}

// deleteGPULabelSet removes all Prometheus GPU series for a single GPU index.
// Called from hardware.go when a GPU disappears between heartbeats or on disconnect.
func (m *RouterMetrics) deleteGPULabelSet(peerID, region, model, engine, gpuIndex, organization, machine string) {
	gpuLabels := prometheus.Labels{
		"peer_id":      peerID,
		"region":       region,
		"model":        model,
		"engine":       engine,
		"gpu_index":    gpuIndex,
		"gpu_id":       peerID + "_" + gpuIndex,
		"organization": organization,
		"machine":      machine,
	}
	m.gpuUtil.Delete(gpuLabels)
	m.gpuVRAMUsed.Delete(gpuLabels)
	m.gpuVRAMFree.Delete(gpuLabels)
	m.gpuVRAMTotal.Delete(gpuLabels)
	m.gpuTemp.Delete(gpuLabels)
	m.gpuPower.Delete(gpuLabels)
}

// AgentHardwareUnregistered removes all hardware Prometheus label sets for peerID.
// gpuIndices holds the actual GPU device indices the agent had; nil/empty for CPU-only nodes.
// Using the actual index list (not a count) correctly handles non-contiguous indices
// produced by CUDA_VISIBLE_DEVICES or MIG partitioning.
// Called by HardwareStore on agent disconnect.
func (m *RouterMetrics) AgentHardwareUnregistered(peerID, region, model, engine, organization, machine string, gpuIndices []int) {
	nodeLabels := prometheus.Labels{
		"peer_id":      peerID,
		"region":       region,
		"model":        model,
		"engine":       engine,
		"organization": organization,
		"machine":      machine,
	}
	m.cpuUsage.Delete(nodeLabels)
	m.memUsedPct.Delete(nodeLabels)
	m.memAvailable.Delete(nodeLabels)
	m.memTotal.Delete(nodeLabels)

	for _, idx := range gpuIndices {
		m.deleteGPULabelSet(peerID, region, model, engine, strconv.Itoa(idx), organization, machine)
	}
}

// AgentEnginePunctualUpdated refreshes engine punctual Prometheus gauges from a
// BackendMetrics snapshot. Nil pointer fields are silently skipped — the gauge
// retains its previous value, preventing false-zero series when the engine does
// not expose a particular metric.
// Called by EnginePunctualStore on each heartbeat that carries backend metrics.
func (m *RouterMetrics) AgentEnginePunctualUpdated(peerID, model, engine, organization, machine string, bm *domain.BackendMetrics) {
	labels := prometheus.Labels{"peer_id": peerID, "model": model, "engine": engine, "organization": organization, "machine": machine}
	if bm.KVCacheUtilization != nil {
		m.engineKVCache.With(labels).Set(*bm.KVCacheUtilization)
	}
	if bm.RunningRequests != nil {
		m.engineRunning.With(labels).Set(*bm.RunningRequests)
	}
	if bm.WaitingRequests != nil {
		m.engineWaiting.With(labels).Set(*bm.WaitingRequests)
	}
	if bm.PreemptionsTotal != nil {
		m.enginePreemptions.With(labels).Set(*bm.PreemptionsTotal)
	}
	if bm.AvgTTFTSeconds != nil {
		m.engineAvgTTFT.With(labels).Set(*bm.AvgTTFTSeconds)
	}
	if bm.P90TTFTSeconds != nil {
		m.engineP90TTFT.With(labels).Set(*bm.P90TTFTSeconds)
	}
	if bm.AvgITLSeconds != nil {
		m.engineAvgITL.With(labels).Set(*bm.AvgITLSeconds)
	}
	if bm.P90ITLSeconds != nil {
		m.engineP90ITL.With(labels).Set(*bm.P90ITLSeconds)
	}
	if bm.PredictedTokensPerSecond != nil {
		m.enginePredictedTPS.With(labels).Set(*bm.PredictedTokensPerSecond)
	}
	if bm.PromptTokensPerSecond != nil {
		m.enginePromptTPS.With(labels).Set(*bm.PromptTokensPerSecond)
	}

	// Feed the custom collector with any histogram snapshots the engine
	// reported. Nil snapshots leave the prior value unchanged.
	m.engineHistograms.update(peerID, model, engine, organization, machine, bm)

	// Finish-reason counter: compute deltas from the last cumulative snapshot
	// and Add() positive increments. First heartbeat after startup has no
	// prior state, so everything counts from zero (baseline is 0, not the
	// cumulative count on the engine — matches how vLLM's own counters work).
	if len(bm.FinishReasonCounts) > 0 {
		peerKey := engineSnapshotKey(peerID, model, engine, organization, machine)
		for reason, d := range m.engineFinishState.deltas(peerKey, bm.FinishReasonCounts) {
			m.engineFinishReason.With(prometheus.Labels{
				"peer_id":         peerID,
				"model":           model,
				"engine":          engine,
				"organization":    organization,
				"machine":         machine,
				"finished_reason": reason,
			}).Add(float64(d))
		}
	}
}

// AgentEnginePunctualUnregistered removes all engine punctual Prometheus label
// sets for peerID. Called by EnginePunctualStore on agent disconnect.
func (m *RouterMetrics) AgentEnginePunctualUnregistered(peerID, model, engine, organization, machine string) {
	labels := prometheus.Labels{"peer_id": peerID, "model": model, "engine": engine, "organization": organization, "machine": machine}
	m.engineKVCache.Delete(labels)
	m.engineRunning.Delete(labels)
	m.engineWaiting.Delete(labels)
	m.enginePreemptions.Delete(labels)
	m.engineAvgTTFT.Delete(labels)
	m.engineP90TTFT.Delete(labels)
	m.engineAvgITL.Delete(labels)
	m.engineP90ITL.Delete(labels)
	m.enginePredictedTPS.Delete(labels)
	m.enginePromptTPS.Delete(labels)

	// Clear histogram snapshots and finish-reason tracker state so no
	// zombie series linger after the agent disconnects.
	m.engineHistograms.remove(peerID, model, engine, organization, machine)
	m.engineFinishState.clear(engineSnapshotKey(peerID, model, engine, organization, machine))

	// Remove every finished_reason variant for this peer in one call —
	// the partial match matches the 5 fixed labels and ignores the
	// variable finished_reason one.
	m.engineFinishReason.DeletePartialMatch(prometheus.Labels{
		"peer_id":      peerID,
		"model":        model,
		"engine":       engine,
		"organization": organization,
		"machine":      machine,
	})
}

// QueueDepthUpdated sets the per-model queue depth gauge.
// Implements policy.QueueMetrics.
func (m *RouterMetrics) QueueDepthUpdated(model string, depth int) {
	m.queueDepth.With(prometheus.Labels{"model": model}).Set(float64(depth))
}

// QueueWaitObserved records a successful wait duration in the per-model histogram.
// Implements policy.QueueMetrics.
func (m *RouterMetrics) QueueWaitObserved(model string, durationSeconds float64) {
	m.queueWaitSeconds.With(prometheus.Labels{"model": model}).Observe(durationSeconds)
}

// PolicyReload increments the policy reload counter.
// trigger is "api" (PUT /admin/policy) or "sighup".
// result is "success" or "error".
func (m *RouterMetrics) PolicyReload(trigger, result string) {
	m.policyReloadTotal.With(prometheus.Labels{"trigger": trigger, "result": result}).Inc()
}

// ObserveHTTPRequest records the duration and status of an HTTP request for RED metrics.
func (m *RouterMetrics) ObserveHTTPRequest(method, route string, statusCode int, durationSeconds float64) {
	code := strconv.Itoa(statusCode)
	m.httpRequestDuration.With(prometheus.Labels{
		"method": method, "route": route, "status_code": code,
	}).Observe(durationSeconds)
}

// HTTPActiveRequestsInc increments the in-flight request gauge.
func (m *RouterMetrics) HTTPActiveRequestsInc(method, route string) {
	m.httpActiveRequests.With(prometheus.Labels{"method": method, "route": route}).Inc()
}

// HTTPActiveRequestsDec decrements the in-flight request gauge.
func (m *RouterMetrics) HTTPActiveRequestsDec(method, route string) {
	m.httpActiveRequests.With(prometheus.Labels{"method": method, "route": route}).Dec()
}

// ObserveHTTPClientRequest records the duration and status of an outbound provider HTTP call.
func (m *RouterMetrics) ObserveHTTPClientRequest(provider string, statusCode int, durationSeconds float64) {
	m.httpClientDuration.With(prometheus.Labels{
		"provider": provider, "status_code": strconv.Itoa(statusCode),
	}).Observe(durationSeconds)
}

// ServeMetrics starts the Prometheus metrics HTTP server on the given address
// (e.g. ":2112"). It runs in its own goroutine and logs fatal errors.
func (m *RouterMetrics) ServeMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	go func() {
		log.Infof("Prometheus metrics server listening on %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Errorf("metrics server error: %v", err)
		}
	}()
}
