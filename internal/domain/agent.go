// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Agent capability values — what type of inference this agent serves.
const (
	CapabilityLLM       = "llm"
	CapabilityEmbedding = "embedding"
	CapabilityReranker  = "reranker"
)

// AgentMetadata contains agent capabilities and identification
type AgentMetadata struct {
	Model         string   `json:"model"`
	Capacity      int      `json:"capacity"`
	Version       string   `json:"version"`
	Region        string   `json:"region,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	Engine        string   `json:"engine,omitempty"`
	Organization  string   `json:"organization,omitempty"`
	Machine       string   `json:"machine,omitempty"`
	HideLLM       bool     `json:"hide_llm,omitempty"`
	LLMPrettyName string   `json:"llm_pretty_name,omitempty"`
	LLMInfo       string   `json:"llm_info,omitempty"`
	Capability    string   `json:"capability,omitempty"`    // "llm" | "embedding" | "reranker"
	GPUModel      string   `json:"gpu_model,omitempty"`     // operator-set hardware identifier (e.g. "RTX4090")
	DeploymentID  string   `json:"deployment_id,omitempty"` // logical deployment id supplied via HIVENET_ROUTER_DEPLOYMENT_ID env
	// ReplicaID identifies this replica, supplied via HIVENET_ROUTER_REPLICA_ID. It
	// pairs with DeploymentID to form a stable join key an external scheduler
	// can use to map a router-side row back to the workload it started. Empty
	// when the agent was not given one (local dev, bare-metal).
	ReplicaID string `json:"replica_id,omitempty"`
}

// Agent represents a connected agent with its connection state.
// The router never dials agents: inference streams are opened over the
// libp2p connection the agent itself established, so no dial addresses
// are stored — a live connection is the only reachability that matters.
type Agent struct {
	ID             peer.ID
	Metadata       AgentMetadata
	LastSeen       time.Time
	Healthy        bool
	BackendHealthy bool // last reported backend health from heartbeat; false = model server down
	ActiveRequests int
	SessionToken   string
	mu             sync.RWMutex
}

// NewAgent creates a new agent instance
func NewAgent(id peer.ID, metadata AgentMetadata, sessionToken string) *Agent {
	return &Agent{
		ID:             id,
		Metadata:       metadata,
		LastSeen:       time.Now(),
		Healthy:        true,
		BackendHealthy: true,
		ActiveRequests: 0,
		SessionToken:   sessionToken,
	}
}

// Lock acquires write lock
func (a *Agent) Lock() {
	a.mu.Lock()
}

// Unlock releases write lock
func (a *Agent) Unlock() {
	a.mu.Unlock()
}

// RLock acquires read lock
func (a *Agent) RLock() {
	a.mu.RLock()
}

// RUnlock releases read lock
func (a *Agent) RUnlock() {
	a.mu.RUnlock()
}

// DecrementLoad safely decrements active requests
func (a *Agent) DecrementLoad() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ActiveRequests--
}

// TryAcquireSlot atomically checks capacity and increments load if room is available.
// Returns true and holds the slot; the caller must call DecrementLoad when done.
// Returns false if the agent is already at capacity.
func (a *Agent) TryAcquireSlot() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ActiveRequests >= a.Metadata.Capacity {
		return false
	}
	a.ActiveRequests++
	return true
}

// UpdateLastSeen updates the last seen timestamp
func (a *Agent) UpdateLastSeen() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.LastSeen = time.Now()
}

// IsHealthy returns the health status
func (a *Agent) IsHealthy() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Healthy
}

// SetHealthy sets the health status
func (a *Agent) SetHealthy(healthy bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Healthy = healthy
}

// IsBackendHealthy returns the last reported backend health status.
func (a *Agent) IsBackendHealthy() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.BackendHealthy
}

// SetBackendHealthy updates the backend health status and returns the previous value.
// The returned previous value lets the caller detect a healthy→unhealthy transition.
func (a *Agent) SetBackendHealthy(healthy bool) (prev bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev = a.BackendHealthy
	a.BackendHealthy = healthy
	return prev
}

// GetLoad returns the current active request count
func (a *Agent) GetLoad() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ActiveRequests
}

// AgentRegistration represents persistent agent registration in storage
type AgentRegistration struct {
	PeerID    string        `json:"peer_id"`
	Model     string        `json:"model"`
	Capacity  int           `json:"capacity"`
	Region    string        `json:"region"`
	IsHealthy bool          `json:"is_healthy"`
	LastSeen  time.Time     `json:"last_seen"`
	Metadata  AgentMetadata `json:"metadata"`
}

// AgentUniversalPunctual holds session-scoped, engine-agnostic counters for one agent.
// Stored under universalPunctual:{peerID} in memDB.
// Resets on router restart; survives agent disconnect/reconnect within the same router run.
type AgentUniversalPunctual struct {
	LastFailureAt       time.Time `json:"last_failure_at"`
	OnlineSince         time.Time `json:"online_since"`         // start of current agent session
	DisconnectionCount  int64     `json:"disconnection_count"`  // disconnections this router session
	RejectedRequests    int64     `json:"rejected_requests"`    // times TryAcquireSlot returned false
	CapacityUtilization float64   `json:"capacity_utilization"` // ActiveRequests/Capacity, recomputed at write
}

// AgentUniversalHistory holds lifetime, engine-agnostic counters for one agent.
// Stored under universalHistory:{peerID} in diskDB.
// Survives router restarts. SRTT/RTTVAR persisted for warm-start on reconnect.
type AgentUniversalHistory struct {
	SuccessfulRequests  int64   `json:"successful_requests"`
	FailedRequests      int64   `json:"failed_requests"`
	SuccessRate         float64 `json:"success_rate"` // SuccessfulRequests/(Successful+Failed), recomputed at write
	InputTokens         int64   `json:"input_tokens"`
	OutputTokens        int64   `json:"output_tokens"`
	TotalDisconnections int64   `json:"total_disconnections"` // cumulative across all sessions
	SRTT                float64 `json:"srtt_ms"`              // smoothed RTT (ms), RFC 6298 α=1/8 — persisted for warm-start
	RTTVAR              float64 `json:"rttvar_ms"`            // RTT variance (ms), RFC 6298 β=1/4 — persisted for warm-start
	AgentFailures       int64   `json:"agent_failures"`       // times health monitor marked this agent unhealthy
	BackendFailures     int64   `json:"backend_failures"`     // times agent reported its backend unhealthy
}

// HardwareSnapshot holds the latest hardware reading pushed by an agent via heartbeat.
// Stored under hardwareSnapshot:{peerID} in memDB.
// GPU slice is empty for CPU-only agents.
type HardwareSnapshot struct {
	GPUs      []GPUMetric  `json:"gpu"`
	CPU       CPUMetric    `json:"cpu"`
	Memory    MemoryMetric `json:"memory"`
	Timestamp int64        `json:"timestamp"`
}

// GPUMetric holds metrics for a single GPU device.
type GPUMetric struct {
	Index          int     `json:"index"`
	UtilPercent    float64 `json:"util_percent"`
	VRAMUsedBytes  int64   `json:"vram_used_bytes"`
	VRAMFreeBytes  int64   `json:"vram_free_bytes"`
	VRAMTotalBytes int64   `json:"vram_total_bytes"`
	TemperatureC   float64 `json:"temperature_c"`
	PowerWatts     float64 `json:"power_watts"`
}

// CPUMetric holds node-level CPU utilization.
type CPUMetric struct {
	UsagePercent float64 `json:"usage_percent"`
}

// MemoryMetric holds node-level system memory metrics.
type MemoryMetric struct {
	UsedPercent    float64 `json:"used_percent"`
	AvailableBytes int64   `json:"available_bytes"`
	TotalBytes     int64   `json:"total_bytes"`
}

// BackendMetrics holds the latest engine-specific metrics scraped from the backend
// by the agent's fast poller and pushed to the router inside every heartbeat.
// Stored under enginePunctual:{peerID} in memDB.
//
// All metric fields use pointer types: nil means "this engine does not expose this
// metric", which prevents false-zero Prometheus series on the router side.
type BackendMetrics struct {
	// KVCacheUtilization is the fraction of the pre-allocated GPU KV-cache in use (0.0–1.0).
	// Source: vllm:kv_cache_usage_perc. Near 1.0 → preemptions imminent → route away.
	KVCacheUtilization *float64 `json:"kv_cache_utilization,omitempty"`

	// RunningRequests is the number of requests currently being processed by the engine.
	// Source: vllm:num_requests_running.
	RunningRequests *float64 `json:"running_requests,omitempty"`

	// WaitingRequests is the number of requests queued in the engine scheduler.
	// Source: vllm:num_requests_waiting. Non-zero → scheduler saturated → route away.
	WaitingRequests *float64 `json:"waiting_requests,omitempty"`

	// PreemptionsTotal is the cumulative count of requests preempted by the engine.
	// Source: vllm:num_preemptions_total. Use rate() in Prometheus to detect cache thrashing.
	PreemptionsTotal *float64 `json:"preemptions_total,omitempty"`

	// AvgTTFTSeconds is the running average time-to-first-token in seconds,
	// derived from vllm:time_to_first_token_seconds histogram (sum/count).
	// Nil when no requests have completed yet (count == 0).
	AvgTTFTSeconds *float64 `json:"avg_ttft_seconds,omitempty"`

	// P90TTFTSeconds is the 90th-percentile time-to-first-token in seconds,
	// estimated via linear interpolation from vllm:time_to_first_token_seconds histogram buckets.
	// Nil when no requests have completed yet.
	//
	// Kept as a scalar field (rather than deriving from TTFTHistogram at query time)
	// because the routing policy evaluator reads this value synchronously from the
	// in-process snapshot at route-decision time.
	P90TTFTSeconds *float64 `json:"p90_ttft_seconds,omitempty"`

	// AvgITLSeconds is the running average inter-token latency in seconds,
	// derived from vllm:inter_token_latency_seconds histogram (sum/count).
	// Nil when no tokens have been generated yet (count == 0).
	AvgITLSeconds *float64 `json:"avg_itl_seconds,omitempty"`

	// P90ITLSeconds is the 90th-percentile inter-token latency in seconds.
	// See P90TTFTSeconds for why this is kept as a scalar alongside the histogram.
	P90ITLSeconds *float64 `json:"p90_itl_seconds,omitempty"`

	// PredictedTokensPerSecond is the token generation throughput in tokens/sec,
	// measured over the last second. Source: llamacpp:predicted_tokens_seconds (llama.cpp only).
	// Nil for engines that do not expose this gauge.
	PredictedTokensPerSecond *float64 `json:"predicted_tokens_per_second,omitempty"`

	// PromptTokensPerSecond is the prompt ingestion throughput in tokens/sec,
	// measured over the last second. Source: llamacpp:prompt_tokens_seconds (llama.cpp only).
	// Nil for engines that do not expose this gauge.
	PromptTokensPerSecond *float64 `json:"prompt_tokens_per_second,omitempty"`

	// TTFTHistogram is the raw time-to-first-token histogram snapshot from the
	// engine. Re-exported by the router via a custom Prometheus collector as
	// hivenet_router_agent_engine_ttft_seconds_{bucket,sum,count}. Enables correct
	// fleet-wide percentile aggregation (histogram_quantile on the sum of rates)
	// and heatmap visualisation — neither of which is possible with scalar gauges.
	TTFTHistogram *HistogramSnapshot `json:"ttft_histogram,omitempty"`

	// ITLHistogram is the raw inter-token latency histogram snapshot.
	// Re-exported as hivenet_router_agent_engine_itl_seconds_{bucket,sum,count}.
	ITLHistogram *HistogramSnapshot `json:"itl_histogram,omitempty"`

	// PromptTokensHistogram is the per-request prompt-length histogram (in tokens).
	// Re-exported as hivenet_router_agent_engine_request_prompt_tokens_{bucket,sum,count}.
	// Drives the workload-shape heatmap on the Inference Engine dashboard.
	PromptTokensHistogram *HistogramSnapshot `json:"prompt_tokens_histogram,omitempty"`

	// GenerationTokensHistogram is the per-request generation-length histogram (in tokens).
	// Re-exported as hivenet_router_agent_engine_request_generation_tokens_{bucket,sum,count}.
	GenerationTokensHistogram *HistogramSnapshot `json:"generation_tokens_histogram,omitempty"`

	// FinishReasonCounts maps vLLM finished_reason label values (stop, length,
	// abort, etc.) to cumulative request counts observed on the engine since it
	// last restarted. The router computes deltas across snapshots and increments
	// hivenet_router_agent_engine_request_success_total{finished_reason=...}. Pod
	// restarts are detected as a decrease (new < prev) and the new value is
	// treated as the post-restart delta.
	FinishReasonCounts map[string]uint64 `json:"finish_reason_counts,omitempty"`
}

// HistogramBucket is a single cumulative bucket point from a Prometheus-style
// histogram. Le is the upper bound (math.Inf(1) is allowed and represents the
// +Inf catch-all bucket). Count is the cumulative observation count up to and
// including that bound.
type HistogramBucket struct {
	Le    float64 `json:"le"`
	Count uint64  `json:"count"`
}

// HistogramSnapshot captures the full state of a Prometheus-style histogram
// at a single point in time. It is designed to be shipped from the agent to
// the router in heartbeat / routing-signal payloads and re-exported as a
// Prometheus histogram via prometheus.NewConstHistogram.
//
// Buckets must be sorted by Le ascending and should include the +Inf bucket
// (whose Count equals the total observation count). Sum and Count redundantly
// mirror what _sum and _count scalars would emit, kept for ConstHistogram
// construction and for engines that expose sum/count but no buckets.
type HistogramSnapshot struct {
	Buckets []HistogramBucket `json:"buckets,omitempty"`
	Sum     float64           `json:"sum"`
	Count   uint64            `json:"count"`
}
