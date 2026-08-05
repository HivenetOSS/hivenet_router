// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import "hivenet_router/internal/domain"

// Policy is the top-level routing configuration loaded from a YAML file.
// It contains a primary routing step, an optional fallback chain, and an
// optional last-resort provider fallback for when all local steps are exhausted.
//
// When loaded from a directory via LoadDirSnapshot, the Models field declares
// which model names this policy applies to. One file can cover multiple models.
// Models is ignored when setting the global policy (--policy-file, _default.yaml,
// or PUT /admin/policy). Models is required and drives routing for named per-model
// policy documents (PUT /admin/policy/models/{name}).
type Policy struct {
	Models           []string          `yaml:"models"           json:"models,omitempty"`
	RoutingPolicy    PolicyStep        `yaml:"routing_policy"   json:"routing_policy"`
	FallbackChain    []FallbackStep    `yaml:"fallback_chain"   json:"fallback_chain,omitempty"`
	FallbackProvider *FallbackProvider `yaml:"fallback_provider" json:"fallback_provider,omitempty"`
}

// FallbackProvider configures a last-resort closed-source model provider.
// It is called only when all local routing steps (primary + fallback chain) are exhausted.
// The provider is identified by engine name and the API key is supplied via env variable.
type FallbackProvider struct {
	// Engine is the provider name: "openai" or "anthropic".
	Engine string `yaml:"engine" json:"engine"`
	// Model is the model sent to the provider API, overriding the original request model.
	Model string `yaml:"model" json:"model"`
}

// PolicyStep is one routing attempt: three layers applied in sequence.
//
//	Layer 1 — match:      static metadata filter (region, engine, tags…)
//	Layer 2 — exclude_if: dynamic metric gates (KV cache, GPU temp…)
//	Layer 3 — strategy:   ranking algorithm to pick the winner
type PolicyStep struct {
	Match     MatchFilter              `yaml:"match"      json:"match"`
	ExcludeIf map[string]ThresholdRule `yaml:"exclude_if" json:"exclude_if,omitempty"`
	Strategy  string                   `yaml:"strategy"   json:"strategy"`
	// MaxTries caps forward attempts for this step. 0 means use the global default.
	MaxTries int `yaml:"max_tries" json:"max_tries,omitempty"`
}

// FallbackStep is a PolicyStep with an optional name used in logs and metrics.
type FallbackStep struct {
	Name       string `yaml:"name" json:"name,omitempty"`
	PolicyStep `yaml:",inline" json:",inline"`
}

// MatchFilter narrows the candidate pool to agents whose metadata satisfies
// every non-empty field. An empty MatchFilter matches all agents.
// All comparisons are case-sensitive.
type MatchFilter struct {
	Region       string   `yaml:"region"       json:"region,omitempty"`
	Engine       string   `yaml:"engine"       json:"engine,omitempty"`
	Tags         []string `yaml:"tags"         json:"tags,omitempty"`
	Organization string   `yaml:"organization" json:"organization,omitempty"`
	Machine      string   `yaml:"machine"      json:"machine,omitempty"`
	GPUModel     string   `yaml:"gpu_model"    json:"gpu_model,omitempty"`
}

// ThresholdRule holds exactly one comparison operator and its threshold value.
// Only one of {GT, LT, GTE, LTE} may be non-nil; Validate enforces this.
type ThresholdRule struct {
	GT  *float64 `yaml:"gt"  json:"gt,omitempty"`
	LT  *float64 `yaml:"lt"  json:"lt,omitempty"`
	GTE *float64 `yaml:"gte" json:"gte,omitempty"`
	LTE *float64 `yaml:"lte" json:"lte,omitempty"`
}

// AgentSnapshot is the live metric view assembled per-agent for policy evaluation.
// All fields are pointers: nil means the metric is unavailable for this agent
// (e.g. a non-vLLM agent has no kv_cache_utilization). A nil metric silently
// passes any exclude_if gate — the gate is skipped for that agent.
type AgentSnapshot struct {
	// Universal — available for all agents.
	CapacityUtilization *float64 // fraction 0.0–1.0
	SuccessRate         *float64 // fraction 0.0–1.0
	SRTT                *float64 // milliseconds (RFC 6298 smoothed RTT)
	ConsecutiveFailures *float64 // absolute count; reset to 0 on any success

	// Engine (vLLM / SGLang) — nil for non-vLLM/SGLang agents.
	KVCacheUtilization *float64 // fraction 0.0–1.0
	RunningRequests    *float64 // absolute count
	WaitingRequests    *float64 // absolute count
	AvgTTFTSeconds     *float64 // seconds
	P90TTFTSeconds     *float64 // seconds
	AvgITLSeconds      *float64 // seconds
	P90ITLSeconds      *float64 // seconds

	// Hardware — nil when no hardware snapshot has been received yet.
	// All fraction fields are normalised to 0.0–1.0 by the evaluator regardless
	// of the raw unit reported by NVML / gopsutil (which use 0–100 percent).
	GPUTemperatureC    *float64 // degrees Celsius (absolute, not a fraction)
	GPUUtilPercent     *float64 // fraction 0.0–1.0 (normalised from NVML 0–100)
	GPUVRAMUsedPercent *float64 // fraction 0.0–1.0 (VRAMUsedBytes / VRAMTotalBytes)
	MemoryUsedPercent  *float64 // fraction 0.0–1.0 (normalised from gopsutil 0–100)
	CPUUsagePercent    *float64 // fraction 0.0–1.0 (normalised from gopsutil 0–100)
}

// ScoredCandidate pairs an agent with its live snapshot so the strategy
// receives both the agent reference and pre-fetched metrics.
type ScoredCandidate struct {
	Agent    *domain.Agent
	Snapshot AgentSnapshot
}
