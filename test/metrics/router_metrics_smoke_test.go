// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
)

// TestRouterMetrics_EmitMethodsNoPanic exercises the main metric-emitting methods
// with valid labels. Prometheus panics on a label-count mismatch or duplicate
// registration, so a clean run is a meaningful contract check across the whole
// metric surface (not just a coverage bump).
func TestRouterMetrics_EmitMethodsNoPanic(t *testing.T) {
	m := metrics.NewRouterMetrics()

	const (
		peerID = "peer-1"
		region = "eu"
		engine = "vllm"
		model  = "qwen"
		cap    = "10"
		org    = "acme"
		mach   = "gpu-box"
		tenant = "tenant-a"
		keyID  = "key-1"
		deploy = "dep-1"
	)

	// Agent lifecycle + health + failure counters.
	m.AgentRegistered(peerID, region, engine, model, cap, org, mach)
	m.AgentHealthUpdated(peerID, region, engine, model, cap, org, mach, true)
	m.AgentHealthUpdated(peerID, region, engine, model, cap, org, mach, false)
	m.SeedAgentFailureCounters(peerID, model, engine, org, mach, 1, 2)
	m.AgentFailure(peerID, model, engine, org, mach)
	m.ModelBackendFailure(peerID, model, engine, org, mach)

	// Routing-level request outcomes + duration histogram.
	m.RequestRouted(region, engine, model, tenant)
	m.RequestFailed(region, engine, model, tenant)
	m.ObserveRequestDuration(tenant, peerID, model, "200", 0.12)

	// Per-tenant accounting: usage, quota/TPD limits, rate/token limiting.
	m.PrimeTenantCounters(tenant, keyID, deploy, model)
	m.TenantRequestSucceeded(tenant, keyID, deploy, model, 10, 20)
	m.TenantRequestFailed(tenant, keyID, deploy, model)
	m.TenantRateLimited(tenant, keyID)
	m.TenantSetQuotaLimit(tenant, 100)
	m.TenantSetTPDLimit(tenant, 100000)
	m.TenantSetPerModelQuotaLimit(tenant, model, 50)
	m.TenantSetPerModelTPDLimit(tenant, model, 50000)
	m.TenantTokenLimited(tenant, keyID, deploy, "input")
	m.TenantObserveRequestDuration(tenant, keyID, deploy, model, 0.3)
	m.QuotaBackendError()

	// Routing-policy pipeline outcomes (primary / fallback / exhausted / reload).
	m.PolicyPrimaryRouted(model)
	m.PolicyFallbackRouted(model)
	m.PolicyProviderFallbackRouted(model)
	m.PolicyExhausted(model)
	m.PolicyReload("sighup", "ok")

	// Queue depth/wait gauges + connection resets.
	m.QueueDepthUpdated(model, 3)
	m.QueueWaitObserved(model, 0.05)
	m.AgentConnectionReset(model, "timeout")

	// Inbound HTTP server + outbound HTTP client instrumentation.
	m.ObserveHTTPRequest("POST", "/v1/chat/completions", 200, 0.02)
	m.HTTPActiveRequestsInc("POST", "/v1/chat/completions")
	m.HTTPActiveRequestsDec("POST", "/v1/chat/completions")
	m.ObserveHTTPClientRequest("openai", 200, 0.5)

	// Hardware snapshot + engine punctual metrics (pointer fields for optionality).
	kv := 0.7
	run := 2.0
	snap := &domain.HardwareSnapshot{
		GPUs:      []domain.GPUMetric{{}},
		Timestamp: 1,
	}
	m.AgentHardwareUpdated(peerID, region, model, engine, org, mach, snap)
	m.AgentEnginePunctualUpdated(peerID, model, engine, org, mach, &domain.BackendMetrics{
		KVCacheUtilization: &kv, RunningRequests: &run,
	})

	// Reset + unregister paths must also be panic-free.
	m.ResetAgentSeries()
	m.AgentEnginePunctualUnregistered(peerID, model, engine, org, mach)
	m.AgentUnregistered(peerID, region, engine, model, cap, org, mach)
}
