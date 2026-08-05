# Prometheus Metrics

Hivenet Router exports comprehensive metrics via Prometheus for monitoring and alerting.

## Metrics Endpoint

```
GET http://localhost:2112/metrics
```

## Metric Tiers

| Tier | Storage | Resets | Examples |
|------|---------|--------|----------|
| Metadata | memDB | Yes | Agent identity, capacity |
| Universal Punctual | memDB | Session | Request counters, success rate |
| Universal History | diskDB | Never | SRTT/RTTVAR, lifetime counters |
| Engine Punctual | memDB | Yes | KV cache, TTFT, ITL |
| Hardware | memDB | Yes | GPU temp, VRAM, CPU |

## Routing-Table Metrics

These metrics track agent registration and routing.

### Agent Info and Health

Labels: `peer_id`, `region`, `engine`, `model`, `capacity`, `organization`, `machine`

```promql
# All currently registered agents (value = 1 while registered)
hivenet_router_routing_agent_info{peer_id="...", region="EU", engine="vllm", model="...", capacity="10", organization="acme", machine="node-1"}

# Agent health: 1=healthy, 0=unhealthy
hivenet_router_routing_agent_healthy{peer_id="...", region="EU", engine="vllm", model="...", capacity="10", organization="acme", machine="node-1"}

# Timestamp of last heartbeat (Unix milliseconds)
hivenet_router_routing_agent_last_seen_timestamp{peer_id="...", region="EU", engine="vllm", model="...", capacity="10", organization="acme", machine="node-1"}

# Count healthy agents by region
sum by (region) (hivenet_router_routing_agent_healthy == 1)
```

### Request Counters (Routing Level)

Labels: `region`, `engine`, `model`, `tenant_id`

```promql
# Requests successfully forwarded to an agent
hivenet_router_routing_requests_routed_total{model="meta-llama/Llama-3.1-8B-Instruct"}

# Requests that could not be forwarded
hivenet_router_routing_requests_failed_total{engine="vllm"}

# Request rate (last 5 minutes)
rate(hivenet_router_routing_requests_routed_total[5m])
```

## Universal Per-Agent Metrics

Labels: `peer_id`, `model`, `engine`, `organization`, `machine`

```promql
# Lifetime successful requests per agent
hivenet_router_agent_requests_success_total{peer_id="12D3Koo0..."}

# Lifetime failed requests per agent
hivenet_router_agent_requests_failed_total{peer_id="12D3Koo0..."}

# Derived success rate (0–1.0)
hivenet_router_agent_success_rate{peer_id="12D3Koo0..."}

# Real-time capacity utilization (active_requests / capacity)
hivenet_router_agent_capacity_utilization{peer_id="12D3Koo0..."}

# Smoothed RTT in milliseconds (RFC 6298)
hivenet_router_agent_srtt_ms{peer_id="12D3Koo0..."}

# RTT variance in milliseconds (RFC 6298)
hivenet_router_agent_rttvar_ms{peer_id="12D3Koo0..."}

# Lifetime input (prompt) tokens
hivenet_router_agent_input_tokens_total{peer_id="12D3Koo0..."}

# Lifetime output (completion) tokens
hivenet_router_agent_output_tokens_total{peer_id="12D3Koo0..."}

# Times agent was at full capacity (slot rejected)
hivenet_router_agent_rejected_requests_total{peer_id="12D3Koo0..."}

# Lifetime disconnections
hivenet_router_agent_disconnections_total{peer_id="12D3Koo0..."}

# Agent marked unhealthy by health monitor (missed heartbeats)
hivenet_router_agent_failure_total{peer_id="12D3Koo0..."}

# Agent reported its backend as unhealthy via heartbeat
hivenet_router_model_backend_failure_total{peer_id="12D3Koo0..."}
```

> These per-agent counters are **persisted to BadgerDB** (`universalHistory:`) and re-seeded into Prometheus when an agent re-registers, so they survive a router restart. To clear them after a deploy and observe a change against a clean baseline, call `POST /admin/metrics/reset` (see [Admin Endpoints](../API%20Reference/05-Admin-Endpoints.md#reset-per-agent-lifetime-counters)).

## Engine Metrics (vLLM / SGLang)

### Scalar Gauges

Labels: `peer_id`, `model`, `engine`, `organization`, `machine`

```promql
# KV cache utilization (0.0–1.0)
hivenet_router_agent_engine_kv_cache_utilization{peer_id="12D3Koo0..."}

# Requests currently being processed
hivenet_router_agent_engine_running_requests{peer_id="12D3Koo0..."}

# Requests queued in the engine scheduler
hivenet_router_agent_engine_waiting_requests{peer_id="12D3Koo0..."}

# Cumulative preemptions — stored as a Gauge; use rate() to detect KV-cache thrashing
# Note: _total suffix is kept for naming consistency but this is a Gauge, not a Counter
hivenet_router_agent_engine_preemptions_total{peer_id="12D3Koo0..."}

# Average time-to-first-token in seconds
hivenet_router_agent_engine_avg_ttft_seconds{peer_id="12D3Koo0..."}

# P90 time-to-first-token in seconds (per-peer scalar; for fleet-wide use the histogram below)
hivenet_router_agent_engine_p90_ttft_seconds{peer_id="12D3Koo0..."}

# Average inter-token latency in seconds (vLLM only)
hivenet_router_agent_engine_avg_itl_seconds{peer_id="12D3Koo0..."}

# P90 inter-token latency in seconds (per-peer scalar; for fleet-wide use the histogram below)
hivenet_router_agent_engine_p90_itl_seconds{peer_id="12D3Koo0..."}

# Requests completed by finish reason (stop|length|abort)
hivenet_router_agent_engine_request_success_total{peer_id="...", finished_reason="stop"}

# Token generation throughput (llama.cpp only)
hivenet_router_agent_engine_predicted_tps{peer_id="..."}
hivenet_router_agent_engine_prompt_tps{peer_id="..."}
```

### Engine Histograms

Labels: `peer_id`, `model`, `engine`, `organization`, `machine`

These are re-exported from the engine's own histogram bucket counts via a custom Prometheus collector. Use `histogram_quantile()` on these for correct fleet-wide percentiles rather than averaging the scalar gauges above.

```promql
# TTFT latency histogram — correct source for fleet-wide percentiles
histogram_quantile(0.90, sum by (le) (rate(hivenet_router_agent_engine_ttft_seconds_bucket[5m])))

# ITL latency histogram
histogram_quantile(0.90, sum by (le) (rate(hivenet_router_agent_engine_itl_seconds_bucket[5m])))

# Per-request prompt token length distribution (workload shape)
histogram_quantile(0.90, sum by (le) (rate(hivenet_router_agent_engine_request_prompt_tokens_bucket[5m])))

# Per-request generation token length distribution (workload shape)
histogram_quantile(0.90, sum by (le) (rate(hivenet_router_agent_engine_request_generation_tokens_bucket[5m])))
```

## Hardware Metrics

### GPU Metrics

Labels: `peer_id`, `region`, `model`, `engine`, `gpu_index`, `gpu_id`, `organization`, `machine`

```promql
# GPU compute utilization percent (0–100)
hivenet_router_agent_gpu_utilization_percent{peer_id="...", gpu_index="0"}

# VRAM in use (bytes)
hivenet_router_agent_gpu_vram_used_bytes{peer_id="...", gpu_index="0"}

# VRAM free (bytes)
hivenet_router_agent_gpu_vram_free_bytes{peer_id="...", gpu_index="0"}

# Total VRAM (bytes)
hivenet_router_agent_gpu_vram_total_bytes{peer_id="...", gpu_index="0"}

# GPU temperature in Celsius
hivenet_router_agent_gpu_temperature_celsius{peer_id="...", gpu_index="0"}

# GPU power draw in watts
hivenet_router_agent_gpu_power_watts{peer_id="...", gpu_index="0"}
```

### CPU / Memory Metrics

Labels: `peer_id`, `region`, `model`, `engine`, `organization`, `machine`

```promql
# CPU utilization percent (0–100)
hivenet_router_agent_cpu_usage_percent{peer_id="..."}

# System memory used percent (0–100)
hivenet_router_agent_memory_used_percent{peer_id="..."}

# System memory available (bytes)
hivenet_router_agent_memory_available_bytes{peer_id="..."}

# Total system memory (bytes)
hivenet_router_agent_memory_total_bytes{peer_id="..."}
```

## Policy Routing Metrics

Labels: `model`

```promql
# Requests served by the primary routing_policy step
hivenet_router_policy_primary_routed_total{model="meta-llama/Llama-3.1-8B-Instruct"}

# Requests served by any fallback_chain step
hivenet_router_policy_fallback_routed_total{model="meta-llama/Llama-3.1-8B-Instruct"}

# Requests served by fallback_provider (closed-source API)
hivenet_router_policy_provider_fallback_total{model="meta-llama/Llama-3.1-8B-Instruct"}

# Requests where all policy steps exhausted (client got 503)
hivenet_router_policy_exhausted_total{model="meta-llama/Llama-3.1-8B-Instruct"}

# Stale agent connections dropped on a connection-level forward failure, so the
# next attempt re-dials. Extra label: reason (currently "forward_failure").
# A sustained rate points at the network repeatedly cutting router→agent links
# (e.g. NetworkPolicy churn, NAT/conntrack drops) rather than agents being down.
hivenet_router_agent_connection_resets_total{model="meta-llama/Llama-3.1-8B-Instruct", reason="forward_failure"}

# Policy hot-reload events (trigger=api|sighup, result=success|error)
hivenet_router_policy_reload_total{trigger="sighup", result="success"}
```

## Queue Metrics

Labels: `model`

```promql
# Current number of requests waiting for a concurrency slot
hivenet_router_queue_depth{model="meta-llama/Llama-3.1-8B-Instruct"}

# Time spent in queue before dispatch (histogram)
histogram_quantile(0.95, rate(hivenet_router_queue_wait_seconds_bucket[5m]))
```

## Tenant / Billing Metrics

These metrics attribute usage to a specific tenant, API key, and deployment. They are the source of truth for enterprise billing rollups and "Last used" tracking per key.

### Label Dimensions

| Label | Source | Default when absent |
|---|---|---|
| `tenant_id` | Auth token (machines service) | `"anonymous"`; `"default"` in no-auth mode |
| `key_id` | `AuthResult.KeyID` from auth token | `"anonymous"` (no-auth mode or static-key provider) |
| `deployment_id` | Agent registration metadata (`HIVENET_ROUTER_DEPLOYMENT_ID` env on the agent pod) | `"unset"` for pre-routing failures (auth, rate-limit, no agent available) or agents that predate this field |
| `model` | Request body `model` field | — |

The `deployment_id` is sourced from the selected agent's registration metadata, not from a request header. Each agent pod is provisioned for exactly one model deployment and advertises its `deployment_id` at registration time. Pre-routing failures (auth errors, rate limits, token limits) correctly carry `"unset"` since no agent has been selected yet.

> **Local development note:** when the router runs in no-auth mode (`AUTH_MODE=none`), `tenant_id` is `"default"` and `key_id` is `"anonymous"`. Prometheus queries against a local instance should filter on `tenant_id="default"` rather than a real tenant slug.

### Counters and Gauges

```promql
# Requests served successfully per tenant / key / deployment
hivenet_router_tenant_requests_success_total{tenant_id="acme", key_id="key-abc", deployment_id="dep-1", model="..."}

# Requests failed per tenant / key / deployment
hivenet_router_tenant_requests_failed_total{tenant_id="acme", key_id="key-abc", deployment_id="dep-1", model="..."}

# Requests rejected by RPM quota (key_id only — deployment context not relevant for rate limiting)
hivenet_router_tenant_rate_limited_total{tenant_id="acme", key_id="key-abc"}

# Requests rejected by daily token budget (phase=input|output)
hivenet_router_tenant_token_limited_total{tenant_id="acme", key_id="key-abc", deployment_id="dep-1", phase="input"}

# Lifetime prompt tokens per tenant / key / deployment
hivenet_router_tenant_input_tokens_total{tenant_id="acme", key_id="key-abc", deployment_id="dep-1", model="..."}

# Lifetime completion tokens per tenant / key / deployment
hivenet_router_tenant_output_tokens_total{tenant_id="acme", key_id="key-abc", deployment_id="dep-1", model="..."}

# Tokens consumed today (UTC calendar day; resets at midnight) — tenant-level only
hivenet_router_tenant_tokens_used_today{tenant_id="acme"}

# Configured RPM limit (0 = unlimited) — tenant-level only
hivenet_router_tenant_quota_rpm_limit{tenant_id="acme"}

# Configured daily token limit (0 = unlimited) — tenant-level only
hivenet_router_tenant_quota_tpd_limit{tenant_id="acme"}

# Unix timestamp (seconds) of the most recent request — enables "Last used" per key
hivenet_router_tenant_last_request_timestamp{tenant_id="acme", key_id="key-abc", deployment_id="dep-1"}

# End-to-end request duration per tenant / key / deployment (histogram)
histogram_quantile(0.95, rate(hivenet_router_tenant_request_duration_seconds_bucket{tenant_id="acme", key_id="key-abc", deployment_id="dep-1"}[5m]))

# Redis quota backend call failures (fail-open events)
hivenet_router_quota_backend_errors_total
```

### Common PromQL Patterns

```promql
# 7-day request count rolled up per deployment (FE list page)
sum by (deployment_id) (
  increase(hivenet_router_tenant_requests_success_total{tenant_id="acme"}[7d])
)

# Last-used timestamp per API key
max by (key_id) (hivenet_router_tenant_last_request_timestamp{tenant_id="acme"})

# Token consumption breakdown by key
sum by (key_id, model) (hivenet_router_tenant_input_tokens_total{tenant_id="acme"})
```

## HTTP Server Metrics

Labels: `method`, `route`, `status_code`

```promql
# Request duration per endpoint (histogram)
histogram_quantile(0.95, rate(http_server_request_duration_seconds_bucket[5m]))

# In-flight requests
http_server_active_requests{method="POST", route="/v1/chat/completions"}
```

## HTTP Client Metrics (Provider Fallback)

Labels: `provider`, `status_code`

```promql
# Outbound provider call duration (histogram)
histogram_quantile(0.95, rate(http_client_request_duration_seconds_bucket[5m]))
```

## Request Duration (Per-Agent)

Labels: `tenant_id`, `peer_id`, `model`, `status_code`

```promql
# End-to-end request duration labeled by tenant, agent, model, and HTTP status
histogram_quantile(0.95, rate(hivenet_router_request_duration_seconds_bucket[5m]))
```

## Alerting Examples

### High Failure Rate

```yaml
- alert: HighFailureRate
  expr: >
    rate(hivenet_router_routing_requests_failed_total[5m]) /
    (rate(hivenet_router_routing_requests_routed_total[5m]) + rate(hivenet_router_routing_requests_failed_total[5m])) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High failure rate (>10%)"
```

### KV Cache Pressure

```yaml
- alert: KVCachePressure
  expr: hivenet_router_agent_engine_kv_cache_utilization > 0.9
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High KV cache usage on {{ $labels.peer_id }}"
```

### GPU Overheating

```yaml
- alert: GPUOverheating
  expr: hivenet_router_agent_gpu_temperature_celsius > 85
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "GPU overheating on {{ $labels.peer_id }}"
```

### Agent Down

```yaml
- alert: AgentDown
  expr: hivenet_router_routing_agent_healthy == 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Agent {{ $labels.peer_id }} is unhealthy"
```

### High SRTT

```yaml
- alert: HighAgentLatency
  expr: hivenet_router_agent_srtt_ms > 5000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Agent {{ $labels.peer_id }} SRTT > 5s"
```

## Grafana Dashboards

Pre-configured dashboards are available in the Docker Compose stack:

- **Hivenet Router** — Agent health, request metrics, policy routing
- **Hivenet Router Audit** — Request logs (requires Loki)

Access at http://localhost:3000

## See Also

- [Grafana Dashboards](02-Grafana-Dashboards.md) - Visual dashboards
- [Audit Logging](03-Audit-Logging.md) - Request logging
- [Detailed Architecture](../Reference/00-Detailed-Architecture.md) - Metric tier system
