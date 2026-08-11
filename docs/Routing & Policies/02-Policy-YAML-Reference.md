# Policy YAML Reference

Complete reference for the routing policy YAML format.

## Basic Structure

```yaml
mode: "reserved"           # or "serverless" — see Admission Control below
max_input_tokens: 0        # per-request caps and occupancy budget; 0 disables
images_max: 0
admit_budget_tokens: 0
max_inflight: 0
shed_if: {}                # front-door shed thresholds

routing_policy:
  match: {}
  exclude_if: {}
  strategy: "least-loaded"
  max_tries: 3

fallback_chain:
  - name: "fallback-name"
    match: {}
    exclude_if: {}
    strategy: "least-loaded"
    max_tries: 2

fallback_provider:
  engine: "openai"
  model: "gpt-4o-mini"
```

`fallback_provider` is a top-level field — not a step inside `fallback_chain`. It is the last resort after all `fallback_chain` steps are exhausted. API keys are supplied via environment variables (`HIVENET_ROUTER_OPENAI_API_KEY`, `HIVENET_ROUTER_ANTHROPIC_API_KEY`), not in the YAML.

The admission-control fields (`mode`, `max_input_tokens`, `images_max`, `admit_budget_tokens`, `max_inflight`, `shed_if`) are all optional and each defaults to "unset" (inert). A routing-only policy that omits them behaves exactly as before. See [Admission Control](05-Admission-Control.md) for concepts and a worked example.

## Layer 1: Match (Static Filter)

Filter agents by static metadata. All conditions must be true (AND logic). All comparisons are **exact and case-sensitive**.

```yaml
match:
  region: "EU-France"
  engine: "vllm"
  tags: ["production", "gpu-a100"]
  organization: "ml-team"
  machine: "gpu-worker-1"
  gpu_model: "RTX4090"
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `region` | string | Agent region label |
| `engine` | string | Backend engine (`vllm`, `ollama`, `sglang`, `llamacpp`, `infinity`, `custom`) |
| `tags` | array | Agent tags (all must match) |
| `organization` | string | Organization label |
| `machine` | string | Machine hostname |
| `gpu_model` | string | GPU model name |

**Empty match:** `{}` matches all agents (after model filter).

## Layer 2: Exclude If (Dynamic Gates)

Exclude agents based on real-time metrics. Any failing condition removes the agent.

A gate whose metric is **unavailable** for an agent (e.g. `kv_cache_utilization` on a non-vLLM agent) is silently skipped — the agent is not excluded.

Unknown field names are **rejected at load time** with a validation error.

```yaml
exclude_if:
  kv_cache_utilization: { gt: 0.85 }
  gpu_temperature_c: { gt: 82 }
  success_rate: { lt: 0.95 }
  srtt: { gt: 500 }
  gpu_vram_used_percent: { gt: 0.9 }
```

### Operators

| Operator | Description |
|----------|-------------|
| `gt` | Greater than |
| `lt` | Less than |
| `gte` | Greater than or equal |
| `lte` | Less than or equal |

Only one operator per field is allowed. Specifying two operators on the same field is a validation error.

### Metrics

#### Universal Metrics (all agents)

| Field | Type | Range | Description |
|-------|------|-------|-------------|
| `capacity_utilization` | float | 0.0–1.0 | `active_requests / capacity` |
| `success_rate` | float | 0.0–1.0 | Lifetime success fraction |
| `srtt` | float | 0+ ms | Smoothed RTT in **milliseconds** (RFC 6298) |
| `consecutive_failures` | float | 0+ | Forward failures since last success (resets on any success) |

#### vLLM / SGLang Engine Metrics

| Field | Type | Range | Description |
|-------|------|-------|-------------|
| `kv_cache_utilization` | float | 0.0–1.0 | KV cache usage |
| `running_requests` | float | 0+ | Requests currently being processed |
| `waiting_requests` | float | 0+ | Requests queued in the engine scheduler |
| `avg_ttft_seconds` | float | 0+ | Average time to first token (seconds) |
| `p90_ttft_seconds` | float | 0+ | P90 time to first token (seconds) |
| `avg_itl_seconds` | float | 0+ | Average inter-token latency (seconds) |
| `p90_itl_seconds` | float | 0+ | P90 inter-token latency (seconds) |

#### Hardware Metrics (NVML + gopsutil)

| Field | Type | Range | Description |
|-------|------|-------|-------------|
| `gpu_temperature_c` | float | 0–150 | Hottest GPU temperature in °C (absolute) |
| `gpu_util_percent` | float | 0.0–1.0 | Highest GPU compute utilization (normalised) |
| `gpu_vram_used_percent` | float | 0.0–1.0 | Highest VRAM used / VRAM total (normalised) |
| `cpu_usage_percent` | float | 0.0–1.0 | Node CPU utilization (normalised) |
| `memory_used_percent` | float | 0.0–1.0 | RAM used / RAM total (normalised) |

> **Note:** All hardware fraction fields (`gpu_util_percent`, `cpu_usage_percent`, `memory_used_percent`, `gpu_vram_used_percent`) are normalised to 0.0–1.0 by the router regardless of the 0–100 values shown in Prometheus gauges. `gpu_temperature_c` is the only hardware field that stays as absolute Celsius.

**Empty exclude_if:** `{}` — no dynamic filtering.

## Layer 3: Strategy (Ranking)

Rank remaining agents. Only `least-loaded` is currently implemented.

```yaml
strategy: "least-loaded"
```

### Available Strategies

| Strategy | Status | Description |
|----------|--------|-------------|
| `least-loaded` | ✅ | Rank by `active_requests / capacity` ascending |
| `lowest-srtt` | 📋 Planned | Rank by smoothed RTT |
| `round-robin` | 📋 Planned | Rotating index |
| `prefix-aware` | 📋 Planned | CHWBL consistent hashing |

## Max Tries

```yaml
max_tries: 3
```

Number of **failed forward attempts to the backend** before escalating to the next fallback step. Slot-acquisition races (where another request takes the slot between scoring and acquisition) do not count as tries and are retried silently within the same step.

- **Default:** 3 (or the value of `--max-tries-per-step`)
- **Per-step override:** set `max_tries` on each `fallback_chain` step independently

## Fallback Chain

Ordered fallback steps when the primary policy produces no eligible agent.

```yaml
fallback_chain:
  - name: "any-region"
    match:
      engine: "vllm"
    exclude_if:
      success_rate: { lt: 0.90 }
    strategy: "least-loaded"
    max_tries: 2

  - name: "relaxed"
    match: {}
    strategy: "least-loaded"
    max_tries: 2
```

### Fallback Step Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Step identifier (used in logs and metrics) |
| `match` | object | Static filter (same fields as primary) |
| `exclude_if` | object | Dynamic gates (same fields as primary) |
| `strategy` | string | Ranking algorithm |
| `max_tries` | int | Failed forward attempts before advancing |

### When Fallback Triggers

A step advances to the next fallback when any of the following occur:

1. **Model not found** — No agents are registered for the requested model
2. **All unhealthy** — Agents exist but all are unhealthy or have a failed backend
3. **No candidates** — No agents pass the `match` filter
4. **All excluded** — All filter-passing agents fail `exclude_if` gates
5. **Capacity queue exhausted** — All healthy agents are full and the wait queue timed out or reached its depth limit
6. **Max tries exhausted** — Failed forward attempts reach `max_tries` for the current step

## Provider Fallback

Last-resort closed-source API (OpenAI, Anthropic). Top-level field — not a step inside `fallback_chain`.

```yaml
fallback_provider:
  engine: "openai"
  model: "gpt-4o-mini"
```

Invoked only after all `fallback_chain` steps are exhausted.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `engine` | string | ✅ | `openai` or `anthropic` |
| `model` | string | ✅ | Model name sent to the provider API |

### API Key Configuration

Credentials are never stored in the YAML — supply them via environment variables:

| Provider | Environment Variable |
|----------|---------------------|
| `openai` | `HIVENET_ROUTER_OPENAI_API_KEY` |
| `anthropic` | `HIVENET_ROUTER_ANTHROPIC_API_KEY` |

## Per-Model Policies

To apply different policies per model, declare the models the file applies to with the `models:` field:

```yaml
models:
  - "meta-llama/Llama-3.1-8B-Instruct"
  - "meta-llama/Llama-3.1-70B-Instruct"

routing_policy:
  match:
    region: "EU-France"
  strategy: "least-loaded"
```

### Loading from a Directory

```bash
./bin/hivenet-router --policy-model-dir /etc/hivenet-router/models/
```

Each `.yaml` / `.yml` file in the directory is a named policy document. Rules:

| Rule | Behaviour |
|------|-----------|
| `_default.yaml` | Loaded as the **global policy override** (replaces `--policy-file`). Its `models:` field is ignored. A parse error here is fatal. |
| Per-model files | Must declare at least one model via `models:`. Files without `models:` are skipped with a warning. |
| Conflict resolution | The **oldest file by modification time** wins when two files claim the same model. The losing file is skipped entirely (logged as an error). |
| Parse errors | Non-fatal for per-model files — the file is skipped and other files continue loading. |

### Per-Model Policy API

Named policies can also be managed at runtime without restarting:

```bash
# List all named policies
GET /admin/policy/models

# Get a named policy
GET /admin/policy/models/{name}

# Create or replace a named policy (YAML body)
PUT /admin/policy/models/{name}

# Delete a named policy (model reverts to global policy)
DELETE /admin/policy/models/{name}
```

> **Note:** API-loaded named policies are ephemeral and lost on restart. Use `--policy-model-dir` for persistent per-model policies.

## Complete Example

```yaml
# Primary: EU vLLM agents with healthy GPU
routing_policy:
  match:
    region: "EU-France"
    engine: "vllm"
    tags: ["production"]
  exclude_if:
    kv_cache_utilization: { gt: 0.85 }
    gpu_temperature_c: { gt: 82 }
    success_rate: { lt: 0.95 }
    srtt: { gt: 500 }
  strategy: "least-loaded"
  max_tries: 3

# Fallback 1: Any region, relaxed filters
fallback_chain:
  - name: "any-region"
    match:
      engine: "vllm"
    exclude_if:
      success_rate: { lt: 0.90 }
    strategy: "least-loaded"
    max_tries: 2

# Last resort: OpenAI (set HIVENET_ROUTER_OPENAI_API_KEY in environment)
fallback_provider:
  engine: "openai"
  model: "gpt-4o-mini"
```

## Admission Control Fields

Front-door caps applied before a request enters the routing pipeline. Every field is optional; a value of `0` (or an omitted field) leaves that gate inert. See [Admission Control](05-Admission-Control.md) for concepts.

### Top-Level Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | string | `reserved` | `reserved` (shared circuit breaker only) or `serverless` (also enforces per-key caps from `auth.yaml`). An empty string normalises to `reserved`; unknown values are a validation error. |
| `max_input_tokens` | int | `0` | Reject any single request whose estimated prompt exceeds this many tokens with `400 input_too_long`. `0` disables. |
| `images_max` | int | `0` | Reject any single request carrying more `image_url` content parts than this with `400 input_too_long`. `0` disables. |
| `admit_budget_tokens` | int | `0` | KV-occupancy admit budget: total token-weighted in-flight work allowed per model. Scaled by [`HIVENET_ROUTER_ADMIT_FRACTION`](../Reference/01-Configuration-Reference.md) (default `0.85`). Over budget the request parks briefly (`HIVENET_ROUTER_ADMIT_PARK_TIMEOUT`, default `250ms`), then returns `429 concurrency_limit_exceeded` with `Retry-After`. `0` disables. |
| `max_inflight` | int | `0` | Concurrent-request backstop per model. Applies in parallel to `admit_budget_tokens`. `0` disables. |
| `shed_if` | object | `{}` | Front-door shed thresholds on live engine signals. See below. |

All caps are non-negative. Negative values are rejected at load.

There is deliberately **no output-token field**. The router never clamps `max_tokens`; the engine bounds output via `max_model_len`.

### `shed_if` Block

Same shape as `exclude_if` (one operator per field: `gt`, `lt`, `gte`, `lte`), but with a narrower field set — only signals that read "the box is full right now" are accepted. Unknown fields are rejected at load.

| Field | Type | Range | Description |
|---|---|---|---|
| `kv_cache_utilization` | float | 0.0–1.0 | Engine KV cache usage. |
| `waiting_requests` | float | 0+ | Requests queued in the engine's own scheduler. |

```yaml
shed_if:
  kv_cache_utilization: { gt: 0.95 }
  waiting_requests: { gt: 20 }
```

### Serverless Mode

Setting `mode: serverless` enables per-key caps in `auth.yaml`:

- `input_tokens_per_minute` and `output_tokens_per_minute` per-key token buckets
- `max_occupancy_share` — fraction of `admit_budget_tokens` the key may hold in flight at once

Per-key caps are ignored on `reserved` policies. See [auth.yaml Reference](../Security%20&%20Auth/03-auth.yaml-Reference.md).

**Startup invariant.** For every serverless policy, each API key that could reach the model must have `input_tokens_per_minute ≥ max_input_tokens`, otherwise the key's per-minute bucket would silently cap the usable context. The router refuses to start (and rejects a `SIGHUP` reload) if the invariant is violated.

### Example

```yaml
models:
  - "Qwen/Qwen3-32B"

mode: serverless
max_input_tokens: 32000
images_max: 10
admit_budget_tokens: 100000
max_inflight: 8
shed_if:
  kv_cache_utilization: { gt: 0.95 }
  waiting_requests: { gt: 20 }

routing_policy:
  match:
    engine: "vllm"
  strategy: "least-loaded"
```

## Loading Policy

### From File

```bash
./bin/hivenet-router --policy-file /etc/hivenet-router/policy.yaml
```

### From Directory (Per-Model)

```bash
./bin/hivenet-router --policy-model-dir /etc/hivenet-router/models/
```

Example directory layout:
```
/etc/hivenet-router/models/
  _default.yaml          # global policy override (optional)
  llama-8b.yaml          # models: ["meta-llama/Llama-3.1-8B-Instruct"]
  llama-70b.yaml         # models: ["meta-llama/Llama-3.1-70B-Instruct"]
```

### Ephemeral via API

```bash
# Replace global policy (lost on restart)
curl -X PUT -H "Content-Type: text/yaml" \
  -d '@policy.yaml' \
  http://localhost:8080/admin/policy

# Replace a named per-model policy (lost on restart)
curl -X PUT -H "Content-Type: text/yaml" \
  -d '@llama-8b.yaml' \
  http://localhost:8080/admin/policy/models/llama-8b
```

## Hot Reload

```bash
# Reload both --policy-file and --policy-model-dir from disk
kill -HUP $(pgrep hivenet-router)
```

SIGHUP reloads all policy sources atomically. In-flight requests complete under their current policy snapshot; new requests pick up the updated policy immediately.

Reload events are tracked by:

```promql
hivenet_router_policy_reload_total{trigger="sighup", result="success"}
hivenet_router_policy_reload_total{trigger="sighup", result="error"}
```

## Validation

Policy is validated on load. Rules enforced:

- `strategy` is required on every step; only `least-loaded` is accepted
- Unknown `exclude_if` field names are rejected (typos silently disable gates otherwise)
- Each `exclude_if` field must have exactly one operator (`gt`, `lt`, `gte`, or `lte`)
- `fallback_provider.engine` and `fallback_provider.model` are both required when the block is present
- `mode` must be `reserved` or `serverless`; an empty value normalises to `reserved`
- `max_input_tokens`, `images_max`, `admit_budget_tokens`, `max_inflight` must be `>= 0`
- Unknown `shed_if` field names are rejected (only `kv_cache_utilization` and `waiting_requests` are accepted); each field must specify exactly one operator
- On startup and every reload, for every `serverless` policy each key that can reach the model must satisfy `input_tokens_per_minute >= max_input_tokens`; a violation fails startup and rejects the reload

The router refuses to start with an invalid `--policy-file`. `PUT /admin/policy` returns HTTP 400 with the validation message on error.

## See Also

- [Routing Concepts](01-Routing-Concepts.md) - Pipeline explanation
- [Fallback Chains](03-Fallback-Chains.md) - Detailed fallback config
- [Provider Fallback](04-Provider-Fallback.md) - OpenAI/Anthropic setup
- [Admission Control](05-Admission-Control.md) - Modes, per-request caps, occupancy budget
- [Admin Endpoints](../API%20Reference/05-Admin-Endpoints.md) - Policy API
