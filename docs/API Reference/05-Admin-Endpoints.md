# Admin Endpoints

Administrative APIs for monitoring and configuration.

## Authentication

Admin endpoints use Bearer token authentication. Set one or more admin keys (comma-separated) in the `HIVENET_ROUTER_ADMIN_API_KEYS` environment variable before starting the router, and enable admin auth in `auth.yaml`:

```yaml
# auth.yaml
admin:
  mode: "api-key"
  # Keys come from HIVENET_ROUTER_ADMIN_API_KEYS env var
```

```bash
export HIVENET_ROUTER_ADMIN_API_KEYS="your-admin-key"

curl -H "Authorization: Bearer your-admin-key" http://localhost:8080/admin/health
```

If `admin.mode` is `none` (the default when no `auth.yaml` is configured), admin endpoints are publicly accessible — restrict network access in production.

> **Dynamic mode is an exception.** When `HIVENET_ROUTER_AUTH_MODE=dynamic`, the router auto-elevates `admin.mode` to `api-key` regardless of `auth.yaml`, and requires `HIVENET_ROUTER_ADMIN_API_KEYS` to be set. Startup fails if it isn't. This guarantees the [API Key Management](#api-key-management) endpoints below are never reachable unauthenticated in dynamic deployments.

## Health Endpoints

### Minimal Health Check

```
GET /health
```

Public endpoint for load balancers.

```json
{"status":"ok"}
```

### Full Health Status

```
GET /admin/health
```

```json
{
  "status": "healthy",
  "timestamp": 1776870993,
  "total_agents": 7,
  "healthy_agents": 7,
  "queue_length": 0,
  "agents": [
    {
      "peer_id": "12D3KooWC2F9LUYFW6cACN2GNPXJwjhE69hMjNbBSSm3wRh715J3",
      "model": "openai/gpt-oss-20b",
      "engine": "vllm",
      "region": "EU",
      "capacity": 20,
      "is_healthy": true,
      "last_seen": 1776870991,
      "version": "13e0ea7a8298b9b5ec37a826e1b21a99b702d513"
    }
  ]
}
```

| Field | Description |
|-------|-------------|
| `status` | `"healthy"` if all agents healthy, `"degraded"` otherwise |
| `total_agents` | Total registered agents |
| `healthy_agents` | Agents currently healthy |
| `queue_length` | Pending requests in the wait queue |
| `agents[].model` | Model served by this agent |
| `agents[].is_healthy` | Whether the agent is healthy |
| `agents[].last_seen` | Unix timestamp of last heartbeat |
| `agents[].version` | Router git commit the agent connected on |

## Routing Table

### Get Routing Table

```
GET /admin/routing-table
```

Returns full agent state including metrics, hardware, and engine data. Response is an array of agents with `total` count.

```json
{
  "total": 2,
  "agents": [
    {
      "peer_id": "12D3KooWC2F9LUYFW6cACN2GNPXJwjhE69hMjNbBSSm3wRh715J3",
      "metadata": {
        "model": "openai/gpt-oss-20b",
        "engine": "vllm",
        "region": "EU",
        "organization": "Hivenet Router",
        "machine": "GPT-OSS-20B",
        "capacity": 20,
        "tags": ["production"],
        "capability": "llm",
        "llm_pretty_name": "Balanced · GPT OSS 20B",
        "llm_info": "Good balance between speed and quality."
      },
      "status": {
        "healthy": true,
        "backend_healthy": true,
        "active_requests": 0,
        "capacity_utilization": 0,
        "last_seen": "2026-04-22T15:16:46.505518138Z"
      },
      "universal": {
        "successful_requests_total": 3649,
        "failed_requests_total": 8,
        "success_rate": 0.9978,
        "input_tokens_total": 1329627,
        "output_tokens_total": 3408307,
        "rejected_requests_total": 0,
        "disconnections_total": 14,
        "agent_failures_total": 14,
        "backend_failures_total": 1,
        "srtt_ms": 804.01,
        "rttvar_ms": 1499.05,
        "latency_state": "KNOWN"
      },
      "engine": {
        "kv_cache_utilization": 0,
        "running_requests": 0,
        "waiting_requests": 0,
        "preemptions_total": 0,
        "avg_ttft_seconds": 0.106,
        "p90_ttft_seconds": 0.232,
        "avg_itl_seconds": 0.0059,
        "p90_itl_seconds": 0.0094
      },
      "hardware": {
        "gpu": [
          {
            "index": 0,
            "util_percent": 0,
            "vram_used_bytes": 24251727872,
            "vram_free_bytes": 1505492992,
            "vram_total_bytes": 25757220864,
            "temperature_c": 23,
            "power_watts": 10.447
          }
        ],
        "cpu": {
          "usage_percent": 2.3
        },
        "memory": {
          "used_percent": 7.28,
          "available_bytes": 92613185536,
          "total_bytes": 101247455232
        },
        "timestamp": "2026-04-22T15:16:46Z"
      }
    }
  ]
}
```

**Notes:**
- `engine` object is only present for vLLM and SGLang agents (not Ollama, Infinity, or Custom)
- `hardware.gpu` is an array — multi-GPU agents have multiple entries
- `universal.latency_state` is `"KNOWN"` once enough RTT samples exist, `"COLD"` initially
- `status.last_seen` is an ISO 8601 string; `agents[].last_seen` in `/admin/health` is a Unix timestamp

### Examples

```bash
# Get routing table
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/routing-table

# Find agents with high KV cache
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/routing-table \
  | jq '.agents[] | select(.engine.kv_cache_utilization > 0.8) | .peer_id'

# List all models and their SRTT
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/routing-table \
  | jq '.agents[] | {model: .metadata.model, srtt_ms: .universal.srtt_ms}'
```

## Models (Operator View)

Mirrors [`/v1/models`](04-Models-List.md) and `/v1/models/:model` but **bypasses the per-key allow-set filter** — operators always see every model registered in the cluster, regardless of which API key (admin or otherwise) the per-tenant filtering would apply to. Use this for dashboards, support tooling, and capacity-planning workflows where you need ground truth.

### List All Models

```
GET /admin/models
```

Returns the same JSON shape as `GET /v1/models` (`{"object": "list", "data": [...]}`) with **no filter applied** — even models that no tenant has a quota for (newly deployed, hidden, etc.) appear.

### Get Model Detail

```
GET /admin/models/{model}
```

Returns the same JSON shape as `GET /v1/models/:model` including the per-agent `agents.list` breakdown. Returns `404` only if no agent is registered for the model — never because of per-key filtering.

### Examples

```bash
# Full model catalog
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/models | jq '.data[].id'

# Hidden models (not exposed in /v1/models for any tenant)
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/models \
  | jq '.data[] | select(.hide_llm == true)'

# Detail for a single model
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  "http://localhost:8080/admin/models/Qwen/Qwen3.6-27B-A3B"
```

### When to use which

| Endpoint | Auth | View |
|---|---|---|
| `GET /v1/models` | tenant API key | filtered by `quota.per_model` keys or `models:` allow-list; full catalog only if neither is set |
| `GET /admin/models` | admin key | **always** full catalog |

## Storage Stats

### Get BadgerDB Stats

```
GET /admin/storage
```

```json
{
  "mem_db": {
    "metadata_count": 7,
    "univ_punctual_count": 7,
    "eng_punctual_count": 5,
    "hardware_snapshot_count": 7
  },
  "disk_db": {
    "univ_history_count": 10,
    "lsm_size_bytes": 1528,
    "vlog_size_bytes": 2147483666,
    "entry_ttl_days": 30
  },
  "gc_interval": "5s",
  "last_gc_at": "2026-04-22T15:16:57Z"
}
```

| Field | Description |
|-------|-------------|
| `mem_db.metadata_count` | Agents with metadata in memDB |
| `mem_db.univ_punctual_count` | Agents with live universal counters |
| `mem_db.eng_punctual_count` | Agents with live engine metrics |
| `mem_db.hardware_snapshot_count` | Agents with live hardware snapshots |
| `disk_db.univ_history_count` | Agent lifetime history records in diskDB |
| `disk_db.lsm_size_bytes` | BadgerDB LSM tree size |
| `disk_db.vlog_size_bytes` | BadgerDB value log size |
| `disk_db.entry_ttl_days` | TTL for diskDB entries (0 = no expiry) |
| `gc_interval` | How often BadgerDB GC runs |
| `last_gc_at` | Timestamp of last GC run |

## Metrics

### Reset Per-Agent Lifetime Counters

```
POST /admin/metrics/reset
```

Clears all **persisted lifetime per-agent counters** so dashboards reflect behaviour *since the reset* instead of historical totals. This affects, in one call:

- the on-disk `universalHistory:` records in BadgerDB (`disk_db.univ_history_count` → 0),
- the in-memory counter state, and
- the matching Prometheus series (`hivenet_router_agent_requests_success_total`, `..._failed_total`, `hivenet_router_agent_success_rate`, `..._disconnections_total`, `hivenet_router_agent_connection_resets_total`, `hivenet_router_agent_srtt_ms`, input/output tokens, etc.).

**Why:** per-agent counters are persisted to disk and re-seeded into Prometheus when an agent re-registers, so they survive a router restart. After deploying a change you would otherwise still see the *historical* error rate. Reset them to observe the change against a clean baseline.

**Not affected:** tenant/billing counters (`quotaTPD:` and `hivenet_router_tenant_*`), agent metadata, liveness gauges (`hivenet_router_routing_agent_info` / `_healthy`), and capacity utilization. Routing-level request counters live only in memory and already reset when the router process restarts (e.g. on deploy).

```json
{
  "status": "metrics reset",
  "message": "per-agent lifetime counters cleared (disk, in-memory, and Prometheus series)"
}
```

```bash
curl -X POST -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/metrics/reset
```

## Policy Endpoints

### Get Current Policy

```
GET /admin/policy
```

```json
{
  "routing_policy": {
    "match": {
      "engine": "vllm",
      "organization": "Hivenet Router",
      "tags": ["production"]
    },
    "exclude_if": {
      "capacity_utilization": {"gt": 0.9},
      "kv_cache_utilization": {"gt": 0.85},
      "gpu_temperature_c": {"gt": 80},
      "success_rate": {"lt": 0.8},
      "waiting_requests": {"gt": 20}
    },
    "strategy": "least-loaded",
    "max_tries": 3
  },
  "fallback_chain": [
    {
      "name": "relaxed",
      "match": {
        "engine": "vllm",
        "organization": "Hivenet Router"
      },
      "exclude_if": {
        "kv_cache_utilization": {"gt": 0.95}
      },
      "strategy": "least-loaded",
      "max_tries": 2
    },
    {
      "name": "last-resort",
      "match": {},
      "strategy": "least-loaded",
      "max_tries": 1
    }
  ]
}
```

### Update Policy (Ephemeral)

```
PUT /admin/policy
```

Replaces current policy (YAML body). **Ephemeral** — lost on restart. Use `--policy-file` + SIGHUP for persistence.

```bash
curl -X PUT -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  -H "Content-Type: text/yaml" \
  -d '
routing_policy:
  match:
    engine: "vllm"
  exclude_if:
    kv_cache_utilization: { gt: 0.85 }
  strategy: "least-loaded"
  max_tries: 3
' \
  http://localhost:8080/admin/policy
```

### Get Per-Model Policies

```
GET /admin/policy/models
```

Returns a map of policy name → policy document.

```json
{
  "gptoss": {
    "models": ["openai/gpt-oss-20b"],
    "routing_policy": {
      "match": {"engine": "vllm"},
      "exclude_if": {
        "gpu_temperature_c": {"gt": 85},
        "kv_cache_utilization": {"gt": 0.95}
      },
      "strategy": "least-loaded",
      "max_tries": 3
    },
    "fallback_chain": [
      {
        "name": "relaxed",
        "match": {"engine": "vllm", "organization": "Hivenet Router"},
        "exclude_if": {"kv_cache_utilization": {"gt": 0.98}},
        "strategy": "least-loaded",
        "max_tries": 2
      },
      {
        "name": "last-resort",
        "match": {},
        "strategy": "least-loaded",
        "max_tries": 1
      }
    ]
  },
  "bge-m3": {
    "models": ["BAAI/bge-m3"],
    "routing_policy": {
      "match": {"engine": "infinity"},
      "exclude_if": {"gpu_temperature_c": {"gt": 85}},
      "strategy": "least-loaded",
      "max_tries": 2
    }
  }
}
```

The key is the **policy name** (not the model name). Each document includes a `models` list of model IDs the policy applies to.

### Get Model Policy

```
GET /admin/policy/models/{name}
```

Returns the single named policy document.

### Update Model Policy

```
PUT /admin/policy/models/{name}
```

Creates or replaces a named policy (YAML body, ephemeral).

### Delete Model Policy

```
DELETE /admin/policy/models/{name}
```

## Examples

### Monitor Agent Health

```bash
#!/bin/bash
while true; do
  curl -s -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
    http://localhost:8080/admin/health \
    | jq '.agents[] | {model: .model, is_healthy}'
  sleep 5
done
```

### Check Model Distribution

```bash
curl -s -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/routing-table \
  | jq '[.agents[] | {model: .metadata.model, region: .metadata.region, srtt_ms: .universal.srtt_ms}]'
```

### Export Current Policy

```bash
curl -s -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/policy > current-policy.json
```

### Hot Reload Policy

```bash
# Update policy file
cat > /etc/hivenet-router/policy.yaml <<EOF
routing_policy:
  strategy: "least-loaded"
  max_tries: 3
EOF

# Send SIGHUP
kill -HUP $(pgrep hivenet-router)
```

## API Key Management

Endpoints for managing the dynamic API key registry at runtime. Only available when `HIVENET_ROUTER_AUTH_MODE=dynamic` — returns `501 Not Implemented` otherwise.

### Upsert API Key

```
PUT /admin/api-keys/:id
```

Adds or updates a single API key. Idempotent when version is newer.

**Request body:**

```json
{
  "version": "rev_00000002",
  "key_hash": "e3b0c44298fc1c149afbf4c8996fb924...",
  "key_preview": "sk-...KJ4",
  "owner": "acme-corp",
  "name": "Acme Production Key",
  "enabled": true,
  "expires_at": "01-01-2027",
  "allowed_models": ["meta-llama/Llama-3.1-8B-Instruct"],
  "quota": {
    "requests_per_minute": 1000,
    "tokens_per_day": 1000000
  }
}
```

**Response (200):**

```json
{"ok": true}
```

**Response (409 — stale version):**

```json
{"error": "stale version", "current_version": "rev_00000003"}
```

| Field | Required | Description |
|-------|----------|-------------|
| `version` | ✅ | Opaque monotonic version string (e.g., `rev_00000001`) |
| `key_hash` | ✅ | SHA-256 hex of the raw bearer token |
| `key_preview` | ❌ | Truncated display form (e.g., `sk-...KJ4`) |
| `owner` | ✅ | Tenant ID for billing and quota tracking |
| `name` | ✅ | Human-readable label for audit logs |
| `enabled` | ✅ | Whether the key is active |
| `expires_at` | ❌ | Expiry date `DD-MM-YYYY`; empty = never expires |
| `allowed_models` | ❌ | Model whitelist; empty = unrestricted |
| `quota.requests_per_minute` | ❌ | RPM limit; 0 = unlimited |
| `quota.tokens_per_day` | ❌ | Daily token limit; 0 = unlimited |

### Delete API Key

```
DELETE /admin/api-keys/:id?version=rev_00000002
```

Revokes a key by ID. Idempotent — returns 200 even if the key doesn't exist.

**Response (200):**

```json
{"ok": true}
```

The `version` query parameter must be newer than the current registry version, or a 409 Conflict is returned.

### Replace All API Keys

```
POST /admin/api-keys/replace
```

Atomically replaces the entire key registry. Always accepted (no version check) — used for bootstrap and reconciliation after router restart.

**Request body:**

```json
{
  "version": "rev_00000001",
  "keys": [
    {
      "id": "key-abc123",
      "key_hash": "e3b0c44298fc1c149afbf4c8996fb924...",
      "key_preview": "sk-...KJ4",
      "owner": "acme-corp",
      "name": "Acme Production Key",
      "enabled": true,
      "expires_at": "01-01-2027",
      "allowed_models": ["meta-llama/Llama-3.1-8B-Instruct"],
      "quota": {
        "requests_per_minute": 1000,
        "tokens_per_day": 1000000
      }
    }
  ]
}
```

**Response (200):**

```json
{"ok": true, "count": 1}
```

Each key entry in the `keys` array uses the same schema as the Upsert endpoint. The `id` field is required for each key in a replace operation.

### Get API Key Registry Version

```
GET /admin/api-keys/version
```

Returns the current registry version and key count.

**Response (200):**

```json
{"version": "rev_00000002", "count": 3}
```

### List All API Keys

```
GET /admin/api-keys
```

Returns all active key entries. **Key hashes are never included** — only metadata for operator visibility and drift detection.

**Response (200):**

```json
{
  "count": 2,
  "keys": [
    {
      "id": "acme-prod",
      "key_hash": "",
      "key_preview": "sk-...KJ4",
      "owner": "acme-corp",
      "name": "Acme Production Key",
      "enabled": true,
      "expires_at": "2027-01-01T00:00:00Z",
      "allowed_models": ["meta-llama/Llama-3.1-8B-Instruct"],
      "quota": {
        "requests_per_minute": 1000,
        "tokens_per_day": 1000000
      }
    }
  ]
}
```

| Field | Description |
|-------|-------------|
| `id` | Key entry ID |
| `key_hash` | Always empty string — hashes are never exposed |
| `key_preview` | Truncated display form (e.g., `sk-...KJ4`) |
| `owner` | Tenant ID |
| `name` | Human-readable label |
| `enabled` | Whether the key is active |
| `expires_at` | ISO 8601 expiry; `null` if never expires |
| `allowed_models` | Model whitelist; empty array = unrestricted |
| `quota` | Per-key rate and token limits |

### Get API Key by ID

```
GET /admin/api-keys/:id
```

Returns a single key entry by ID. **Key hash is never included.**

**Response (200):**

```json
{
  "key": {
    "id": "acme-prod",
    "key_hash": "",
    "key_preview": "sk-...KJ4",
    "owner": "acme-corp",
    "name": "Acme Production Key",
    "enabled": true,
    "expires_at": "2027-01-01T00:00:00Z",
    "allowed_models": ["meta-llama/Llama-3.1-8B-Instruct"],
    "quota": {
      "requests_per_minute": 1000,
      "tokens_per_day": 1000000
    }
  }
}
```

**Response (404 — key not found):**

```json
{"error": "key not found: acme-prod"}
```

### Key Management Examples

```bash
# Bootstrap all keys after router starts
curl -X POST \
  -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  -H "Content-Type: application/json" \
  -d @keys-snapshot.json \
  http://localhost:8080/admin/api-keys/replace

# List all active keys (no hashes exposed)
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/api-keys

# Get a single key by ID
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/api-keys/acme-prod

# Check current version
curl -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  http://localhost:8080/admin/api-keys/version

# Revoke a compromised key
curl -X DELETE \
  -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  "http://localhost:8080/admin/api-keys/key-abc123?version=rev_00000003"

# Update a key's quota
curl -X PUT \
  -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  -H "Content-Type: application/json" \
  -d '{
    "version": "rev_00000004",
    "key_hash": "...",
    "owner": "acme-corp",
    "name": "Acme Production Key",
    "enabled": true,
    "quota": {"requests_per_minute": 2000, "tokens_per_day": 2000000}
  }' \
  http://localhost:8080/admin/api-keys/key-abc123
```

### Security Notes

- Raw key hashes are **never** echoed in any response
- Version monotonicity prevents stale data from overwriting newer state
- `POST /admin/api-keys/replace` bypasses version check for bootstrap after restart
- These endpoints are the key-management surface, so admin auth is mandatory in dynamic mode. The router automatically elevates `admin.mode` to `api-key` when `HIVENET_ROUTER_AUTH_MODE=dynamic` and requires `HIVENET_ROUTER_ADMIN_API_KEYS` to be set; startup fails with `auth: admin section mode=api-key but HIVENET_ROUTER_ADMIN_API_KEYS env var is not set` if it's missing. There is no env-var-only configuration path that leaves `/admin/*` public in dynamic mode.

## Error Responses

### Unauthorized

```json
{"error": "unauthorized"}
```

HTTP Status: 401

## See Also

- [Chat Completions](01-Chat-Completions.md) - User-facing API
- [Routing Policy](../Routing%20%26%20Policies/02-Policy-YAML-Reference.md) - Policy format
- [Prometheus Metrics](../Observability/01-Prometheus-Metrics.md) - Metrics endpoint
