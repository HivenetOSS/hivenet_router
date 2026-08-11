# Admission Control

Admission control is the front door that keeps a replica from being overcommitted. Before a request is queued for routing, the router checks it against per-request caps and a token-weighted occupancy budget. Requests that would push a model over its budget are held briefly, then rejected with a clean `429` — so a burst of large prompts degrades cleanly instead of blowing out KV cache and inflating every co-tenant's latency.

There are three independent gates, each opt-in:

1. **Per-request caps** — reject any single request whose prompt or image count exceeds a hard maximum (`400 input_too_long`).
2. **KV-occupancy admit budget** — cap the token-weighted in-flight work per model (`429 concurrency_limit_exceeded`).
3. **Front-door shedding** — drop requests when live engine signals (KV cache utilization, waiting queue depth) cross a threshold.

All caps are optional. A cap set to `0` (or omitted) leaves that gate inert, so a policy that declares none of them behaves exactly as before.

The router applies no output-token clamp anywhere. `max_tokens` on the request body is forwarded verbatim; the engine bounds output via `max_model_len`.

## When to use it

Turn admission control on when a model's replicas can be pushed over capacity by a mix of clients:

- **Multi-tenant serverless models.** Several API keys share the same replica pool. One tenant's 32k prompt should not slow down every other tenant's 500-token chat. Set `mode: serverless`, declare `max_input_tokens`, `images_max`, and `admit_budget_tokens`, and set a per-key `max_occupancy_share` in `auth.yaml` for fairness.
- **Dedicated (reserved) models.** A single tenant rents the whole replica pool. Per-key fairness is not needed, but the same caps still protect the engine from a runaway client. Leave `mode: reserved` (the default) and set `admit_budget_tokens` and `max_inflight`.
- **Bursty embedding or vision workloads.** `images_max` and `max_input_tokens` let you reject an obviously oversized request up front rather than watching it eat a slot for 30 s before the backend rejects it.

If a model has one client that already self-regulates, admission control adds no value. Leave every field unset.

## Modes

Every policy declares a `mode`:

| Mode | Meaning |
|---|---|
| `reserved` (default) | Shared circuit-breaker limits only. No per-key caps. One client rents the whole replica pool. |
| `serverless` | Same circuit breaker, plus per-key caps for keys that share the replica pool. |

`mode` is normalised at load: an empty value becomes `reserved`. An unknown value is a validation error.

Per-key caps (`max_occupancy_share`, `input_tokens_per_minute`, `output_tokens_per_minute` in `auth.yaml`) are only enforced on models governed by a `serverless` policy. On a `reserved` model they are ignored.

## Per-request caps

Two hard caps rejected on the front door with `400 input_too_long`:

- **`max_input_tokens`** — the router estimates the prompt (text + a per-message floor) and rejects the request if it exceeds this number.
- **`images_max`** — the router counts `image_url` content parts across all messages and rejects the request if the count exceeds this number.

The token cap bounds text; the image cap bounds image payloads (image token cost is invisible to the text estimator). A text-only request is bounded only by `max_input_tokens`; an image-only request is bounded only by `images_max`.

The governing policy is resolved per model. A per-model named policy overrides the global policy; if neither declares a cap, the gate is inert for that model.

```yaml
# policy.yaml
mode: serverless
max_input_tokens: 32000   # reject prompts over ~32k tokens
images_max: 10            # reject requests with more than 10 images
```

Both caps are inclusive: a request with exactly `max_input_tokens` tokens is admitted; one with `max_input_tokens + 1` is not.

## KV-occupancy admit budget

A plain in-flight request counter cannot tell a small chat from a 32k prompt with 4k output. Three keys sending one huge request each do the same damage as one key sending three. The admit budget is **token-weighted**: it tracks Σ(footprint) over in-flight requests per model.

A request's footprint is `input_tokens + output_tokens`:

- A request that declares `max_tokens` (or `max_completion_tokens`) reserves that output up front.
- A request with no declared output reserves only its input estimate and grows live as the response streams.

A new request is admitted only while `sum(footprint) + this_request_footprint ≤ admit_fraction × admit_budget_tokens`. If it does not fit, the request parks for up to `admit_park_timeout` waiting for capacity to free. On timeout it is rejected with:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 1

{
  "error": {
    "code": "concurrency_limit_exceeded",
    "message": "server at capacity for this model, please retry",
    "source": "router"
  }
}
```

`max_inflight` is a plain concurrent-request backstop that applies in parallel. If it is set, no more than `max_inflight` requests may be in flight for the model at once, regardless of their token footprint.

The reservation is released on every request exit path — success, backend error, timeout, client disconnect, mid-stream failure, provider failover — so a slot never leaks and permanently shrinks the budget.

### The admit fraction

The budget is not `admit_budget_tokens` exactly. It is `admit_fraction × admit_budget_tokens`, where `admit_fraction` is a router-wide knob (default `0.85`). The 15% headroom absorbs error in the input-token estimator: the estimator sees text content only, so multimodal or tool-only content is under-counted. Lower the fraction if your prompts contain a lot of images; raise it once the estimator improves.

Configure via environment variables:

- `HIVENET_ROUTER_ADMIT_FRACTION` — float in `(0, 1]`. Default `0.85`. Values outside the range are ignored (the default is kept).
- `HIVENET_ROUTER_ADMIT_PARK_TIMEOUT` — Go duration string (e.g. `250ms`). Default `250ms`. `0` rejects immediately.

See [Configuration Reference](../Reference/01-Configuration-Reference.md).

### Worked example

A model with:

```yaml
# policy.yaml — models: ["Qwen/Qwen3-32B"]
mode: serverless
max_input_tokens: 32000
admit_budget_tokens: 100000
max_inflight: 8
```

and `admit_fraction: 0.85` has an effective budget of **85,000 tokens** per model.

| Time | Event | In-flight footprint | Decision |
|---|---|---|---|
| t=0 | Request A: 20k input, `max_tokens: 4000` | 24,000 | Admit |
| t=1 | Request B: 30k input, no `max_tokens` | 24,000 + 30,000 = 54,000 | Admit; B will grow as it streams |
| t=2 | Request C: 30k input, `max_tokens: 4000` | 54,000 + 34,000 = 88,000 | **Park** (over 85,000). Wait up to `admit_park_timeout`. |
| t=2.1 | Request A completes; releases 24,000 | 30,000 | C wakes, retries, admitted at 30,000 + 34,000 = 64,000 |

If A had not completed within `admit_park_timeout`, C would have received `429 concurrency_limit_exceeded` with `Retry-After: 1`. `max_inflight: 8` also caps the row above at 8 concurrent requests independent of size.

### Interaction with `max_occupancy_share`

On a `serverless` policy, each API key may declare `max_occupancy_share` in `auth.yaml` — the fraction of the admit budget that key may hold in flight at once. It sits at the key level (not inside `quota`) because it is measured against `admit_budget_tokens`, not a per-minute rate. Valid range is `(0, 1]`; unset means unlimited. See [auth.yaml Reference](../Security%20&%20Auth/03-auth.yaml-Reference.md).

### Startup invariant

For a `serverless` policy, every API key that could reach the model must have an `input_tokens_per_minute` bucket large enough to hold one maximum-size prompt (`input_tokens_per_minute ≥ max_input_tokens`). Otherwise the key's per-minute bucket would silently cap the usable context.

The router refuses to start if this invariant is violated, and rejects the reload — keeping the previous config — if a `SIGHUP` would introduce it. The error names the offending key and policy:

```
auth: key entry 2 ("sk-...abc") on serverless models [Qwen/Qwen3-32B]: input_tokens_per_minute (16000) is below the policy max_input_tokens (32000)
```

## Front-door shedding

`shed_if` drops requests when live engine signals cross a threshold, before they are queued. It uses the same operator syntax as `exclude_if`, but is narrower on purpose: only signals that read "the box is full right now" are accepted.

| Field | Meaning |
|---|---|
| `kv_cache_utilization` | KV cache usage as a fraction (0.0–1.0). |
| `waiting_requests` | Requests currently queued in the engine's own scheduler. |

Each field takes exactly one operator (`gt`, `lt`, `gte`, `lte`). Unknown field names are rejected at load — a typo would silently disable the gate.

```yaml
shed_if:
  kv_cache_utilization: { gt: 0.95 }
  waiting_requests: { gt: 20 }
```

## Complete example

A `serverless` model with all four gates on:

```yaml
# policy.yaml (or a per-model file under --policy-model-dir)
models:
  - "Qwen/Qwen3-32B"

mode: serverless

# Per-request caps — reject oversized requests up front (400 input_too_long)
max_input_tokens: 32000
images_max: 10

# Occupancy budget — token-weighted in-flight cap (429 concurrency_limit_exceeded)
admit_budget_tokens: 100000
max_inflight: 8

# Front-door shed — drop new requests when the engine is already saturated
shed_if:
  kv_cache_utilization: { gt: 0.95 }
  waiting_requests: { gt: 20 }

routing_policy:
  match:
    engine: "vllm"
  strategy: "least-loaded"
```

And a serverless key in `auth.yaml`, sized to satisfy the input-bucket invariant:

```yaml
api:
  mode: "api-key"
  keys:
    - key_hash: "…"
      key_preview: "sk-...abc"
      metadata:
        name: "Tenant A"
        owner: "tenant-a"
      max_occupancy_share: 0.25   # this key holds at most 25% of the admit budget
      quota:
        requests_per_minute: 200
        input_tokens_per_minute: 32000    # must cover max_input_tokens on serverless
        output_tokens_per_minute: 64000
        tokens_per_day: 5_000_000
```

## Error codes

| Code | HTTP | Trigger |
|---|---|---|
| `input_too_long` | 400 | Request exceeded `max_input_tokens` or `images_max` on the model's policy. |
| `concurrency_limit_exceeded` | 429 | The model's occupancy budget or `max_inflight` was full and the park window elapsed. Response includes `Retry-After: 1`. |

See [Error Codes](../Reference/02-Error-Codes.md).

## See also

- [Policy YAML Reference](02-Policy-YAML-Reference.md) — every admission field on a policy
- [auth.yaml Reference](../Security%20&%20Auth/03-auth.yaml-Reference.md) — per-key `max_occupancy_share`, `input_tokens_per_minute`, `output_tokens_per_minute`
- [Configuration Reference](../Reference/01-Configuration-Reference.md) — `HIVENET_ROUTER_ADMIT_FRACTION`, `HIVENET_ROUTER_ADMIT_PARK_TIMEOUT`
- [Error Codes](../Reference/02-Error-Codes.md) — `input_too_long`, `concurrency_limit_exceeded`
