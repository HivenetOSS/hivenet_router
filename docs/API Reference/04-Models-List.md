# Models List

Discover available models and their agent distribution.

> **Per-key filtering.** Both endpoints below return only the models the
> calling API key is allowed to use. A key with [per-model quotas](../Security%20%26%20Auth/03-auth.yaml-Reference.md#per-model-quotas)
> sees exactly the models declared in its `quota.per_model` map; a key with a
> non-empty [`models:` allow-list](../Security%20%26%20Auth/03-auth.yaml-Reference.md#model-whitelist)
> sees only those; a key with neither restriction sees the full catalog.
> Unauthorised models return `404` (not `403`) so the cluster's full model
> list is not leaked across tenants.
>
> Operators who need ground truth (every model registered in the cluster,
> regardless of per-key filtering) should use the admin twin endpoints
> [`/admin/models` and `/admin/models/:model`](05-Admin-Endpoints.md#models-operator-view)
> instead — same JSON shape, no filter applied.

## List All Models

### Endpoint

```
GET /v1/models
```

### Response

```json
{
  "object": "list",
  "data": [
    {
      "id": "openai/gpt-oss-20b",
      "object": "model",
      "pretty_name": "Balanced · GPT OSS 20B",
      "info": "Good balance between speed and quality.",
      "capability": "llm",
      "agents": {
        "total": 2,
        "healthy": 2,
        "total_capacity": 40,
        "engines": ["vllm"],
        "regions": ["EU", "US"]
      }
    },
    {
      "id": "BAAI/bge-m3",
      "object": "model",
      "pretty_name": "Embeddings · BGE-M3",
      "info": "Multilingual dense embeddings for retrieval.",
      "capability": "embedding",
      "hide_llm": true,
      "agents": {
        "total": 1,
        "healthy": 1,
        "total_capacity": 20,
        "engines": ["infinity"],
        "regions": ["EU"]
      }
    },
    {
      "id": "BAAI/bge-reranker-large",
      "object": "model",
      "pretty_name": "Reranker · BGE-Large",
      "info": "Cross-encoder reranker for RAG pipelines.",
      "capability": "reranker",
      "hide_llm": true,
      "agents": {
        "total": 1,
        "healthy": 1,
        "total_capacity": 20,
        "engines": ["infinity"],
        "regions": ["EU"]
      }
    }
  ]
}
```

### Response Fields

| Field | Description |
|-------|-------------|
| `id` | Model name as registered by the agent |
| `object` | Always `"model"` |
| `pretty_name` | Human-readable display name (from `--llm-pretty-name`) |
| `info` | Short description (from `--llm-info`) |
| `capability` | `llm`, `embedding`, or `reranker` |
| `hide_llm` | `true` if the model is hidden from the public listing (from `--hide-llm`) |
| `agents.total` | Total agents serving this model |
| `agents.healthy` | Agents currently healthy |
| `agents.total_capacity` | Sum of capacity across all agents |
| `agents.engines` | Deduplicated list of backend engines |
| `agents.regions` | Deduplicated list of regions |

### Examples

```bash
curl http://localhost:8080/v1/models
```

```bash
# Filter embedding models
curl http://localhost:8080/v1/models | jq '.data[] | select(.capability == "embedding")'

# Filter reranker models
curl http://localhost:8080/v1/models | jq '.data[] | select(.capability == "reranker")'

# List model IDs only
curl http://localhost:8080/v1/models | jq '.data[].id'
```

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080",
    api_key="sk-hivenet-..."
)

models = client.models.list()
for model in models.data:
    print(model.id)
```

## Get Model Detail

### Endpoint

```
GET /v1/models/{model}
```

Returns the same fields as the list endpoint plus a per-agent breakdown in `agents.list`.

### Response

```json
{
  "id": "openai/gpt-oss-20b",
  "object": "model",
  "pretty_name": "Balanced · GPT OSS 20B",
  "info": "Good balance between speed and quality.",
  "capability": "llm",
  "agents": {
    "total": 2,
    "healthy": 2,
    "total_capacity": 40,
    "engines": ["vllm"],
    "regions": ["EU", "US"],
    "list": [
      {
        "peer_id": "12D3Koo0...",
        "engine": "vllm",
        "region": "EU",
        "organization": "ml-team",
        "machine": "gpu-worker-1",
        "capacity": 20,
        "is_healthy": true,
        "last_seen": 1713724800
      },
      {
        "peer_id": "12D3Koo1...",
        "engine": "vllm",
        "region": "US",
        "organization": "ml-team",
        "machine": "gpu-worker-2",
        "capacity": 20,
        "is_healthy": true,
        "last_seen": 1713724800
      }
    ]
  }
}
```

### Examples

```bash
# URL-encode slashes or quote the URL
curl "http://localhost:8080/v1/models/openai/gpt-oss-20b"
```

```bash
# Check per-agent health
curl "http://localhost:8080/v1/models/openai/gpt-oss-20b" \
  | jq '.agents.list[] | {peer_id, region, is_healthy}'
```

## Model Capabilities

| Capability | Endpoints | Agent Flag |
|------------|-----------|------------|
| `llm` | `/v1/chat/completions` | `--capability llm` (default) |
| `embedding` | `/v1/embeddings` | `--capability embedding` |
| `reranker` | `/v1/rerank` | `--capability reranker` |

## Error Responses

### Model Not Found

```json
{
  "error": {
    "code": "validation_model_not_found",
    "message": "Model 'gpt-4' not found"
  }
}
```

HTTP Status: 404

## See Also

- [Chat Completions](01-Chat-Completions.md) - Use models for inference
- [Embeddings](02-Embeddings.md) - Embedding generation
- [Reranking](03-Reranking.md) - Reranking endpoint
- [Admin Endpoints](05-Admin-Endpoints.md) - Administrative APIs
- [Routing Concepts](../Routing%20%26%20Policies/01-Routing-Concepts.md) - How models are routed
