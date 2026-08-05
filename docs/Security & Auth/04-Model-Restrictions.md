# Model Restrictions

Configure per-key model access control to enforce least privilege and isolate tenants.

## Access Control Modes

### Full Access

Empty models array grants access to all registered models:

```yaml
api:
  mode: "api-key"
  keys:
    - key_hash: "..."
      key_preview: "sk-...abc"
      metadata:
        name: "Full Access"
        owner: "acme-corp"
      models: []  # Empty = all models
```

### Restricted Access

Specify allowed models explicitly:

```yaml
keys:
  - key_hash: "..."
    key_preview: "sk-...def"
    metadata:
      name: "Beta Tester"
      owner: "beta-testers"
    models:
      - "meta-llama/Llama-3.1-8B-Instruct"
      - "mistralai/Mistral-7B-Instruct-v0.3"
```

### Single Model

Restrict to a single model:

```yaml
keys:
  - key_hash: "..."
    key_preview: "sk-...ghi"
    metadata:
      name: "Embeddings Service"
      owner: "search-team"
    models:
      - "BAAI/bge-m3"  # Embeddings only
```

## Use Cases

### Multi-Tenant Isolation

Isolate different tenants to their own models:

```yaml
api:
  mode: "api-key"
  keys:
    # Tenant A - Llama models
    - key_hash: "..."
      key_preview: "sk-...aaa"
      metadata:
        name: "Tenant A"
        owner: "tenant-a"
      models:
        - "meta-llama/Llama-3.1-8B-Instruct"
        - "meta-llama/Llama-3.1-70B-Instruct"

    # Tenant B - Mistral models
    - key_hash: "..."
      key_preview: "sk-...bbb"
      metadata:
        name: "Tenant B"
        owner: "tenant-b"
      models:
        - "mistralai/Mistral-7B-Instruct-v0.3"
```

### Service Isolation

Restrict internal services to specific model types:

```yaml
keys:
  # Chat service
  - key_hash: "..."
    metadata:
      name: "Chat Service"
      owner: "chat-service"
    models:
      - "meta-llama/Llama-3.1-8B-Instruct"

  # Embeddings service
  - key_hash: "..."
    metadata:
      name: "Embeddings Service"
      owner: "embeddings-service"
    models:
      - "BAAI/bge-m3"

  # Reranking service
  - key_hash: "..."
    metadata:
      name: "Reranking Service"
      owner: "rerank-service"
    models:
      - "BAAI/bge-reranker-v2-m3"
```

## Access Denied Response

When a key requests a model not in its whitelist:

```json
{
  "error": {
    "code": "model_forbidden",
    "message": "your API key does not have access to model: meta-llama/Llama-3.1-8B-Instruct",
    "source": "router"
  }
}
```

HTTP Status: 403

## Monitoring

### Requests by Tenant and Model

```promql
sum by (tenant_id, model) (rate(hivenet_router_routing_requests_routed_total[1h]))
```

### Access Denied Events

```logql
{job="router"} | json | error_code = "model_forbidden"
```

## See Also

- [auth.yaml Reference](03-auth.yaml-Reference.md) - Configuration schema
- [API Keys](02-API-Keys.md) - Key creation and management
- [Audit Logging](../Observability/03-Audit-Logging.md) - Usage tracking
