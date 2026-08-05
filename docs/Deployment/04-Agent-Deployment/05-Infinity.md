# Infinity Agent

Deploy a Hivenet Router agent connected to an [Infinity](https://github.com/michaelfeil/infinity) embedding and reranking server.

> **Embeddings and reranking only.** The `infinity` engine does not support chat completions. Use `--capability embedding` or `--capability reranker` when starting the agent.

> **One agent = one model.** Each agent process registers exactly one model with the router. Infinity itself can serve multiple models in a single process — start one agent per model, all pointing to the same backend URL with `--model` to pin each agent to its model.

---

## Prerequisites

- [michaelfeil/infinity](https://github.com/michaelfeil/infinity) installed and running (see below)
- Hivenet Router running and reachable

---

## Step 1: Start Infinity

Infinity supports serving multiple models in a single process via repeatable `--model-id` / `--served-model-name` flags. `--served-model-name` sets the model name exposed in the API (used in `--model` of agent and client requests).

`INFINITY_URL_PREFIX="/v1"` is required so the API is served at `/v1/embeddings`, `/v1/rerank`, and `/v1/models`.

### Docker

```bash
export INFINITY_URL_PREFIX="/v1"

docker run -d --name infinity \
  --gpus all \
  -p 7997:7997 \
  -v ~/models:/models \
  -e INFINITY_URL_PREFIX="/v1" \
  michaelf34/infinity:latest \
  v2 \
    --model-id BAAI/bge-m3 \
    --model-id BAAI/bge-reranker-large \
    --served-model-name bge-m3 \
    --served-model-name bge-reranker-large \
    --host 0.0.0.0 \
    --port 7997 \
    --device cuda \
    --dtype float16 \
    --batch-size 32
```

### Bare-Metal

```bash
pip install "infinity-emb[all]"

export INFINITY_URL_PREFIX="/v1"

nohup infinity_emb v2 \
  --model-id BAAI/bge-m3 \
  --model-id BAAI/bge-reranker-large \
  --served-model-name bge-m3 \
  --served-model-name bge-reranker-large \
  --host 0.0.0.0 \
  --port 7997 \
  --device cuda \
  --dtype float16 \
  --batch-size 32 \
  > ~/infinity.log 2>&1 &
```

Wait until Infinity is ready:

```bash
curl http://localhost:7997/health        # returns 200 when ready
curl http://localhost:7997/v1/models     # should list bge-m3 and bge-reranker-large
```

---

## Step 2: Start the Agents

Both agents point to the same Infinity instance. Use `--model` to pin each agent to its model and `--capability` to declare what it serves.

### Embedding Agent

```bash
./bin/hivenet-agent \
  --engine             infinity \
  --capability         embedding \
  --model              bge-m3 \
  --backend-url        http://localhost:7997 \
  --router-grpc        <ROUTER_IP>:50051 \
  --router-p2p         /ip4/<ROUTER_IP>/tcp/9000 \
  --jwt-secret-file    jwt.secret \
  --capacity           64 \
  --region             EU-Embeddings \
  --identity-path      /opt/hivenet-router/agent_embed.key
```

### Reranking Agent

```bash
./bin/hivenet-agent \
  --engine             infinity \
  --capability         reranker \
  --model              bge-reranker-large \
  --backend-url        http://localhost:7997 \
  --router-grpc        <ROUTER_IP>:50051 \
  --router-p2p         /ip4/<ROUTER_IP>/tcp/9000 \
  --jwt-secret-file    jwt.secret \
  --capacity           32 \
  --region             EU-Reranking \
  --identity-path      /opt/hivenet-router/agent_reranker.key
```

> When running multiple agents on the same host, each needs a different `--identity-path`. libp2p ports are picked automatically — nothing dials the agent.

---

## Verify Connection

```bash
# On the router — confirm both agents registered
curl http://<ROUTER_IP>:8080/admin/health | jq .

# Test embeddings
curl -X POST http://<ROUTER_IP>:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "bge-m3",
    "input": ["Hello world", "How are you?"]
  }'

# Test reranking
curl -X POST http://<ROUTER_IP>:8080/v1/rerank \
  -H "Content-Type: application/json" \
  -d '{
    "model": "bge-reranker-large",
    "query": "What is the capital of France?",
    "documents": [
      "Paris is the capital of France.",
      "London is the capital of the United Kingdom."
    ]
  }'
```

---

## Routing Policy

```yaml
routing_policy:
  match:
    engine: "infinity"
    region: "EU-Embeddings"
  exclude_if:
    success_rate: { lt: 0.95 }
  strategy: "least-loaded"

fallback_chain:
  - name: "reranker"
    match:
      engine: "infinity"
      region: "EU-Reranking"
    strategy: "least-loaded"
```

---

## Monitoring

```promql
# Embedding request rate
rate(hivenet_router_routing_requests_routed_total{model="bge-m3"}[1h])

# Reranking request rate
rate(hivenet_router_routing_requests_routed_total{model="bge-reranker-large"}[1h])

# SRTT per Infinity agent
hivenet_router_agent_srtt_ms{engine="infinity"}
```

---

## Troubleshooting

**`/v1/models` returns 404:**

Ensure `INFINITY_URL_PREFIX="/v1"` is set before starting Infinity. Without it, API paths are served at `/` instead of `/v1/`.

**Agent not discovering model:**

```bash
curl http://localhost:7997/health
curl http://localhost:7997/v1/models | jq '.data[].id'
```

The names returned must match the `--served-model-name` values used at Infinity launch and the `--model` values passed to each agent.

**Running multiple agents on the same host:**

Each agent only needs its own `--identity-path` — libp2p ports are picked automatically, and nothing dials the agent.

**Agent gets a new peer ID after restart:**

Ensure `--identity-path` points to a persistent file. The first start creates it; subsequent starts load it.

---

## See Also

- [Custom Engine](04-Custom-Engine.md) — Any OpenAI-compatible backend
- [Routing Concepts](../../Routing%20%26%20Policies/01-Routing-Concepts.md) — Policy routing
