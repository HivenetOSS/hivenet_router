# vLLM Agent

Deploy a Hivenet Router agent connected to a vLLM inference backend.

> **One agent = one model.** Each agent process registers exactly one model with the router. If vLLM is serving multiple models, start one agent process per model and use `--model <name>` to pin each agent to its model.

---

## Prerequisites

- vLLM installed and running (see below)
- NVIDIA GPU with CUDA support
- Hivenet Router running and reachable

---

## Step 1: Start vLLM

### Docker

```bash
docker run -d --name vllm \
  --gpus all \
  --network host \
  -v /data/models:/data/models \
  vllm/vllm-openai:latest \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --host 0.0.0.0 \
  --port 8888 \
  --tensor-parallel-size 1 \
  --max-num-seqs 256
```

### Bare-Metal

```bash
pip install vllm

vllm serve meta-llama/Llama-3.1-8B-Instruct \
  --host 0.0.0.0 \
  --port 8888 \
  --tensor-parallel-size 1 \
  --max-num-seqs 256
```

Wait until vLLM is ready before starting the agent:

```bash
curl http://localhost:8888/health   # returns 200 when the model is loaded
```

---

## Step 2: Start the Agent

### Bare-Metal

```bash
./bin/hivenet-agent \
  --engine             vllm \
  --backend-url        http://localhost:8888 \
  --router-grpc        <ROUTER_IP>:50051 \
  --router-p2p         /ip4/<ROUTER_IP>/tcp/9000 \
  --jwt-secret-file    jwt.secret \
  --capacity           256 \
  --region             EU-France \
  --organization       ml-team \
  --machine            gpu-worker-1 \
  --identity-path      /opt/hivenet-router/agent_identity.key
```

Set `--capacity` to match vLLM's `--max-num-seqs` — this prevents the router from queuing more requests than vLLM can handle concurrently.

### Docker

```bash
docker run -d --name hivenet-agent \
  --network host \
  --gpus all \
  -v /path/to/jwt.secret:/jwt.secret:ro \
  hivenet-agent:latest \
    --engine             vllm \
    --backend-url        http://localhost:8888 \
    --router-grpc        <ROUTER_IP>:50051 \
    --router-p2p         /ip4/<ROUTER_IP>/tcp/9000 \
    --jwt-secret-file    /jwt.secret \
    --capacity           256 \
    --region             EU-France \
    --identity-path      /opt/hivenet-router/agent_identity.key
```

For Docker Compose, see [Docker Compose — Each GPU Server](../02-Docker-Compose.md#step-2-each-gpu-server).

---

## Model Selection

The agent connects to one model. There are two ways to specify it:

**Auto-detect (default):** The agent queries `GET /v1/models` at startup and registers the first model returned:

```
GET http://localhost:8888/v1/models
→ registers models[0]
```

**Explicit (`--model`):** Pin the agent to a specific model. Use this when vLLM serves multiple models — start one agent per model:

```bash
# Agent for Llama-3.1-8B
./bin/hivenet-agent --model meta-llama/Llama-3.1-8B-Instruct --engine vllm ...

# Separate agent for Llama-3.1-70B (different process, different --identity-path)
./bin/hivenet-agent --model meta-llama/Llama-3.1-70B-Instruct --engine vllm ...
```

Each agent registers as an independent peer with the router.

---

## Verify Connection

```bash
# On the router — check both agents registered
curl http://<ROUTER_IP>:8080/admin/health | jq .

# List models available to clients
curl http://<ROUTER_IP>:8080/v1/models | jq '.data[].id'

# Test inference
curl -X POST http://<ROUTER_IP>:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "meta-llama/Llama-3.1-8B-Instruct",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

---

## vLLM-Specific Metrics

Hivenet Router scrapes these vLLM metrics every 500 ms and aggregates them at the router's Prometheus endpoint (`:2112`). Agents have no separate Prometheus port.

| Metric | vLLM source | Description | Policy field |
|--------|-------------|-------------|--------------|
| KV cache utilization | `vllm:kv_cache_usage_perc` | Fraction of KV cache in use (0–1) | `kv_cache_utilization` |
| Running requests | `vllm:num_requests_running` | Requests currently being decoded | `running_requests` |
| Waiting requests | `vllm:num_requests_waiting` | Requests queued, not yet scheduled | `waiting_requests` |
| TTFT avg / P90 | `vllm:time_to_first_token_seconds` | Time to first output token | — |
| ITL avg / P90 | `vllm:inter_token_latency_seconds` | Per-token decode latency | — |
| Preemptions | `vllm:num_preemptions_total` | Cumulative KV-cache evictions | — |

### View Metrics

```bash
# On the router — all agent metrics are here
curl http://<ROUTER_IP>:2112/metrics | grep hivenet_router_agent_engine

# Or via the routing table
curl http://<ROUTER_IP>:8080/admin/routing-table | jq '.agents[] | {peer_id, kv_cache_utilization, running_requests}'
```

---

## Capacity Planning

Set `--capacity` to vLLM's `--max-num-seqs` (or slightly below for headroom). This is the maximum number of concurrent requests the agent will accept before the router stops sending it new ones.

```bash
# vLLM configured for 256 max sequences
vllm serve ... --max-num-seqs 256

# Match the agent capacity
--capacity 256
```

---

## Troubleshooting

**Agent not discovering a model:**

```bash
# Verify vLLM is up and returning a model
curl http://localhost:8888/health
curl http://localhost:8888/v1/models | jq '.data[].id'
```

Wait for vLLM to finish loading — `/health` returns 200 when ready, but model loading can take longer.

**Wrong model registered (auto-detect picked the wrong one):**

Use `--model` to pin the agent to the correct model name.

**High KV cache usage:**

```bash
# Reduce accepted concurrency
--capacity 50

# Or exclude the agent from routing when cache is full
# In policy YAML:
# exclude_if:
#   kv_cache_utilization: { gt: 0.85 }
```

**Preemptions increasing:**

vLLM is evicting in-flight KV caches under memory pressure. Reduce `--capacity` or lower `--max-num-seqs` in vLLM to match available VRAM.

**Backend requests timing out:**

```bash
# Increase the HTTP timeout (default: 5 minutes)
--http-timeout 10m
```

**Agent gets a new peer ID after restart:**

Ensure `--identity-path` points to a persistent file. The first start creates it; subsequent starts load it.

---

## Next Steps

- [Ollama Agent](02-Ollama.md) — Ollama backend setup
- [SGLang Agent](03-SGLang.md) — SGLang backend setup
- [Routing Concepts](../../Routing%20%26%20Policies/01-Routing-Concepts.md) — Configure routing policies
- [Hardware Metrics](../../Advanced/01-Hardware-Metrics.md) — GPU monitoring
