# SGLang Agent

Deploy a Hivenet Router agent connected to an SGLang inference backend.

> **One agent = one model.** Each agent process registers exactly one model with the router. Use `--model <name>` to pin each agent to its model and start one agent process per model.

---

## Prerequisites

- SGLang installed and running (see below)
- NVIDIA GPU with CUDA support
- Hivenet Router running and reachable

---

## Step 1: Start SGLang

### Docker

```bash
docker run -d --name sglang \
  --gpus all \
  --network host \
  sglangai/sglang:latest \
  python3 -m sglang.launch_server \
    --model-path meta-llama/Llama-3.1-8B-Instruct \
    --host 0.0.0.0 \
    --port 3000 \
    --enable-metrics
```

### Bare-Metal

Install system dependency required by `sgl_kernel`:

```bash
sudo apt-get install -y libnuma-dev
pip install "sglang[all]"
```

Launch:

```bash
python3 -m sglang.launch_server \
  --model-path meta-llama/Llama-3.1-8B-Instruct \
  --host 0.0.0.0 \
  --port 3000 \
  --enable-metrics
```

For multi-GPU, use `--tp <num_gpus>` (e.g. `--tp 2` for two GPUs).

> **`--enable-metrics` is required** for Hivenet Router to scrape KV cache and request-queue metrics from SGLang.

Wait until SGLang is ready:

```bash
curl http://localhost:3000/health   # returns 200 when the model is loaded
```

---

## Step 2: Start the Agent

### Bare-Metal

```bash
./bin/hivenet-agent \
  --engine             sglang \
  --backend-url        http://localhost:3000 \
  --router-grpc        <ROUTER_IP>:50051 \
  --router-p2p         /ip4/<ROUTER_IP>/tcp/9000 \
  --jwt-secret-file    jwt.secret \
  --capacity           20 \
  --region             EU-France \
  --organization       ml-team \
  --identity-path      /opt/hivenet-router/agent_identity.key
```

### Docker

```bash
docker run -d --name hivenet-agent \
  --network host \
  --gpus all \
  -v /path/to/jwt.secret:/jwt.secret:ro \
  hivenet-agent:latest \
    --engine             sglang \
    --backend-url        http://localhost:3000 \
    --router-grpc        <ROUTER_IP>:50051 \
    --router-p2p         /ip4/<ROUTER_IP>/tcp/9000 \
    --jwt-secret-file    /jwt.secret \
    --capacity           20 \
    --region             EU-France \
    --identity-path      /opt/hivenet-router/agent_identity.key
```

For Docker Compose, see [Docker Compose — Each GPU Server](../02-Docker-Compose.md#step-2-each-gpu-server).

---

## SGLang-Specific Metrics

Hivenet Router scrapes these SGLang metrics every 500 ms (requires `--enable-metrics`). All metrics are aggregated at the router's Prometheus endpoint (`:2112`). Agents have no separate Prometheus port.

| Metric | SGLang source | Description | Policy field |
|--------|--------------|-------------|--------------|
| KV/token cache utilization | `sglang:token_usage` | Fraction of token cache in use (0–1) | `kv_cache_utilization` |
| Running requests | `sglang:num_running_reqs` | Requests currently being decoded | `running_requests` |
| Waiting requests | `sglang:num_queue_reqs` | Requests queued in the scheduler | `waiting_requests` |
| TTFT avg / P90 | `sglang:time_to_first_token_seconds` | Time to first output token | — |

> SGLang does not expose an ITL (inter-token latency) histogram — those fields are not populated for this engine.

### View Metrics

```bash
# On the router — all agent metrics are here
curl http://<ROUTER_IP>:2112/metrics | grep hivenet_router_agent_engine

# Or via the routing table
curl http://<ROUTER_IP>:8080/admin/routing-table | jq '.agents[] | {peer_id, kv_cache_utilization, running_requests}'
```

---

## Verify Connection

```bash
# On the router — confirm agent registered
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

## Troubleshooting

**Agent not connecting:**

```bash
curl http://localhost:3000/health
curl http://localhost:3000/v1/models
```

**Metrics not appearing in Grafana:**

Ensure SGLang was started with `--enable-metrics`. Without it, `/metrics` returns 404 and engine metrics will be absent.

**High latency:**

```bash
# Enable RadixAttention (prefix cache) — SGLang-specific flag
python3 -m sglang.launch_server ... --enable-radix-attn

# Increase KV cache budget
python3 -m sglang.launch_server ... --mem-fraction-static 0.9
```

**Agent gets a new peer ID after restart:**

Ensure `--identity-path` points to a persistent file. The first start creates it; subsequent starts load it.

---

## Next Steps

- [vLLM Agent](01-vLLM.md) — vLLM backend
- [Ollama Agent](02-Ollama.md) — Ollama backend
- [Custom Engine](04-Custom-Engine.md) — Any OpenAI-compatible backend
- [Routing Concepts](../../Routing%20%26%20Policies/01-Routing-Concepts.md) — Configure routing policies
