# Docker Quickstart

Deploy Hivenet Router across multiple machines with Docker. This guide walks you through a minimal two-agent cluster: one Router that receives client requests and two GPU Agent servers that serve inference.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Requests                           │
│              POST /v1/chat/completions                       │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP :8080
                           ▼
            ┌────────────────────────────┐
            │         Router             │
            │   :8080  HTTP API          │
            │   :50051 gRPC auth         │
            │   :9000  libp2p P2P        │
            │   :2112  Prometheus        │
            └──────────▲────────────────┘
                       │ agents connect outbound (gRPC :50051, libp2p :9000)
          ┌────────────┴────────────┐
          │                         │
┌──────────────────┐     ┌──────────────────┐
│   Agent (gpu-1)  │     │   Agent (gpu-2)  │
│   (outbound only)│     │   (outbound only)│
│   vLLM   :8888   │     │   vLLM   :8888   │
└──────────────────┘     └──────────────────┘
```

### Connection flow

1. **Agent → Router (gRPC :50051):** Agent authenticates with the router, receives a session token and the router's libp2p address.
2. **Agent → Router (libp2p :9000):** Agent connects to the router's libp2p node and registers itself. The connection stays open.
3. **Router → Agent (same connection):** For every inference request, the router forwards the request back over the libp2p connection the agent already opened.

This means the agent initiates **all** connections — inference requests are forwarded back over the agent's own libp2p connection, so agents need **no inbound ports, no public IP, and no firewall ingress rules**, and work behind NAT with zero network configuration.

## Prerequisites

- **3 machines** (or VMs):
  - 1 Router server (any machine, no GPU required)
  - 2+ Agent servers (NVIDIA GPU required for inference)
- **Docker 20.10+** and **Docker Compose 2.0+** on all machines
- **NVIDIA driver** already installed on GPU servers (e.g. via `ubuntu-drivers autoinstall`)
- **Network:** Agent servers can reach the Router on TCP ports 50051 (gRPC) and 9000 (libp2p) — outbound only; agents need no inbound reachability

## Deployment Topology

| Machine | Role | Example IP | GPU |
|---------|------|------------|-----|
| router-server | Router | `192.168.1.100` | None |
| gpu-eu-1 | Agent + vLLM | `192.168.1.101` | NVIDIA A100 |
| gpu-eu-2 | Agent + vLLM | `192.168.1.102` | NVIDIA A100 |

---

## Step 0: Prepare Each GPU Server

Run the setup script once on **each GPU agent server**. It installs Docker, the NVIDIA Container Toolkit, and host compilation packages for inference engines.

```bash
git clone https://github.com/HivenetOSS/hivenet_router.git
cd hivenet_router
chmod +x scripts/setup-agent-host.sh

sudo ./scripts/setup-agent-host.sh
```

The script will:
- Verify the NVIDIA driver is present (`nvidia-smi`)
- Install Docker CE + Compose plugin (skipped if already installed)
- Install and configure the NVIDIA Container Toolkit so Docker containers can access the GPU
- Install `nvidia-cuda-toolkit` and `python3.12-dev` for engines that compile native extensions

> No firewall configuration is needed on agent servers — agents make outbound-only connections to the router and require no open inbound ports.

---

## Step 1: Generate JWT Secret

The JWT secret authenticates agents against the router. All machines must share the same secret.

**On router-server:**

```bash
openssl rand -hex 32 > jwt.secret
chmod 600 jwt.secret
```

Copy to all agent servers:

```bash
scp jwt.secret gpu-eu-1:/opt/hivenet-router/jwt.secret
scp jwt.secret gpu-eu-2:/opt/hivenet-router/jwt.secret
```

---

## Step 2: Start the Router (router-server)

```bash
mkdir -p $(pwd)/badger

docker run -d \
  --name hivenet-router \
  --network host \
  -v $(pwd)/jwt.secret:/jwt.secret:ro \
  -v $(pwd)/badger:/badger \
  hivenet-router:latest \
  --jwt-secret-file /jwt.secret \
  --grpc-port :50051 \
  --p2p-port 9000 \
  --http-port :8080 \
  --metrics-port :2112 \
  --disk-db-path /badger
```

Verify the router is healthy:

```bash
curl http://localhost:8080/admin/health
```

Expected: `{"status":"ok","agents":0}` (zero agents registered yet).

> **Firewall on router-server:** open TCP 8080 (clients), 50051 and 9000 (agents). Keep 2112 (Prometheus) restricted to your monitoring network.

---

## Step 3: Start Agent on gpu-eu-1

Required flags for multi-machine deployments:
- `--router-p2p /ip4/<ROUTER_IP>/tcp/9000` — the router's libp2p address (agent uses this to register after gRPC auth)
- `--identity-path` — persists the agent's Ed25519 key so it keeps the same peer ID across restarts

The agent needs no address configuration of its own — it connects outbound to the router and inference requests are forwarded back over that connection.

**Start the inference backend (vLLM):**

```bash
docker run -d \
  --name vllm \
  --gpus all \
  --network host \
  -v /path/to/models:/models \
  vllm/vllm-openai:latest \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --host 0.0.0.0 \
  --port 8888 \
  --max-num-seqs 32
```

Wait for vLLM to finish loading the model:

```bash
curl http://localhost:8888/health   # returns 200 when ready
```

**Start the agent:**

```bash
docker run -d \
  --name hivenet-agent \
  --network host \
  --gpus all \
  -v /opt/hivenet-router/jwt.secret:/jwt.secret:ro \
  -v /opt/hivenet-router:/data \
  hivenet-agent:latest \
  --router-grpc        192.168.1.100:50051 \
  --router-p2p         /ip4/192.168.1.100/tcp/9000 \
  --jwt-secret-file    /jwt.secret \
  --engine             vllm \
  --backend-url        http://localhost:8888 \
  --capacity           32 \
  --region             EU-Primary \
  --identity-path      /data/agent_identity.key
```

`--capacity 32` is the maximum number of concurrent requests this agent will accept. Set it to match vLLM's `--max-num-seqs`.

Check the agent connected:

```bash
docker logs hivenet-agent | grep -i "registered\|connected\|error"
```

---

## Step 4: Start Agent on gpu-eu-2

```bash
# Start vLLM backend
docker run -d \
  --name vllm \
  --gpus all \
  --network host \
  -v /path/to/models:/models \
  vllm/vllm-openai:latest \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --host 0.0.0.0 \
  --port 8888 \
  --max-num-seqs 32

# Start agent (note: different region)
docker run -d \
  --name hivenet-agent \
  --network host \
  --gpus all \
  -v /opt/hivenet-router/jwt.secret:/jwt.secret:ro \
  -v /opt/hivenet-router:/data \
  hivenet-agent:latest \
  --router-grpc        192.168.1.100:50051 \
  --router-p2p         /ip4/192.168.1.100/tcp/9000 \
  --jwt-secret-file    /jwt.secret \
  --engine             vllm \
  --backend-url        http://localhost:8888 \
  --capacity           32 \
  --region             EU-Secondary \
  --identity-path      /data/agent_identity.key
```

The agent queries `GET /v1/models` at startup and registers the first model returned. One agent = one model — see [vLLM Agent](04-Agent-Deployment/01-vLLM.md) for multi-model setups.

---

## Step 5: Verify Registration

**On router-server:**

```bash
curl http://localhost:8080/admin/health | jq .
```

Expected output (two agents registered):

```json
{
  "status": "ok",
  "agents": 2,
  "agent_list": [
    {
      "peer_id": "12D3KooW...",
      "region": "EU-Primary",
      "engine": "vllm",
      "model": "meta-llama/Llama-3.1-8B-Instruct",
      "capacity": 32,
      "healthy": true,
      "backend_healthy": true
    },
    {
      "peer_id": "12D3KooX...",
      "region": "EU-Secondary",
      "engine": "vllm",
      "model": "meta-llama/Llama-3.1-8B-Instruct",
      "capacity": 32,
      "healthy": true,
      "backend_healthy": true
    }
  ]
}
```

For a full snapshot including live SRTT, hardware, and engine metrics:

```bash
curl http://localhost:8080/admin/routing-table | jq .
```

---

## Step 6: Test Inference

From any machine that can reach the router's HTTP port:

```bash
curl -X POST http://192.168.1.100:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "meta-llama/Llama-3.1-8B-Instruct",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

> API auth is disabled by default (all requests pass through as tenant `default`). To enable key-based auth, pass `--auth-config-file /path/to/auth.yaml` to the router and include `Authorization: Bearer <key>` in requests. See [API Keys](../Security%20%26%20Auth/02-API-Keys.md).

---

## Step 7: Observe Load Distribution

Send multiple requests and check how the router distributes them:

```bash
# Send 10 requests in parallel
for i in {1..10}; do
  curl -s http://192.168.1.100:8080/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{"model":"meta-llama/Llama-3.1-8B-Instruct","messages":[{"role":"user","content":"Hi"}]}' &
done
wait

# Check routing counters per agent (replace peer_id values)
curl -s http://192.168.1.100:2112/metrics | grep hivenet_router_routing_requests_routed_total
```

---

## Port Reference

| Service | Host | Port | Open to |
|---------|------|------|---------|
| Router HTTP API | router | 8080 | Clients |
| Router gRPC auth | router | 50051 | Agent servers only |
| Router libp2p P2P | router | 9000 | Agent servers only |
| Router Prometheus | router | 2112 | Monitoring network only |
| vLLM backend | each agent | 8888 | Localhost only (not exposed) |

Agents need no open inbound ports at all — they only make outbound connections to the router (50051 and 9000). The vLLM backend port (8888) never needs to be opened externally — the agent accesses it on localhost inside the same host.

---

## Single-Machine Testing

For local testing on one machine (no public addresses needed):

```bash
git clone https://github.com/HivenetOSS/hivenet_router.git
cd hivenet_router
openssl rand -hex 32 > jwt.secret

# Start everything with Docker Compose
HIVENET_ROUTER_JWT_SECRET="$(cat jwt.secret)" docker compose up -d
```

The Compose file wires the router and a local agent on the same Docker network. Agents never need address configuration — in any deployment they simply dial out to the router and receive inference requests back over that connection.

---

## Cleanup

```bash
# On each agent server
docker stop hivenet-agent vllm && docker rm hivenet-agent vllm

# On router-server
docker stop hivenet-router && docker rm hivenet-router
```

---

## Troubleshooting

### Agent not registering

```bash
# Check agent logs for connection errors
docker logs hivenet-agent

# Verify the agent can reach the router's gRPC port
nc -zv 192.168.1.100 50051

# Verify the agent can reach the router's libp2p port
nc -zv 192.168.1.100 9000
```

Common causes:
- **Wrong `--router-grpc` address** — must be the router's public IP, not localhost
- **Outbound connectivity blocked** — the agent must be able to make egress connections to the router on 50051 (gRPC) and 9000 (libp2p); check any egress firewall or proxy on the agent host

### vLLM not ready

```bash
docker logs vllm
# Wait for "Application startup complete" before starting the agent
curl http://localhost:8888/health
```

### Firewall on router-server

```bash
# Open ports agents connect to (restrict source to agent IPs in production)
ufw allow 50051/tcp   # gRPC
ufw allow 9000/tcp    # libp2p

# Open the HTTP API only to trusted clients
ufw allow 8080/tcp

# Prometheus — restrict to monitoring network, never expose publicly
ufw allow from <monitoring-ip> to any port 2112 proto tcp
```

### GPU metrics not showing

```bash
# Verify NVIDIA Container Toolkit is configured
docker info | grep -i nvidia
nvcc --version

# The agent container must run with --gpus all for NVML access
docker inspect hivenet-agent | grep -A5 DeviceRequests
```

---

## Next Steps

- [Docker Compose Full Stack](02-Docker-Compose.md) — Add Prometheus + Grafana observability
- [Agent Deployment](04-Agent-Deployment/) — vLLM, Ollama, llama.cpp, SGLang configurations
- [Routing Concepts](../Routing%20%26%20Policies/01-Routing-Concepts.md) — Configure routing policies
