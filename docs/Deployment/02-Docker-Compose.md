# Docker Compose — Full Observability Stack

The repository ships two Compose files. This guide covers both and explains how to combine them for a production multi-machine deployment.

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Router + full observability stack (Prometheus, Grafana, Tempo, Loki, Promtail) — run on the router server |
| `docker-compose.agent.yml` | Agent — run on each GPU server |

> **GPU host preparation:** before starting agents, run `setup-agent-host.sh` on each GPU server as described in [Docker Quickstart — Step 0](01-Docker-Quickstart.md#step-0-prepare-each-gpu-server). That script installs Docker and the NVIDIA Container Toolkit.

---

## Stack Overview

### `docker-compose.yml` — Router + Observability

```
┌─────────────────────────────────────────────────────────────────┐
│  Router Server                                                  │
│                                                                 │
│  hivenet-router   :8888  HTTP API (clients)                  │
│                      :8902  gRPC auth (agents)                  │
│                      :8903  libp2p P2P (agents + inference)     │
│                      :2112  Prometheus metrics (internal only)  │
│                                                                 │
│  prom/prometheus     :9090  (internal only — Grafana reads it)  │
│  grafana/grafana     :3000  Dashboard UI                        │
│  grafana/tempo       :4317  OTLP trace ingestion (internal)     │
│  grafana/loki        :3100  Log ingestion (internal)            │
│  grafana/promtail    —      Ships router audit logs → Loki      │
└─────────────────────────────────────────────────────────────────┘
```

**Prometheus scrapes only the router's `:2112` endpoint.** All agent metrics (hardware, engine, SRTT, counters) are aggregated by the router and exposed there — Prometheus does not need to reach individual agent servers.

### `docker-compose.agent.yml` — Agent

```
┌─────────────────────────────────────────────────────────────────┐
│  GPU Server                                                     │
│                                                                 │
│  hivenet-agent    outbound libp2p → router (:8903)            │
│                                                                 │
│  vLLM / Ollama / …   :8888  inference backend (host, not Docker)│
└─────────────────────────────────────────────────────────────────┘
```

The agent connects outbound to the router (`ROUTER_GRPC`, `ROUTER_P2P`), and the router forwards inference requests back over that same libp2p connection. The agent needs no inbound ports, no public IP, and no firewall rules — it works behind NAT and Docker with zero network configuration. See [the connection flow in the quickstart](01-Docker-Quickstart.md#connection-flow) for details.

---

## Persistent Volumes

| Volume | Used by | Contains |
|--------|---------|---------|
| `badger_data` | Router | BadgerDB — SRTT history, agent counters (survives restarts) |
| `audit_logs` | Router + Promtail | Audit JSONL — shared write/read via Docker volume |
| `prometheus_data` | Prometheus | Metric TSDB |
| `grafana_data` | Grafana | Dashboards, users, sessions |
| `loki_data` | Loki | Log chunks and index |
| `tempo_data` | Tempo | Trace WAL and blocks |
| `agent_data` | Agent | libp2p identity key (stable peer ID across restarts) |

---

## Single-Machine Quickstart

For local development or testing — router, agent, and inference backend on one machine:

```bash
git clone https://github.com/HivenetOSS/hivenet_router.git
cd hivenet_router

# Start the full stack (router + observability)
HIVENET_ROUTER_JWT_SECRET="$(openssl rand -hex 32)" docker compose up -d

# Verify all services are running
docker compose ps
```

The Compose file includes a local agent service already wired to the router via the internal Docker network. Agents never need address configuration — they only dial out to the router.

Access Grafana at **http://localhost:3000** (default credentials: `admin / changeme` — change before exposing publicly via `GF_SECURITY_ADMIN_PASSWORD`).

---

## Multi-Machine Deployment

### Step 1: Router Server

**On router-server:**

```bash
git clone https://github.com/HivenetOSS/hivenet_router.git
cd hivenet_router

# Set required secrets — do not use the defaults in production
HIVENET_ROUTER_JWT_SECRET="$(openssl rand -hex 32)" \
  docker compose up -d
```

Verify the router is healthy:

```bash
curl http://localhost:8888/admin/health
```

> **Firewall on router-server:** open TCP 8888 (clients), 8902 and 8903 (agents). Keep 9090 (Prometheus), 3100 (Loki), 4317 (Tempo), and 2112 (metrics) off the public internet — Grafana (:3000) should be behind a reverse proxy or SSH tunnel in production.

---

### Step 2: Each GPU Server

Copy the JWT secret from the router, then start the agent using `docker-compose.agent.yml`:

```bash
# Copy JWT secret from router server
scp router-server:/path/to/hivenet_router/docker-compose.agent.yml .
# Or clone the repo on the agent server too — the file is in the root

# Start the agent
ROUTER_GRPC=<ROUTER_IP>:8902 \
ROUTER_P2P=/ip4/<ROUTER_IP>/tcp/8903 \
HIVENET_ROUTER_JWT_SECRET=<same-secret-as-router> \
AGENT_REGION=EU-Primary \
  docker compose -f docker-compose.agent.yml up -d
```

The agent Compose file already contains:
- `--gpus all` GPU resource reservation
- `host.docker.internal` → host gateway so the container reaches the vLLM backend on the host

Available environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `ROUTER_GRPC` | Yes | Router gRPC address, e.g. `1.2.3.4:8902` |
| `ROUTER_P2P` | Yes | Router libp2p multiaddr, e.g. `/ip4/1.2.3.4/tcp/8903` |
| `HIVENET_ROUTER_JWT_SECRET` | Yes | Same secret as the router |
| `AGENT_REGION` | No | Region label, default `EU-France` |
| `AGENT_CAPACITY` | No | Max concurrent requests, default `10` |
| `MACHINE` | No | Machine identifier for metrics labels |

Check the agent registered:

```bash
docker compose -f docker-compose.agent.yml logs -f agent
# Look for: "registered with router" or "session established"

# On router-server — confirm both agents appear
curl http://localhost:8888/admin/health | jq .
```

---

## Configuration

### Routing Policy

Mount a policy file into the router container. Uncomment the relevant block in `docker-compose.yml`:

```yaml
services:
  router:
    volumes:
      - ./policies:/app/policies:ro
    command:
      # Option A — single global policy:
      - "--policy-file"
      - "/app/policies/_default.yaml"
      # Option B — per-model policy directory (recommended):
      - "--policy-model-dir"
      - "/app/policies"
```

See [Policy YAML Reference](../Routing%20%26%20Policies/02-Policy-YAML-Reference.md) for the full schema.

### API Key Auth

Auth is disabled by default. Two modes are available:

**Static mode** — keys from `auth.yaml`:

1. Copy `deploy/auth/auth.api-key.yaml` to `deploy/auth/auth.yaml` and fill in your key hashes
2. Uncomment in `docker-compose.yml`:
   ```yaml
   volumes:
     - ./deploy/auth/auth.yaml:/app/auth.yaml:ro
   command:
     - "--auth-config-file"
     - "/app/auth.yaml"
   ```

**Dynamic mode** — keys pushed at runtime by the machines service:

```yaml
environment:
  - HIVENET_ROUTER_AUTH_MODE=dynamic
  - HIVENET_ROUTER_ADMIN_API_KEYS=your-admin-key-here   # required in dynamic mode
```

No `auth.yaml` needed. Keys are managed via `POST /admin/api-keys/replace`. `HIVENET_ROUTER_ADMIN_API_KEYS` is mandatory: the router auto-elevates admin auth to `api-key` so the key-management endpoints aren't public, and startup fails if it's missing. See [API Keys — Dynamic Key Mode](../Security%20%26%20Auth/02-API-Keys.md#dynamic-key-mode).

### Debug Logging

```yaml
services:
  router:
    environment:
      - GOLOG_LOG_LEVEL=debug          # all subsystems
      # or per-subsystem: router=debug,policy=debug,metrics=debug
  agent:
    environment:
      - GOLOG_LOG_LEVEL=debug
```

---

## Accessing Services

| Service | URL | Notes |
|---------|-----|-------|
| Router HTTP API | `http://localhost:8888` | OpenAI-compatible |
| Router admin | `http://localhost:8888/admin/health` | Full agent status |
| Grafana | `http://localhost:3000` | `admin / changeme` |
| Prometheus | `http://localhost:9090` | Unexposed by default — use SSH tunnel |

To expose Prometheus externally (e.g. for KEDA or third-party tools), uncomment the `ports` block in `docker-compose.yml` and firewall it appropriately.

### Useful PromQL

```promql
# Active agents
count(hivenet_router_routing_agent_info)

# Request rate
rate(hivenet_router_routing_requests_routed_total[5m])

# Per-agent success rate
rate(hivenet_router_routing_requests_routed_total[5m])
  / (rate(hivenet_router_routing_requests_routed_total[5m]) + rate(hivenet_router_routing_requests_failed_total[5m]))

# P90 SRTT across fleet
histogram_quantile(0.9, sum by (le) (rate(hivenet_router_agent_srtt_ms_bucket[5m])))
```

### Grafana Dashboards

Pre-provisioned dashboards:
- **Hivenet Router** — agent health, request counters, SRTT, hardware metrics, policy routing
- **Hivenet Router Audit** — audit log viewer with tenant/model/status filters (Loki-backed)
- Traces visible in Grafana's **Explore → Tempo** datasource

---

## Backup and Restore

### Backup

```bash
# Stop the stack first to ensure consistent snapshots
docker compose down

docker run --rm \
  -v hivenet_router_badger_data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar czf /backup/badger-$(date +%Y%m%d).tar.gz /data

docker run --rm \
  -v hivenet_router_prometheus_data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar czf /backup/prometheus-$(date +%Y%m%d).tar.gz /data

docker run --rm \
  -v hivenet_router_grafana_data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar czf /backup/grafana-$(date +%Y%m%d).tar.gz /data
```

### Restore

```bash
docker compose down

docker run --rm \
  -v hivenet_router_badger_data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar xzf /backup/badger-<date>.tar.gz -C /

docker compose up -d
```

---

## Troubleshooting

### Agent not appearing in router

```bash
# Check agent logs
docker compose -f docker-compose.agent.yml logs agent

# Verify connectivity from GPU server to router
nc -zv <ROUTER_IP> 8902   # gRPC
nc -zv <ROUTER_IP> 8903   # libp2p

# Check the router received the registration
docker compose logs router | grep -i "registered\|agent"
```

### Prometheus not scraping

```bash
# Check Prometheus targets (internal port, use exec)
docker compose exec prometheus wget -qO- http://localhost:9090/api/v1/targets \
  | jq '.data.activeTargets[] | {job: .labels.job, health: .health, lastError: .lastError}'

# Verify the router's metrics endpoint is reachable from Prometheus container
docker compose exec prometheus wget -qO- http://router:2112/metrics | head -5
```

### Grafana shows no data

```bash
# Confirm Prometheus has data
docker compose exec prometheus wget -qO- \
  'http://localhost:9090/api/v1/query?query=hivenet_router_routing_agent_info' | jq .

# Check Loki for log ingestion errors
docker compose logs loki | tail -20
docker compose logs promtail | tail -20
```

### Agent metrics missing (multi-machine)

In multi-machine deployment the agents push all metrics to the **router** — there is no per-agent Prometheus port to scrape. If agent metrics are missing in Grafana, the agent is either not registered or not sending heartbeats:

```bash
# Check routing table for live agent metrics
curl http://localhost:8888/admin/routing-table | jq '.agents[] | {peer_id, srtt_ms, gpu_temp}'
```

---

## Next Steps

- [Bare-Metal Deployment](03-Bare-Metal.md) — Running binaries directly without Docker
- [Agent Deployment](04-Agent-Deployment/) — vLLM, Ollama, llama.cpp, SGLang configurations
- [Prometheus Metrics](../Observability/01-Prometheus-Metrics.md) — Full metric reference
- [Grafana Dashboards](../Observability/02-Grafana-Dashboards.md) — Dashboard setup and customisation
