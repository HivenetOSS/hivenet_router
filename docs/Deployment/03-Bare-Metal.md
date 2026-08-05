# Bare-Metal Deployment

Run Hivenet Router as native binaries managed by systemd. This is the recommended approach for production servers where you want full control over resource limits, log rotation, and process supervision without a container runtime.

> **Connection model and port layout** are the same as for Docker. Read [Docker Quickstart — Connection flow](01-Docker-Quickstart.md#connection-flow) and the [Port Reference](01-Docker-Quickstart.md#port-reference) if you haven't already — this document does not repeat them.

---

## Prerequisites

| Requirement | Router server | Agent / GPU servers |
|-------------|:---:|:---:|
| Linux (Ubuntu 22.04+ / Debian 12+ / RHEL 9+) | ✓ | ✓ |
| Go 1.23+ (build only — not needed at runtime) | ✓ | ✓ |
| NVIDIA driver + `nvidia-smi` working | — | ✓ |
| Port 8080 / 50051 / 9000 open to agents | ✓ | — |

---

## Step 1: Build Binaries

Build on any machine with Go 1.23+ and copy the resulting binaries to the target servers. Binaries have no runtime dependencies — no Go installation needed on production hosts.

```bash
git clone https://github.com/HivenetOSS/hivenet_router.git
cd hivenet_router

go build -o bin/hivenet-router ./cmd/router/
go build -o bin/hivenet-agent  ./cmd/agent/

# Smoke-test
./bin/hivenet-router --help
./bin/hivenet-agent  --help
```

Distribute to servers:

```bash
# Router binary
scp bin/hivenet-router router-server:/opt/hivenet-router/

# Agent binary (all GPU servers)
scp bin/hivenet-agent gpu-eu-1:/opt/hivenet-router/
scp bin/hivenet-agent gpu-eu-2:/opt/hivenet-router/
```

---

## Step 2: JWT Secret

**On router-server:**

```bash
sudo mkdir -p /opt/hivenet-router
sudo install -m 600 -o root -g root /dev/null /opt/hivenet-router/jwt.secret
openssl rand -hex 32 | sudo tee /opt/hivenet-router/jwt.secret > /dev/null
```

Copy to every GPU server:

```bash
scp /opt/hivenet-router/jwt.secret gpu-eu-1:/opt/hivenet-router/jwt.secret
scp /opt/hivenet-router/jwt.secret gpu-eu-2:/opt/hivenet-router/jwt.secret
```

---

## Step 3: Start the Inference Backend

Start the inference engine (vLLM, Ollama, llama.cpp, …) on each GPU server **before** starting the agent. The agent reads the available models from the backend's `/v1/models` endpoint at startup and won't register if the backend is unreachable.

```bash
# vLLM example
vllm serve meta-llama/Llama-3.1-8B-Instruct \
  --host 0.0.0.0 \
  --port 8888 \
  --tensor-parallel-size 1 \
  --max-model-len 8192

# Wait until ready
curl http://localhost:8888/health   # returns 200 when the model is loaded
```

See [Agent Deployment guides](04-Agent-Deployment/) for vLLM, Ollama, llama.cpp, and SGLang-specific flags.

---

## Step 4: Start the Router

On **router-server**, create the data directory and run the router. `--disk-db-path` points to where BadgerDB stores SRTT history and agent counters across restarts — always set this to a persistent path, not the default `./badger_disk`.

```bash
sudo mkdir -p /var/lib/hivenet-router/badger
sudo chown $USER /var/lib/hivenet-router/badger

/opt/hivenet-router/hivenet-router \
  --jwt-secret-file /opt/hivenet-router/jwt.secret \
  --http-port     :8080 \
  --grpc-port     :50051 \
  --p2p-port      9000 \
  --metrics-port  :2112 \
  --disk-db-path  /var/lib/hivenet-router/badger
```

Verify:

```bash
curl http://localhost:8080/admin/health
# Expected: {"status":"ok","agents":0}
```

---

## Step 5: Start an Agent

Two flags are required on any multi-machine deployment:

- `--router-p2p /ip4/<ROUTER_IP>/tcp/9000` — the router's libp2p address (needed alongside `--router-grpc`)
- `--identity-path` — path where the agent's Ed25519 key is stored; without a stable path the agent gets a new peer ID on every restart and the router sees it as a different agent each time

**On gpu-eu-1 (192.168.1.101):**

```bash
/opt/hivenet-router/hivenet-agent \
  --router-grpc        192.168.1.100:50051 \
  --router-p2p         /ip4/192.168.1.100/tcp/9000 \
  --jwt-secret-file    /opt/hivenet-router/jwt.secret \
  --engine             vllm \
  --backend-url        http://localhost:8888 \
  --capacity           32 \
  --region             EU-Primary \
  --identity-path      /opt/hivenet-router/agent_identity.key
```

**On gpu-eu-2 (192.168.1.102):**

```bash
/opt/hivenet-router/hivenet-agent \
  --router-grpc        192.168.1.100:50051 \
  --router-p2p         /ip4/192.168.1.100/tcp/9000 \
  --jwt-secret-file    /opt/hivenet-router/jwt.secret \
  --engine             vllm \
  --backend-url        http://localhost:8888 \
  --capacity           32 \
  --region             EU-Secondary \
  --identity-path      /opt/hivenet-router/agent_identity.key
```

Verify both agents registered — see [Docker Quickstart — Step 5](01-Docker-Quickstart.md#step-5-verify-registration) for the expected output and the test-inference command.

---

## Running as systemd Services

For production, run both binaries under systemd so they restart on failure, log to journald, and start automatically on boot.

### Router service

**`/etc/systemd/system/hivenet-router.service`:**

```ini
[Unit]
Description=Hivenet Router
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hivenet-router
Group=hivenet-router
WorkingDirectory=/opt/hivenet-router

ExecStart=/opt/hivenet-router/hivenet-router \
  --jwt-secret-file   /opt/hivenet-router/jwt.secret \
  --http-port         :8080 \
  --grpc-port         :50051 \
  --p2p-port          9000 \
  --metrics-port      :2112 \
  --disk-db-path      /var/lib/hivenet-router/badger

Restart=on-failure
RestartSec=5s

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=hivenet-router

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/hivenet-router/badger /var/log/hivenet-router

[Install]
WantedBy=multi-user.target
```

### Agent service

**`/etc/systemd/system/hivenet-agent.service`** (one per GPU server — fill in the `ExecStart` values for each host):

```ini
[Unit]
Description=Hivenet Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hivenet-router
Group=hivenet-router
WorkingDirectory=/opt/hivenet-router

# ── Edit these two lines per server ──────────────────────────────────────────
Environment=ROUTER_IP=192.168.1.100
Environment=AGENT_REGION=EU-Primary
# ─────────────────────────────────────────────────────────────────────────────

ExecStart=/opt/hivenet-router/hivenet-agent \
  --router-grpc        ${ROUTER_IP}:50051 \
  --router-p2p         /ip4/${ROUTER_IP}/tcp/9000 \
  --jwt-secret-file    /opt/hivenet-router/jwt.secret \
  --engine             vllm \
  --backend-url        http://localhost:8888 \
  --capacity           32 \
  --region             ${AGENT_REGION} \
  --identity-path      /opt/hivenet-router/agent_identity.key

Restart=on-failure
RestartSec=10s

StandardOutput=journal
StandardError=journal
SyslogIdentifier=hivenet-agent

NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/opt/hivenet-router

[Install]
WantedBy=multi-user.target
```

### Enable and start

Run these commands on each server after writing the unit file:

```bash
# Create the service user (no login shell, no home directory)
sudo useradd --system --no-create-home --shell /usr/sbin/nologin hivenet-router

# Fix ownership
sudo chown -R hivenet-router:hivenet-router /opt/hivenet-router /var/lib/hivenet-router

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now hivenet-router   # on router-server
sudo systemctl enable --now hivenet-agent    # on GPU servers

# Check status
sudo systemctl status hivenet-router
sudo journalctl -fu hivenet-router
```

---

## Scaling — Add a GPU Server

```bash
# 1. Copy binary and JWT secret
scp bin/hivenet-agent  gpu-us-1:/opt/hivenet-router/
scp /opt/hivenet-router/jwt.secret gpu-us-1:/opt/hivenet-router/

# 2. Copy and edit the unit file
scp /etc/systemd/system/hivenet-agent.service gpu-us-1:/etc/systemd/system/
# Edit AGENT_REGION on gpu-us-1

# 3. Start
sudo systemctl daemon-reload && sudo systemctl enable --now hivenet-agent
```

No router restart needed — the new agent registers automatically on connect.

---

## Upgrade Procedure

```bash
# 1. Build new binaries
go build -o bin/hivenet-router ./cmd/router/
go build -o bin/hivenet-agent  ./cmd/agent/

# 2. Stop agents first (on all GPU servers) so in-flight requests drain
sudo systemctl stop hivenet-agent

# 3. Stop router
sudo systemctl stop hivenet-router

# 4. Back up current binaries
cp /opt/hivenet-router/hivenet-router /opt/hivenet-router/hivenet-router.bak
cp /opt/hivenet-router/hivenet-agent  /opt/hivenet-router/hivenet-agent.bak

# 5. Deploy new binaries
scp bin/hivenet-router router-server:/opt/hivenet-router/
scp bin/hivenet-agent  gpu-eu-1:/opt/hivenet-router/
scp bin/hivenet-agent  gpu-eu-2:/opt/hivenet-router/

# 6. Start router first, then agents
sudo systemctl start hivenet-router
# verify router is healthy before starting agents
curl http://192.168.1.100:8080/admin/health
sudo systemctl start hivenet-agent   # on each GPU server

# 7. Rollback if needed
sudo systemctl stop hivenet-router hivenet-agent
mv /opt/hivenet-router/hivenet-router.bak /opt/hivenet-router/hivenet-router
mv /opt/hivenet-router/hivenet-agent.bak  /opt/hivenet-router/hivenet-agent
sudo systemctl start hivenet-router hivenet-agent
```

---

## NAT / Edge Device Deployment

Agents behind NAT (no public IP) need no special setup. The agent establishes an outbound libp2p connection to the router, and the router forwards inference requests back over that same connection — the router never dials the agent. As long as the edge device can reach the router's ports 50051 and 9000 outbound, it works; the SSH reverse-tunnel workaround previously documented here is no longer needed.

---

## Troubleshooting

**Ports already in use:**

```bash
sudo ss -tlnp | grep -E '8080|50051|9000'
```

**Agent not connecting to router:**

```bash
# From GPU server — verify router ports are reachable (agents connect outbound only)
nc -zv <ROUTER_IP> 50051
nc -zv <ROUTER_IP> 9000

# Router firewall
sudo ufw status | grep -E '50051|9000|8080'

# Agents accept no inbound connections — no agent-side firewall rule is needed
```

**Agent gets a new peer ID after restart:**

Ensure `--identity-path` points to a persistent file that survives reboots. The first startup creates the file; subsequent startups load it.

```bash
ls -la /opt/hivenet-router/agent_identity.key
```

**GPU metrics not appearing:**

The agent process must run as a user in the `video` group (or root) to access NVML:

```bash
sudo usermod -aG video hivenet-router
sudo systemctl restart hivenet-agent
```

**Check live metrics and SRTT:**

```bash
curl http://192.168.1.100:8080/admin/routing-table | jq '.agents[] | {peer_id, region, srtt_ms}'
curl http://192.168.1.100:2112/metrics | grep hivenet_router_routing_requests_routed_total
```

---

## Next Steps

- [Docker Compose](02-Docker-Compose.md) — Add Prometheus + Grafana observability alongside the router
- [Agent Deployment](04-Agent-Deployment/) — Engine-specific flags for vLLM, Ollama, llama.cpp, SGLang
- [Configuration Reference](../Reference/01-Configuration-Reference.md) — All flags and environment variables
