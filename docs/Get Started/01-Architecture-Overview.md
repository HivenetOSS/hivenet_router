# Architecture Overview

Hivenet Router follows a hub-and-spoke architecture where a central Router manages multiple Agents, each proxying requests to local inference servers.

## Architecture Overview

![Hivenet Router Architecture](../images/Architecture.png)

## Key Components

### Router (Central Hub)

The Router is the brain of Hivenet Router, exposing multiple interfaces:

| Interface | Port | Purpose |
|-----------|------|---------|
| HTTP API | `:8080` | OpenAI-compatible inference endpoints |
| gRPC Auth | `:50051` | Agent authentication (JWT + TLS) |
| libp2p P2P | `:9000` | Encrypted data plane to agents |
| Prometheus | `:2112` | Metrics export |

**Responsibilities:**
- Routes requests via 3-layer policy pipeline
- Maintains agent health state (5s check interval)
- Stores agent metadata and metrics in BadgerDB
- Enforces authentication and quotas

### Agent (Lightweight Proxy)

Agents connect backend inference servers to the Router:

**Responsibilities:**
- Authenticates to Router via gRPC + JWT
- Registers via libp2p HTTP
- Forwards inference requests to backend
- Collects hardware metrics (NVML GPU, CPU, memory)
- Scrapes engine metrics
- Sends heartbeats (5s) and routing signals (500ms)

**Supported Backends:**
- vLLM (primary, full metrics support)
- Ollama (strips `:latest` suffix from model names)
- SGLang (same metrics API as vLLM)
- llama.cpp (via `--metrics` flag; predicted/prompt TPS metrics)
- Infinity (embedding + reranking)
- Custom (any OpenAI-compatible server; requires `--model` and `--health-url`)

## Request Flow

1. **Client sends request** to `POST /v1/chat/completions`
2. **Router authenticates** client (API key or none)
3. **Router evaluates policy**:
   - Layer 1: Static filter (region, engine, tags, model)
   - Layer 2: Dynamic gates (KV cache < 85%, success rate > 95%)
   - Layer 3: Strategy ranking (least-loaded by default)
4. **Router forwards** request via libp2p to selected agent
5. **Agent proxies** to backend inference server
6. **Response flows back** through same path
7. **Router returns** to client

## Storage

Hivenet Router uses dual BadgerDB instances:

| Database | Location | Purpose | Lifetime |
|----------|----------|---------|----------|
| memDB | In-memory | Ephemeral state (metadata, punctual metrics) | Session |
| diskDB | `./badger_disk/` | Lifetime counters, SRTT/RTTVAR | 30 days |

## Protocols

| Plane | Protocol | Encryption |
|-------|----------|------------|
| Client → Router | HTTP/2 | TLS (optional) |
| Agent → Router (Auth) | gRPC | TLS 1.3 (HKDF-derived from JWT secret) |
| Router ↔ Agent (Data) | libp2p | Noise protocol |

## Authentication

| Type | Implementation | Use Case |
|------|----------------|----------|
| API Keys | Static SHA-256 hashed keys (`auth/static_key.go`) | Service-to-service, tenant quotas |
| JWT | HS256 via `lestrrat-go/jwx` v3 | Agent authentication (gRPC) |
| Quotas | `golang.org/x/time/rate` + BadgerDB/memory | Per-tenant RPM and daily token limits |

## When to Use Which Deployment

| Scenario | Recommended Mode |
|----------|------------------|
| Quick testing | [Docker Quickstart](../Deployment/01-Docker-Quickstart.md) |
| Production with observability | [Docker Compose](../Deployment/02-Docker-Compose.md) |
| Bare-metal edge devices | [Bare-metal Deployment](../Deployment/03-Bare-metal.md) |
| Kubernetes clusters | Helm chart (planned) |

## Technology Stack

### Core Technologies

| Layer | Technology | Purpose |
|-------|------------|---------|
| HTTP Framework | [Gin](github.com/gin-gonic/gin) v1.12.0 | OpenAI-compatible REST API with HTTP/2 + SSE support |
| Storage | [BadgerDB](github.com/dgraph-io/badger) v4 | LSM-tree KV store (memDB + diskDB) |
| P2P Networking | [libp2p](github.com/libp2p/go-libp2p) v0.47.0 | Encrypted Router ↔ Agent communication |
| RPC | [gRPC](google.golang.org/grpc) v1.79.2 | Agent authentication control plane |
| JWT | [lestrrat-go/jwx](github.com/lestrrat-go/jwx) v3 | HS256 token validation (~7 ns/request) |
| Metrics | [Prometheus](github.com/prometheus/client_golang) v1.23.2 | Exporter with private registry |
| YAML | [goccy/go-yaml](github.com/goccy/go-yaml) | Routing policy configuration |
| Rate Limiting | [golang.org/x/time/rate](golang.org/x/time/rate) | Token bucket algorithm for RPM enforcement |
| CLI | Go `flag` package | Standard library flag parsing (router + agent) |

### Routing Strategies

| Strategy | Status | Description |
|----------|--------|-------------|
| `least-loaded` | ✅ Implemented | Select agent with lowest `ActiveRequests/Capacity` ratio |
| `lowest-srtt` | 🚧 Planned | Select agent with lowest smoothed RTT (RFC 6298) |
| `round-robin` | 🚧 Planned | Cycle through candidates evenly |
| Prefix-aware (CHWBL) | 🚧 Planned | Consistent hashing with bounded loads for KV cache reuse |

### Quota Enforcement

| Backend | Storage | Persistence |
|---------|---------|-------------|
| `memory` (default) | In-process | Lost on restart |
| `badger` | BadgerDB diskDB | Survives restarts (48h TTL) |

Configured via `QuotaBackend` in `Config` struct or `HIVENET_ROUTER_QUOTA_BACKEND` env var.

## See Also

- [Detailed Architecture](../Reference/00-Detailed-Architecture.md) - Complete technical reference
- [Why Hivenet Router](03-Why-Hivenet-Router.md) - Value proposition
- [Routing Concepts](../Routing%20%26%20Policies/01-Routing-Concepts.md) - Policy pipeline explained