# Why Hivenet Router

Hivenet Router is a high-performance Language Model as a Service (LaaS) platform designed for sovereign, distributed inference. Built by **Hivenet Router**, a company specializing in decentralized GPU compute and sovereign cloud infrastructure.

## About Hivenet Router

**Hivenet Router** operates a distributed network of GPU and storage infrastructure across France, UAE, and USA. With €12M Series A funding and research partnerships with INRIA (French research institute), Hivenet Router focuses on:

- **Data sovereignty by architecture** - Not policy, but design. Your data stays where you specify.
- **Sustainability** - 77% greener than centralized cloud through efficient distributed utilization.
- **No hyperscaler dependency** - Own hardware, own infrastructure, no routing through AWS/GCP/Azure.
- **99.7% SLA-backed availability** - Enterprise-grade reliability on decentralized infrastructure.

**Hivenet Router** extends this philosophy to LLM inference: sovereign, distributed, transparent.

## Sovereignty and Control

**Own your inference stack.** Unlike managed services, Hivenet Router gives you complete control over:

- **Data privacy** - Requests never leave your infrastructure
- **Model selection** - Run any model on any backend
- **Cost predictability** - No per-token fees, no surprise charges
- **Auditability** - Full visibility into routing decisions and request logs

## NAT Traversal for Decentralized GPU

**Connect GPUs across networks without complex networking.** Built on libp2p, Hivenet Router:

- Traverses NAT automatically (no port forwarding required)
- Encrypts all agent traffic with Noise protocol
- Persists connections (no connection pooling overhead)
- Supports decentralized deployments across sites and clouds

## Multi-Engine Support

**One API for all backends.** Hivenet Router normalizes different inference engines:

| Engine | Use Case | Metrics Support |
|--------|----------|-----------------|
| vLLM | High-throughput LLM | Full (KV cache, TTFT, ITL) |
| Ollama | Local/edge inference | Basic |
| SGLang | Structured generation | Full (same as vLLM) |
| Infinity | Embedding + reranking | Basic |
| Custom | Any OpenAI-compatible | Configurable |

Route to the right engine for each workload without changing client code.

## Hardware-Aware Routing

**Route based on real GPU state, not just availability.** Hivenet Router collects and acts on:

- GPU temperature (avoid thermal throttling)
- VRAM utilization (prevent OOM)
- GPU compute utilization (balance load)
- CPU and memory pressure (system health)

Example policy excluding overheated GPUs:
```yaml
exclude_if:
  gpu_temperature_c: { gt: 82 }
  gpu_vram_used_percent: { gt: 0.9 }
```

## Open Source Transparency

**No black boxes.** Everything is visible and auditable:

- Policy evaluation logic in plain YAML
- Request routing decisions logged
- Metrics exposed via Prometheus
- Full source code available (Apache 2.0)

## Comparison with Alternatives

| Feature | Hivenet Router | LiteLLM | KubeAI | Portkey |
|---------|------------|---------|--------|---------|
| Self-hosted | ✅ | ✅ | ✅ | ❌ (SaaS) |
| NAT traversal | ✅ (libp2p) | ❌ | ❌ | ❌ |
| Hardware-aware routing | ✅ | ❌ | ❌ | ❌ |
| Multi-engine | ✅ | ✅ | ✅ | ❌ |
| Kubernetes-agnostic | ✅ | ✅ | ❌ (K8s-only) | ❌ |
| Closed-source fallback | ✅ | ✅ | ❌ | ✅ |
| Embedded + reranking | ✅ | ✅ | ❌ | ❌ |

## Performance

**Built for speed:**

- **Sub-millisecond routing decisions** - Radix tree policy matching
- **~7ns JWT validation** - lestrrat-go/jwx with JWKS auto-refresh
- **~2.3µs BadgerDB reads** - LSM-tree in-memory lookups
- **~7ns Prometheus counters** - Native client_golang

## Ideal For

- **Multi-site GPU clusters** - Route across datacenters with NAT traversal
- **Hybrid inference** - Mix on-prem GPUs with cloud fallback
- **Edge deployments** - Connect remote GPUs without complex networking
- **Cost optimization** - Route cheaper models first, fallback to premium
- **Compliance requirements** - Keep data on-prem with full auditability

## Not Ideal For

- **Single-backend setups** - Use vLLM/Ollama directly
- **Serverless-only environments** - Requires persistent processes
- **Managed service preference** - Consider Portkey or Azure AI

## See Also

- [Architecture Overview](01-Architecture-Overview.md) - How it works
- [Quickstart](00-Quickstart.md) - Get started in 5 minutes
- [From LiteLLM](../Migration%20Guides/01-From-LiteLLM.md) - Migration guide