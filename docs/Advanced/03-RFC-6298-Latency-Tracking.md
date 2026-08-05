# RFC 6298 Latency Tracking

Hivenet Router measures per-agent latency using an asymmetric EWMA adapted from RFC 6298, the TCP retransmission timeout algorithm. The result is a Smoothed RTT (SRTT) that drives both routing policy gates and future latency-based selection strategies.

## What RTT Measures Here

The RTT value fed into SRTT is the elapsed time from just before the router sends an HTTP request over libp2p to an agent, until the first bytes of the HTTP response headers arrive back. For non-streaming responses (the agent waits until inference is complete before replying), this is approximately:

```
RTT ≈ transport latency (router → agent) + full inference time + transport latency (agent → router)
```

For streaming responses, the agent flushes headers as soon as the first token is ready, so RTT is:

```
RTT ≈ transport latency + time-to-first-token on the agent
```

In both cases RTT reflects **how responsive the agent is from the router's point of view** — a combination of network distance and actual compute performance.

## Why Not End-to-End Client Latency?

End-to-end (E2E) latency — measured from client request arrival at the router to the last byte delivered back to the client — was deliberately not used for several reasons:

**1. Polluted by factors outside the agent's control.** E2E includes client-to-router network time, time spent in the router's request queue waiting for a slot, and time to stream tokens back to the client. A slow client connection or a congested queue would make a healthy agent look slow, causing the router to route *away* from an agent that has nothing wrong with it.

**2. Timing is wrong for the decision.** The routing decision for request N+1 is made while request N is still in-flight. E2E is unavailable until after the full response is delivered — too late to be used for the next dispatch. Router-to-agent RTT is available as soon as headers arrive, which for streaming is after the first token: early, frequent, and from the right perspective.

**3. Streaming completions have no definitive end time.** Token delivery is paced by the client reading the SSE stream. A client that reads slowly produces a long E2E time with no correlation to agent performance.

**4. Cross-agent comparison is meaningless.** Clients come from different geographies. Agent A serving a nearby client looks faster than Agent B serving a distant client even if B has lower inherent latency. Router-to-agent RTT measures all agents from the same vantage point (the router), making the values directly comparable for routing decisions.

## Algorithm

### SRTT Calculation

```
SRTT = (1 - α) × SRTT + α × R'
```

Where R' is the new RTT sample and α adapts based on whether latency is improving or worsening:

```
if R' < SRTT:          # New sample is better (latency improved)
    α = 0.5            # Fast downward convergence
else:                  # New sample is worse (latency spike)
    α = 0.125          # RFC 6298 standard — spike resistance
```

### RTTVAR Calculation

RTTVAR tracks the mean deviation (spread) of RTT samples. It is updated **before** SRTT on every measurement, as required by RFC 6298 §2.3:

```
RTTVAR = (1 - β) × RTTVAR + β × |SRTT - R'|
```

Where β = 0.25 (constant, RFC 6298 §2.3).

### First Measurement

On the very first sample for an agent, RFC 6298 §2.2 initialisation is used:

```
SRTT   = R'
RTTVAR = R' / 2
```

### Why Asymmetric α?

**Fast downward convergence (α=0.5 when R' < SRTT):** Without this, a single cold-start request — e.g. Ollama loading a model, which can take 10+ seconds — would keep SRTT inflated for ~40 subsequent requests and bias routing decisions against the agent for minutes. The high α lets SRTT quickly "forget" the outlier once normal latency resumes.

**Spike resistance (α=0.125 when R' > SRTT):** Momentary slowdowns (GC pause, a competing request, a brief CPU steal on the host) should not immediately deflect traffic away from an otherwise healthy agent. The low RFC 6298 standard α absorbs the spike over several subsequent measurements.

The combination produces a tracker that is fast to improve and slow to degrade — the right bias for a router that should stay with a good agent until degradation is sustained.

## Implementation

```go
func (u *UniversalCounterStore) updateRTT(s *agentCounterState, rttMs float64) {
    if !s.srttInited {
        // RFC 6298 §2.2: first measurement initialisation.
        s.srtt = rttMs
        s.rttvar = rttMs / 2
        s.srttInited = true
        return
    }
    // RTTVAR must be updated before SRTT (RFC 6298 §2.3 ordering requirement).
    s.rttvar = 0.75*s.rttvar + 0.25*math.Abs(s.srtt-rttMs)

    // Asymmetric α: converge fast when improving, filter spikes when worsening.
    if rttMs < s.srtt {
        s.srtt = 0.5*s.srtt + 0.5*rttMs    // α=0.5 — fast downward adaptation
    } else {
        s.srtt = 0.875*s.srtt + 0.125*rttMs // α=0.125 — RFC 6298, spike-resistant
    }
}
```

## Persistence

SRTT and RTTVAR are flushed to BadgerDB `diskDB` every 30 seconds and on agent disconnect:

```
diskDB: universalHistory:{peerID} → {srtt, rttvar, srttInited}
```

This enables **warm-start after router restart**: instead of beginning every reconnecting agent at SRTT=0 and converging from scratch, the router loads the last known value from disk.

**Without persistence:**

```
Router restarts.
Request 1: 190ms RTT → SRTT = 190ms  (correct, but only because it's the first sample)
```

Works by coincidence the first time. After Ollama model-load:

```
Request 1: model loading → 12s RTT  → SRTT = 12s
Request 2: normal        → 200ms    → SRTT = 6.1s   (still wrong)
...
Request 8: normal        → 195ms    → SRTT ≈ 200ms  (converges slowly)
```

**With persistence:**

```
Router restarts → loads SRTT=195ms from disk.
Request 1: 190ms RTT → SRTT = 192ms  (accurate immediately)
```

## Spike Resistance Example

```
Normal: 200ms RTT → SRTT = 200ms
Spike:  5000ms RTT (GC pause) → SRTT = 0.875×200 + 0.125×5000 = 800ms
Next:   195ms RTT → SRTT = 0.875×800 + 0.125×195 = 724ms
Next:   200ms RTT → SRTT = 0.875×724 + 0.125×200 = 658ms
...   (converges back to ~200ms over ~8 requests)
```

Without asymmetric α (standard α=0.125 only):

```
Spike: 5000ms → SRTT = 0.875×200 + 0.125×5000 = 800ms
Recovery is identical — spike resistance is symmetric in standard RFC 6298.
```

Without any smoothing (raw RTT):

```
Spike: 5000ms → SRTT = 5000ms immediately. Next request is routed elsewhere.
```

## Usage in Routing

### Exclude Slow Agents

```yaml
routing_policy:
  match:
    engine: "vllm"
  exclude_if:
    srtt_ms: { gt: 500 }   # Exclude agents with SRTT > 500ms
  strategy: "least-loaded"
```

### Lowest SRTT Strategy (Planned)

```yaml
routing_policy:
  strategy: "lowest-srtt"  # Select agent with lowest SRTT
```

## Monitoring

```promql
# SRTT per agent (milliseconds)
hivenet_router_agent_srtt_ms{peer_id="12D3Koo0..."}

# Average SRTT by region
avg by (region) (hivenet_router_agent_srtt_ms)

# Agents with high SRTT (>500ms)
hivenet_router_agent_srtt_ms > 500

# RTTVAR per agent — high values indicate unstable/bursty latency
hivenet_router_agent_rttvar_ms{peer_id="12D3Koo0..."}

# Agents with high variance (>300ms deviation)
hivenet_router_agent_rttvar_ms > 300
```

## Debugging

### Inspect Current SRTT Values

The routing table snapshot includes live SRTT/RTTVAR for every connected agent:

```bash
curl -s http://localhost:8080/admin/routing-table | jq '.agents[] | {peer_id, srtt_ms, rttvar_ms}'
```

### Enable Debug Logging

Log level is controlled via the `GOLOG_LOG_LEVEL` environment variable (uses [go-log](https://github.com/ipfs/go-log)):

```bash
# All subsystems at debug
GOLOG_LOG_LEVEL=debug ./bin/hivenet-router

# Only the router and metrics subsystems
GOLOG_LOG_LEVEL=router=debug,metrics=debug ./bin/hivenet-router
```

Then watch the RTT update lines:

```bash
journalctl -fu hivenet-router | grep -i srtt
```

## See Also

- [Detailed Architecture](../Reference/00-Detailed-Architecture.md) - Storage architecture
- [Performance Characteristics](../Reference/03-Performance-Characteristics.md) - Timing targets
- [RFC 6298](https://datatracker.ietf.org/doc/html/rfc6298) - Original specification
