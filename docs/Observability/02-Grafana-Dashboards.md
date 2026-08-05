# Grafana Dashboards

Pre-configured dashboards for monitoring Hivenet Router router and agents.

## Access

Default URL: http://localhost:3000

Login: `admin` / `admin` (change in production)

## Hivenet Router Dashboard

Main operational dashboard for router monitoring.

### Overview Panel

- **Request Rate** - Requests per second over time
- **Active Agents** - Count of healthy registered agents
- **Error Rate** - Percentage of failed requests
- **P90 Latency** - 90th percentile request latency

### Agent Health

```
┌─────────────────────────────────────────┐
│ Agent Health by Region                  │
├─────────────────────────────────────────┤
│ EU-France:  3 healthy, 0 unhealthy      │
│ EU-Germany: 2 healthy, 1 unhealthy      │
│ US-East:    4 healthy, 0 unhealthy      │
└─────────────────────────────────────────┘
```

**Query:**

```promql
sum by (region) (hivenet_router_routing_agent_healthy == 1)
```

### Request Metrics

- **Requests by Model** - Traffic distribution across models
- **Requests by Region** - Geographic distribution
- **Fallback Usage** - Fallback step activation rate
- **Queue Depth** - Wait queue utilization per model

### Latency Distribution

- **Time to First Token (TTFT)** - Histogram and percentiles
- **Inter-Token Latency (ITL)** - Streaming performance
- **End-to-End Latency** - Complete request duration

### vLLM Metrics

- **KV Cache Utilization** - Memory usage per agent
- **Running Requests** - Active inference count
- **Waiting Requests** - Queue depth per agent
- **Preemptions** - Context switching events

### Hardware Metrics

- **GPU Temperature** - Thermal monitoring per agent
- **GPU Utilization** - Compute usage
- **VRAM Usage** - Memory consumption
- **Power Draw** - Energy consumption (W)

## Hivenet Router Tenant Quota Dashboard

Quota and billing dashboard (`hivenet-router-tenants`).

- **Requests by Tenant** — Request counts per tenant
- **Token Usage** — Input/output tokens by tenant and model
- **Rate Limit Events** — RPM and token budget rejections
- **Quota Gauges** — Configured RPM and daily token limits

Access at `http://localhost:3000/d/hivenet-router-tenants/`

## Hivenet Router Audit Dashboard

Request logging and compliance dashboard (requires Loki). See [Audit Logging](03-Audit-Logging.md) for log format, LogQL queries, and Loki/Promtail configuration.

Access at `http://localhost:3000/d/hivenet-router-audit/`

## Custom Dashboard Creation

### Import Dashboard

1. Navigate to Dashboard → Import
2. Upload JSON file from `deploy/grafana/provisioning/dashboards/`
3. Select Prometheus/Loki data sources
4. Click Import

### Dashboard JSON

```bash
# Router dashboard
deploy/grafana/provisioning/dashboards/hivenet-router.json

# Audit dashboard
deploy/grafana/provisioning/dashboards/audit.json

# Tenant quota dashboard
deploy/grafana/provisioning/dashboards/tenants.json
```

### Variable Definitions

Define template variables for dynamic filtering:

```
$region - Agent region (EU-France, US-East, etc.)
$engine - Backend engine (vllm, ollama, sglang)
$model - Model name
$peer_id - Agent peer ID
$tenant - Tenant identifier
```

## Alerting

Grafana can fire alerts directly from dashboard panels. Click a panel → **Edit → Alert** to define a condition and notification contact point.

For Prometheus-managed alert rules, see [Prometheus Metrics → Alerting Examples](01-Prometheus-Metrics.md#alerting-examples).

## Dashboard Provisioning

Auto-provision dashboards via configuration:

```yaml
# /etc/grafana/provisioning/dashboards/hivenet-router.yaml
apiVersion: 1

providers:
  - name: 'Hivenet Router'
    orgId: 1
    folder: ''
    folderUid: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /etc/grafana/dashboards
```

## See Also

- [Prometheus Metrics](01-Prometheus-Metrics.md) - Metric reference
- [Audit Logging](03-Audit-Logging.md) - Log configuration
- [Docker Compose](../Deployment/02-Docker-Compose.md) - Full observability stack
