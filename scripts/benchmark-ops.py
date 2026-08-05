#!/usr/bin/env python3
# benchmark-ops.py — Hivenet Router operational benchmark
#
# Tests router and agent operations: cluster health, concurrent load,
# routing distribution, and error handling.
#
# Usage:
#   python3 scripts/benchmark-ops.py -u https://router.example.com -k "$API_KEY" -a "$ADMIN_KEY"
#   ROUTER_URL=... LLM_API_KEY=... ADMIN_API_KEY=... python3 scripts/benchmark-ops.py
#
# Environment variables (override flags):
#   ROUTER_URL       — router base URL
#   LLM_API_KEY      — API key for /v1 endpoints
#   ADMIN_API_KEY    — API key for /admin endpoints
#   COMMIT_SHA       — git commit SHA (for metadata)
#   OPS_CONCURRENCY  — max concurrent requests for load test (default 10)
#   TEST_MODEL       — model ID to send requests against (default "openai/gpt-oss-20b")
#   TEMPO_URL        — Tempo query endpoint (optional; enables trace timing breakdown)
#   TEMPO_BASIC_AUTH — Basic auth credentials for Tempo (format: "user:password", optional)
#   TEMPO_REQUIRED   — if "1", treat Tempo unreachable / no traces fetched as a failed check
#   STRICT           — if "1", exit non-zero when any check (including SLOs) fails
#
# SLO thresholds (router-phase budgets — end-to-end latency is reported but
# not gated, because it's dominated by model inference which is out of scope
# for the router. Router-phase metrics come from Tempo spans, so TEMPO_URL
# must be set for these SLOs to evaluate; TEMPO_REQUIRED=1 makes that mandatory.)
#   SLO_SUCCESS_RATE_MIN        — minimum success rate per workload (default 0.95)
#   SLO_ADMIN_P95_MS            — p95 per-sample admin endpoint latency (default 1000).
#                                 Sample count per endpoint is controlled by
#                                 ADMIN_LATENCY_SAMPLES (default 10).
#   SLO_ROUTING_OVERHEAD_P95_MS — p95 of (total − backend inference), i.e. pure router
#                                 overhead (default 100)
#   SLO_QUEUE_WAIT_P95_MS       — p95 queue-wait before dispatch (default 50)
#   SLO_DISPATCH_POLICY_P95_MS  — p95 of policy eval time (dispatch span
#                                 minus nested forward) (default 20)

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ── Workloads ─────────────────────────────────────────────────────────────────
#
# We benchmark router operations, not model quality. The trivial workload
# isolates router overhead; the midsize workload exercises the router's
# handling of larger request/response payloads (serialization, libp2p
# forward, queue admission). Both use a tiny model so inference time is
# small and any latency change shows up as router-side overhead.

TEST_MODEL = os.environ.get("TEST_MODEL", "openai/gpt-oss-20b")

_MIDSIZE_BASE = (
    "Consider a distributed system where multiple services coordinate via an "
    "event bus. Each service publishes state changes, and downstream consumers "
    "react to those events to maintain local caches and derived state. The "
    "system must guarantee at-least-once delivery, idempotent consumption, and "
    "bounded recovery time under backpressure. Events carry a monotonically "
    "increasing sequence number, a timestamp, and a causality token used for "
    "out-of-order reordering. Load is uneven: two of the fifteen event types "
    "account for eighty percent of traffic, and their cardinality is on the "
    "order of millions of distinct partitioning keys. "
)


def _midsize_prompt(target_chars=2000):
    body = (_MIDSIZE_BASE * ((target_chars // len(_MIDSIZE_BASE)) + 1))[:target_chars]
    return body + (
        "\n\nGiven this context, compare the trade-offs between Kafka, NATS, "
        "and RabbitMQ as the event bus. Be concise."
    )


WORKLOADS = [
    {"name": "trivial", "prompt": "Say hello in one word.", "max_tokens": 10},
    {"name": "midsize", "prompt": _midsize_prompt(), "max_tokens": 200},
]


def _env_flag(name):
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _env_int(name, default):
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name, default):
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _default_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Hivenet Router operational benchmark",
    )
    parser.add_argument("-u", dest="router_url", default=os.environ.get("ROUTER_URL", ""),
                        help="Router base URL")
    parser.add_argument("-k", dest="api_key", default=os.environ.get("LLM_API_KEY", ""),
                        help="API key for /v1 endpoints")
    parser.add_argument("-a", dest="admin_key", default=os.environ.get("ADMIN_API_KEY", ""),
                        help="API key for /admin endpoints")
    parser.add_argument("-o", dest="json_out", default="",
                        help="Write JSON results to this file")
    parser.add_argument("-m", dest="md_out", default="",
                        help="Write Markdown summary to this file")
    parser.add_argument("-c", dest="commit_sha",
                        default=os.environ.get("COMMIT_SHA", _default_commit()),
                        help="Git commit SHA (for metadata)")
    parser.add_argument("-n", dest="concurrency", type=int,
                        default=int(os.environ.get("OPS_CONCURRENCY", "10")),
                        help="Max concurrent requests for load test (default 10)")
    parser.add_argument("-t", dest="timeout", type=int,
                        default=int(os.environ.get("TIMEOUT", "30")),
                        help="Per-request timeout in seconds (default 30)")
    return parser.parse_args()


# ── HTTP helper ───────────────────────────────────────────────────────────────

def fetch(url, headers=None, method="GET", data=None, req_timeout=None, default_timeout=30):
    """HTTP helper returning (status, body_dict, latency_ms, resp_headers)."""
    hdrs = headers or {}
    t = req_timeout or default_timeout
    body_bytes = json.dumps(data).encode() if data else None
    req = Request(url, data=body_bytes, headers=hdrs, method=method)
    if body_bytes:
        req.add_header("Content-Type", "application/json")
    start = time.time()
    try:
        with urlopen(req, timeout=t) as r:
            raw = r.read()
            resp_hdrs = {k.lower(): v for k, v in r.getheaders()}
            try:
                body = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                body = {"raw": raw.decode("utf-8", errors="replace")}
            return r.status, body, int((time.time() - start) * 1000), resp_hdrs
    except HTTPError as e:
        resp_hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        try:
            body = json.loads(e.read())
        except Exception:
            body = {"raw": str(e)}
        return e.code, body, int((time.time() - start) * 1000), resp_hdrs
    except Exception as e:
        return 0, {"error": str(e)}, int((time.time() - start) * 1000), {}


def extract_trace_id(resp_headers):
    """Extract trace ID from W3C traceparent header (00-<traceID>-<spanID>-<flags>)."""
    tp = resp_headers.get("traceparent", "")
    parts = tp.split("-")
    if len(parts) >= 2 and len(parts[1]) == 32:
        return parts[1]
    return None


def percentile(sorted_list, p):
    if not sorted_list:
        return 0
    idx = int(len(sorted_list) * p / 100)
    return sorted_list[min(idx, len(sorted_list) - 1)]


# ── Benchmark steps ───────────────────────────────────────────────────────────

def run_health_snapshot(router_url, v1_headers, admin_headers, step_label, timeout):
    print(f"  [{step_label}] Cluster health snapshot...", file=sys.stderr, flush=True)
    _f = lambda url, hdrs=None, method="GET", data=None, t=None: fetch(
        url, hdrs, method, data, t, timeout)

    health_results = {}

    status, body, lat, _ = _f(f"{router_url}/health")
    health_results["liveness"] = {"status": status, "latency_ms": lat, "ok": status == 200}

    status, body, lat, _ = _f(f"{router_url}/admin/health", admin_headers)
    health_results["admin_health"] = {
        "status": status, "latency_ms": lat, "ok": status == 200,
        "data": body if status == 200 else None,
    }
    admin_health = body if status == 200 else {}

    status, body, lat, _ = _f(f"{router_url}/admin/routing-table", admin_headers)
    health_results["routing_table"] = {
        "status": status, "latency_ms": lat, "ok": status == 200,
        "agent_count": body.get("total", 0) if status == 200 else 0,
    }
    routing_table = body if status == 200 else {}

    status, body, lat, _ = _f(f"{router_url}/admin/policy", admin_headers)
    health_results["policy"] = {
        "status": status, "latency_ms": lat, "ok": status == 200,
        "has_policy": status == 200 and bool(body),
    }

    status, body, lat, _ = _f(f"{router_url}/v1/models", v1_headers)
    models_data = body.get("data", []) if status == 200 else []
    health_results["models"] = {
        "status": status, "latency_ms": lat, "ok": status == 200,
        "model_count": len(models_data),
        "models": [m["id"] for m in models_data],
    }

    model_details = {}
    for m in models_data:
        mid = m["id"]
        status, body, lat, _ = _f(f"{router_url}/v1/models/{mid}", v1_headers)
        if status == 200:
            agents_info = body.get("agents", {})
            model_details[mid] = {
                "total_agents": agents_info.get("total", 0),
                "healthy_agents": agents_info.get("healthy", 0),
                "total_capacity": agents_info.get("total_capacity", 0),
            }
    health_results["model_details"] = model_details

    total_agents = admin_health.get("total_agents", 0)
    healthy_agents = admin_health.get("healthy_agents", 0)
    queue_length = admin_health.get("queue_length", 0)

    health_results["summary"] = {
        "total_agents": total_agents,
        "healthy_agents": healthy_agents,
        "queue_length": queue_length,
        "all_healthy": total_agents > 0 and healthy_agents == total_agents,
        "router_status": admin_health.get("status", "unknown"),
    }

    print(f"    agents={total_agents} healthy={healthy_agents} queue={queue_length} models={len(models_data)}", file=sys.stderr)
    return health_results, models_data


def run_error_handling(router_url, v1_headers, models_data, step_label, timeout):
    print(f"  [{step_label}] Error handling...", file=sys.stderr, flush=True)
    _f = lambda url, hdrs=None, method="GET", data=None, t=None: fetch(
        url, hdrs, method, data, t, timeout)

    error_results = []

    status, body, lat, _ = _f(
        f"{router_url}/v1/chat/completions", v1_headers, "POST",
        {"model": "__nonexistent_model_benchmark__",
         "messages": [{"role": "user", "content": "what is France capital?"}],
         "max_tokens": 1}
    )
    error_results.append({
        "test": "invalid_model", "expected_status": "4xx/5xx",
        "actual_status": status, "latency_ms": lat,
        "ok": status in (404, 400, 503) and lat < 5000,
        "fast_response": lat < 5000,
    })
    print(f"    invalid_model: {status} in {lat}ms", file=sys.stderr)

    status, body, lat, _ = _f(
        f"{router_url}/v1/chat/completions", v1_headers, "POST",
        {"model": models_data[0]["id"] if models_data else "test",
         "messages": [], "max_tokens": 200}
    )
    error_results.append({
        "test": "empty_messages", "expected_status": "200 or 400",
        "actual_status": status, "latency_ms": lat,
        "ok": status in (200, 400),
        "fast_response": lat < 5000,
    })
    print(f"    empty_messages: {status} in {lat}ms", file=sys.stderr)

    status, body, lat, _ = _f(
        f"{router_url}/v1/chat/completions", v1_headers, "POST",
        {"messages": [{"role": "user", "content": "what is France capital?"}],
         "max_tokens": 200}
    )
    error_results.append({
        "test": "missing_model", "expected_status": 400,
        "actual_status": status, "latency_ms": lat,
        "ok": status == 400 and lat < 2000,
        "fast_response": lat < 2000,
    })
    print(f"    missing_model: {status} in {lat}ms", file=sys.stderr)

    return error_results


def run_load_test(router_url, v1_headers, admin_headers, models_data, concurrency,
                  workload, step_label, timeout, collected_trace_ids):
    workload_name = workload["name"]
    prompt = workload["prompt"]
    max_tokens = workload["max_tokens"]
    print(
        f"  [{step_label}] Concurrent load test — {workload_name} "
        f"(n={concurrency}, prompt_chars={len(prompt)}, max_tokens={max_tokens})...",
        file=sys.stderr, flush=True,
    )
    _f = lambda url, hdrs=None, method="GET", data=None, t=None: fetch(
        url, hdrs, method, data, t, timeout)

    load_results = {"workload": workload_name}

    if not models_data:
        load_results["skipped"] = "no models available"
        print("    skipped — no models", file=sys.stderr)
        return load_results

    def send_request(idx):
        s, b, lat, hdrs = fetch(
            f"{router_url}/v1/chat/completions", v1_headers, "POST",
            {"model": TEST_MODEL,
             "messages": [{"role": "user", "content": prompt}],
             "max_tokens": max_tokens},
            req_timeout=120, default_timeout=timeout,
        )
        tid = extract_trace_id(hdrs)
        if tid and s == 200:
            collected_trace_ids.append(tid)
        return {"index": idx, "status": s, "latency_ms": lat, "ok": s == 200, "trace_id": tid}

    start_time = time.time()
    results_list = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(send_request, i): i for i in range(concurrency)}
        for f in as_completed(futures):
            results_list.append(f.result())
    wall_time_ms = int((time.time() - start_time) * 1000)

    latencies = sorted([r["latency_ms"] for r in results_list if r["ok"]])
    success_count = sum(1 for r in results_list if r["ok"])
    fail_count = concurrency - success_count

    load_results.update({
        "model": TEST_MODEL,
        "prompt_chars": len(prompt),
        "max_tokens": max_tokens,
        "concurrency": concurrency,
        "total_requests": concurrency,
        "success_count": success_count,
        "fail_count": fail_count,
        "success_rate": round(success_count / concurrency, 3),
        "wall_time_ms": wall_time_ms,
        "requests_per_second": round(concurrency / (wall_time_ms / 1000), 2) if wall_time_ms > 0 else 0,
        "latency_p50_ms": percentile(latencies, 50),
        "latency_p95_ms": percentile(latencies, 95),
        "latency_p99_ms": percentile(latencies, 99),
        "latency_min_ms": min(latencies) if latencies else 0,
        "latency_max_ms": max(latencies) if latencies else 0,
    })

    print(
        f"    {success_count}/{concurrency} ok, "
        f"p50={load_results['latency_p50_ms']}ms "
        f"p95={load_results['latency_p95_ms']}ms "
        f"wall={wall_time_ms}ms",
        file=sys.stderr,
    )

    time.sleep(1)
    status, body, lat, _ = _f(f"{router_url}/admin/routing-table", admin_headers)
    if status == 200:
        agents_after = body.get("agents", [])
        active_slots = sum(a.get("active_requests", 0) for a in agents_after)
        load_results["post_load_active_slots"] = active_slots
        load_results["slot_leak_detected"] = active_slots > 0
        if active_slots > 0:
            print(f"    WARNING: {active_slots} active slots still held after {workload_name} load test", file=sys.stderr)

    return load_results


def run_routing_distribution(router_url, v1_headers, admin_headers, models_data, total_agents,
                              step_label, timeout, collected_trace_ids):
    print(f"  [{step_label}] Routing distribution...", file=sys.stderr, flush=True)
    _f = lambda url, hdrs=None, method="GET", data=None, t=None: fetch(
        url, hdrs, method, data, t, timeout)

    distribution_results = {}

    if not models_data:
        distribution_results["skipped"] = "no models available"
        print("    skipped — no models", file=sys.stderr)
        return distribution_results

    test_model = TEST_MODEL

    pre_snapshot = {}
    status, body, lat, _ = _f(f"{router_url}/admin/routing-table", admin_headers)
    if status == 200:
        for a in body.get("agents", []):
            if a.get("model") == test_model:
                pre_snapshot[a.get("id", a.get("url", ""))] = a.get("success_total", 0)

    batch_size = max(6, total_agents * 3) if total_agents > 0 else 6
    batch_size = min(batch_size, 20)
    batch_results = []
    for i in range(batch_size):
        s, b, lat, hdrs = fetch(
            f"{router_url}/v1/chat/completions", v1_headers, "POST",
            {"model": test_model,
             "messages": [{"role": "user", "content": f"Reply with the number {i}."}],
             "max_tokens": 5},
            req_timeout=60, default_timeout=timeout,
        )
        tid = extract_trace_id(hdrs)
        if tid and s == 200:
            collected_trace_ids.append(tid)
        batch_results.append({"status": s, "latency_ms": lat, "ok": s == 200, "trace_id": tid})

    batch_success = sum(1 for r in batch_results if r["ok"])
    batch_latencies = sorted([r["latency_ms"] for r in batch_results if r["ok"]])

    distribution_results = {
        "model": test_model,
        "batch_size": batch_size,
        "success_count": batch_success,
        "success_rate": round(batch_success / batch_size, 3) if batch_size > 0 else 0,
        "latency_p50_ms": percentile(batch_latencies, 50) if batch_latencies else 0,
        "latency_avg_ms": int(sum(batch_latencies) / len(batch_latencies)) if batch_latencies else 0,
    }

    status, body, lat, _ = _f(f"{router_url}/admin/routing-table", admin_headers)
    if status == 200:
        agents_rt = body.get("agents", [])
        model_agents = [a for a in agents_rt if a.get("model") == test_model]
        if model_agents:
            delta_counts = []
            for a in model_agents:
                aid = a.get("id", a.get("url", ""))
                post = a.get("success_total", 0)
                pre = pre_snapshot.get(aid, 0)
                delta_counts.append(post - pre)
            distribution_results["agent_count"] = len(model_agents)
            distribution_results["agents_with_traffic"] = sum(1 for d in delta_counts if d > 0)
            if len(model_agents) > 1 and sum(delta_counts) > 0:
                avg_count = sum(delta_counts) / len(delta_counts)
                if avg_count > 0:
                    variance = sum((c - avg_count) ** 2 for c in delta_counts) / len(delta_counts)
                    cv = (variance ** 0.5) / avg_count
                    distribution_results["spread_score"] = round(max(0, 1.0 - cv), 3)
                else:
                    distribution_results["spread_score"] = 0

    print(
        f"    {batch_success}/{batch_size} ok, "
        f"agents_with_traffic={distribution_results.get('agents_with_traffic', '?')}",
        file=sys.stderr,
    )
    return distribution_results


def run_admin_latency(router_url, v1_headers, admin_headers, step_label, timeout):
    samples = max(1, _env_int("ADMIN_LATENCY_SAMPLES", 10))
    print(
        f"  [{step_label}] Admin API latency ({samples} samples/endpoint)...",
        file=sys.stderr, flush=True,
    )
    _f = lambda url, hdrs=None: fetch(url, hdrs, default_timeout=timeout)

    endpoints = [
        ("health", f"{router_url}/health", None),
        ("admin_health", f"{router_url}/admin/health", admin_headers),
        ("routing_table", f"{router_url}/admin/routing-table", admin_headers),
        ("policy", f"{router_url}/admin/policy", admin_headers),
        ("models", f"{router_url}/v1/models", v1_headers),
    ]
    admin_latency_results = {}
    for name, url, hdrs in endpoints:
        latencies_admin = []
        for _ in range(samples):
            s, _, lat, _ = _f(url, hdrs)
            if s == 200:
                latencies_admin.append(lat)
        sorted_lat = sorted(latencies_admin)
        admin_latency_results[name] = {
            "avg_ms": int(sum(sorted_lat) / len(sorted_lat)) if sorted_lat else 0,
            "p95_ms": percentile(sorted_lat, 95) if sorted_lat else 0,
            "min_ms": min(sorted_lat) if sorted_lat else 0,
            "max_ms": max(sorted_lat) if sorted_lat else 0,
            "sample_count": len(sorted_lat),
            "ok": len(sorted_lat) == samples,
        }
        print(
            f"    {name}: avg={admin_latency_results[name]['avg_ms']}ms "
            f"p95={admin_latency_results[name]['p95_ms']}ms "
            f"(n={len(sorted_lat)})",
            file=sys.stderr,
        )

    return admin_latency_results


def run_trace_timing(tempo_url, tempo_basic_auth, collected_trace_ids, step_label, timeout):
    print(
        f"  [{step_label}] Trace timing breakdown ({len(collected_trace_ids)} traces)...",
        file=sys.stderr, flush=True,
    )

    def parse_spans(trace_json):
        spans = []
        for batch in trace_json.get("batches", []):
            for scope_spans in batch.get("scopeSpans", []):
                for span in scope_spans.get("spans", []):
                    attrs = {}
                    for a in span.get("attributes", []):
                        key = a.get("key", "")
                        val = a.get("value", {})
                        for vtype in ("stringValue", "intValue", "doubleValue", "boolValue"):
                            if vtype in val:
                                attrs[key] = val[vtype]
                                break
                    start_ns = int(span.get("startTimeUnixNano", "0"))
                    end_ns = int(span.get("endTimeUnixNano", "0"))
                    duration_ms = (end_ns - start_ns) / 1e6 if end_ns > start_ns else 0
                    spans.append({
                        "name": span.get("name", ""),
                        "span_id": span.get("spanId", ""),
                        "parent_span_id": span.get("parentSpanId", ""),
                        "start_ns": start_ns,
                        "end_ns": end_ns,
                        "duration_ms": round(duration_ms, 2),
                        "attributes": attrs,
                    })
        return spans

    def extract_phase_timing(spans):
        phases = {}
        for s in spans:
            name = s["name"]
            if name in ("dispatch", "forward_to_agent", "forward_to_backend"):
                phases[name] = {"duration_ms": s["duration_ms"]}
                if "rtt_ms" in s["attributes"]:
                    phases[name]["rtt_ms"] = float(s["attributes"]["rtt_ms"])
                if "duration_ms" in s["attributes"]:
                    phases[name]["inference_ms"] = float(s["attributes"]["duration_ms"])
                if "response.tokens_per_second" in s["attributes"]:
                    phases[name]["tokens_per_second"] = float(s["attributes"]["response.tokens_per_second"])

        root_candidates = [s for s in spans if not s.get("parent_span_id")]
        if not root_candidates:
            root_candidates = sorted(spans, key=lambda s: s["duration_ms"], reverse=True)
        if root_candidates:
            root = root_candidates[0]
            phases["total"] = {"duration_ms": root["duration_ms"]}

            dispatch_spans = [s for s in spans if s["name"] == "dispatch"]
            if dispatch_spans:
                queue_wait_ms = (dispatch_spans[0]["start_ns"] - root["start_ns"]) / 1e6
                phases["queue_wait"] = {"duration_ms": round(max(0, queue_wait_ms), 2)}

        # Router-only derived phases — these are what the benchmark gates on
        # when "we care about router operations, not inference."
        #   routing_overhead = total - backend inference
        #   dispatch_policy  = policy eval time (dispatch span minus nested forward)
        if "total" in phases and "forward_to_backend" in phases:
            phases["routing_overhead"] = {
                "duration_ms": round(max(
                    0, phases["total"]["duration_ms"] - phases["forward_to_backend"]["duration_ms"]
                ), 2)
            }
        if "dispatch" in phases and "forward_to_agent" in phases:
            phases["dispatch_policy"] = {
                "duration_ms": round(max(
                    0, phases["dispatch"]["duration_ms"] - phases["forward_to_agent"]["duration_ms"]
                ), 2)
            }

        return phases

    tempo_headers = {}
    if tempo_basic_auth:
        encoded = base64.b64encode(tempo_basic_auth.encode()).decode()
        tempo_headers["Authorization"] = f"Basic {encoded}"

    test_s, test_body, test_lat, _ = fetch(
        f"{tempo_url}/api/search?limit=1", tempo_headers, req_timeout=10, default_timeout=timeout)
    tempo_ok = test_s == 200
    if not tempo_ok:
        print(f"    ERROR: Tempo not reachable (status={test_s}, body={str(test_body)[:100]})", file=sys.stderr)
    else:
        print(f"    Tempo reachable ({test_lat}ms)", file=sys.stderr)

    per_trace_phases = []
    fetch_errors = []
    success = 0

    if tempo_ok:
        if not collected_trace_ids:
            print("    ERROR: No traces collected — skipping phase timing analysis", file=sys.stderr)
        else:
            sample_tid = collected_trace_ids[0]
            print(f"    polling trace {sample_tid[:12]}... until agent spans arrive", file=sys.stderr, flush=True)
            ready = False
            for attempt in range(8):
                time.sleep(8)
                try:
                    s, body, _, _ = fetch(
                        f"{tempo_url}/api/traces/{sample_tid}", tempo_headers,
                        req_timeout=10, default_timeout=timeout)
                    if s == 200:
                        spans = parse_spans(body)
                        names = [sp["name"] for sp in spans]
                        print(f"      [{attempt+1}/8] {len(spans)} spans: {names}", file=sys.stderr, flush=True)
                        if len(spans) >= 3:
                            ready = True
                            break
                    else:
                        print(f"      [{attempt+1}/8] HTTP {s}", file=sys.stderr, flush=True)
                except Exception as e:
                    print(f"      [{attempt+1}/8] error: {e}", file=sys.stderr, flush=True)

            if ready:
                # Per-trace retry: agent spans may flush later than router spans,
                # so a trace that's incomplete on the first fetch often fills in
                # a few seconds later. Retry up to 3 times per trace.
                for tid in collected_trace_ids[:20]:
                    last_err = None
                    for attempt in range(3):
                        if attempt > 0:
                            time.sleep(5)
                        try:
                            s, body, _, _ = fetch(
                                f"{tempo_url}/api/traces/{tid}", tempo_headers,
                                req_timeout=10, default_timeout=timeout)
                            if s == 200:
                                spans = parse_spans(body)
                                if len(spans) >= 3:
                                    phases = extract_phase_timing(spans)
                                    if phases and len(phases) >= 2:
                                        per_trace_phases.append(phases)
                                        success += 1
                                        last_err = None
                                        break
                                    last_err = f"{tid[:8]}: {len(phases)} phases from {len(spans)} spans"
                                else:
                                    last_err = f"{tid[:8]}: only {len(spans)} spans"
                            else:
                                last_err = f"{tid[:8]}: HTTP {s}"
                        except Exception as e:
                            last_err = f"{tid[:8]}: {e}"
                    if last_err:
                        fetch_errors.append(last_err)
            else:
                fetch_errors.append(f"timed out: agent spans never arrived for {sample_tid[:12]}")

    print(f"    fetched {success}/{min(len(collected_trace_ids), 20)} traces", file=sys.stderr)
    if fetch_errors and success == 0:
        for err in fetch_errors[:5]:
            print(f"    ERROR: {err}", file=sys.stderr)

    trace_diagnostics = {
        "tempo_reachable": tempo_ok,
        "tempo_status": test_s,
        "errors": fetch_errors[:5],
    }

    if per_trace_phases:
        all_phase_names = set()
        for p in per_trace_phases:
            all_phase_names.update(p.keys())

        phase_summary = {}
        for phase_name in sorted(all_phase_names):
            durations = sorted([
                p[phase_name]["duration_ms"]
                for p in per_trace_phases
                if phase_name in p
            ])
            if durations:
                phase_summary[phase_name] = {
                    "avg_ms": round(sum(durations) / len(durations), 1),
                    "p50_ms": round(percentile(durations, 50), 1),
                    "p95_ms": round(percentile(durations, 95), 1),
                    "min_ms": round(min(durations), 1),
                    "max_ms": round(max(durations), 1),
                    "sample_count": len(durations),
                }
                rtt_values = [
                    p[phase_name].get("rtt_ms")
                    for p in per_trace_phases
                    if phase_name in p and "rtt_ms" in p[phase_name]
                ]
                if rtt_values:
                    phase_summary[phase_name]["avg_rtt_ms"] = round(
                        sum(rtt_values) / len(rtt_values), 1)

        return {
            "enabled": True,
            "traces_collected": len(collected_trace_ids),
            "traces_fetched": success,
            "phases": phase_summary,
            "diagnostics": trace_diagnostics,
        }
    else:
        return {
            "enabled": True,
            "traces_collected": len(collected_trace_ids),
            "traces_fetched": 0,
            "phases": {},
            "diagnostics": trace_diagnostics,
        }


# ── SLO evaluation ────────────────────────────────────────────────────────────

def load_slo_thresholds():
    return {
        "success_rate_min": _env_float("SLO_SUCCESS_RATE_MIN", 0.95),
        "admin_p95_ms": _env_int("SLO_ADMIN_P95_MS", 1000),
        "routing_overhead_p95_ms": _env_int("SLO_ROUTING_OVERHEAD_P95_MS", 100),
        "queue_wait_p95_ms": _env_int("SLO_QUEUE_WAIT_P95_MS", 50),
        "dispatch_policy_p95_ms": _env_int("SLO_DISPATCH_POLICY_P95_MS", 20),
    }


def evaluate_slos(slo, load_trivial, load_midsize, admin_latency, trace_timing,
                  tempo_required):
    """Return (checks, evaluations) where checks is list of (name, ok) and
    evaluations maps check_name → {threshold, actual, unit} for reporting.

    Gates success rate on both workloads, admin endpoint latency, and the
    three router-phase metrics derived from Tempo spans. End-to-end workload
    latency is intentionally NOT gated — it's dominated by model inference,
    which this benchmark doesn't claim to measure."""
    checks = []
    evaluations = {}

    def add(name, ok, threshold, actual, unit=""):
        checks.append((name, ok))
        evaluations[name] = {"threshold": threshold, "actual": actual, "unit": unit}

    for workload_results, key_prefix in (
        (load_trivial, "trivial"),
        (load_midsize, "midsize"),
    ):
        if "success_rate" not in workload_results:
            continue
        sr = workload_results["success_rate"]
        add(f"slo_{key_prefix}_success_rate", sr >= slo["success_rate_min"],
            slo["success_rate_min"], sr)

    for name, info in admin_latency.items():
        p95_ms = info.get("p95_ms", 0)
        add(f"slo_admin_{name}_p95_ms", p95_ms <= slo["admin_p95_ms"],
            slo["admin_p95_ms"], p95_ms, "ms")

    # Router-phase SLOs sourced from Tempo trace spans.
    phases = trace_timing.get("phases", {}) if trace_timing.get("enabled") else {}
    for phase_name, slo_key, check_name in (
        ("routing_overhead", "routing_overhead_p95_ms", "slo_routing_overhead_p95_ms"),
        ("queue_wait", "queue_wait_p95_ms", "slo_queue_wait_p95_ms"),
        ("dispatch_policy", "dispatch_policy_p95_ms", "slo_dispatch_policy_p95_ms"),
    ):
        budget = slo[slo_key]
        if phase_name in phases:
            p95 = phases[phase_name].get("p95_ms", 0)
            add(check_name, p95 <= budget, budget, p95, "ms")
        elif tempo_required:
            add(check_name, False, budget, "no_trace_data", "ms")

    if tempo_required:
        reachable = bool(
            trace_timing.get("enabled")
            and trace_timing.get("diagnostics", {}).get("tempo_reachable", False)
        )
        add("slo_tempo_reachable", reachable, True, reachable)
        fetched = trace_timing.get("traces_fetched", 0)
        add("slo_tempo_traces_fetched", fetched > 0, 1, fetched, "traces")

    return checks, evaluations


# ── Markdown generation ───────────────────────────────────────────────────────

def _render_load_section(lines, load, title):
    if "success_rate" not in load:
        return
    lines.append(f"### {title}")
    lines.append("")
    extras = []
    if "prompt_chars" in load:
        extras.append(f"**Prompt:** ~{load['prompt_chars']} chars")
    if "max_tokens" in load:
        extras.append(f"**Max tokens:** {load['max_tokens']}")
    header = f"**Model:** {load['model']} | **Concurrency:** {load['concurrency']}"
    if extras:
        header += " | " + " | ".join(extras)
    lines.append(header)
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|:-------|------:|")
    lines.append(f"| Success Rate | {load['success_rate'] * 100:.1f}% ({load['success_count']}/{load['total_requests']}) |")
    lines.append(f"| Throughput | {load['requests_per_second']} req/s |")
    lines.append(f"| p50 Latency | {load['latency_p50_ms']:,}ms |")
    lines.append(f"| p95 Latency | {load['latency_p95_ms']:,}ms |")
    lines.append(f"| p99 Latency | {load['latency_p99_ms']:,}ms |")
    lines.append(f"| Min Latency | {load['latency_min_ms']:,}ms |")
    lines.append(f"| Max Latency | {load['latency_max_ms']:,}ms |")
    lines.append(f"| Wall Time | {load['wall_time_ms']:,}ms |")
    if "slot_leak_detected" in load:
        leak = "YES" if load["slot_leak_detected"] else "none"
        lines.append(f"| Slot Leak | {leak} |")
    lines.append("")


def generate_markdown(data):
    meta = data["meta"]
    checks = data["checks"]
    health = data["cluster_health"]
    errors = data["error_handling"]
    load = data["concurrent_load"]
    midsize_load = data.get("midsize_load", {})
    dist = data["routing_distribution"]
    admin_lat = data["admin_api_latency"]
    traces = data.get("trace_timing", {})
    slo_evaluations = data.get("slo_evaluations", {})
    slo_thresholds = meta.get("slo_thresholds", {})

    lines = []
    lines.append("## Operational Benchmark Results")
    lines.append("")
    lines.append(
        f"**Commit:** `{meta['commit']}` | **Date:** {meta['timestamp']} | "
        f"**Checks:** {meta['pass_count']}/{meta['check_count']} passed"
    )
    lines.append("")

    lines.append("### Health Checks")
    lines.append("")
    lines.append("| Check | Status |")
    lines.append("|:------|:------:|")
    for name, ok in checks.items():
        icon = "PASS" if ok else "FAIL"
        lines.append(f"| {name} | {icon} |")
    lines.append("")

    summary = health.get("summary", {})
    lines.append("### Cluster State")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|:-------|------:|")
    lines.append(f"| Agents | {summary.get('total_agents', 0)} |")
    lines.append(f"| Healthy | {summary.get('healthy_agents', 0)} |")
    lines.append(f"| Queue | {summary.get('queue_length', 0)} |")
    lines.append(f"| Models | {health.get('models', {}).get('model_count', 0)} |")
    lines.append(f"| Router Status | {summary.get('router_status', '?')} |")
    lines.append("")

    if health.get("model_details"):
        lines.append("### Models")
        lines.append("")
        lines.append("| Model | Agents | Healthy | Capacity |")
        lines.append("|:------|:------:|:-------:|:--------:|")
        for mid, info in health["model_details"].items():
            lines.append(
                f"| {mid} | {info['total_agents']} | {info['healthy_agents']} | {info['total_capacity']} |"
            )
        lines.append("")

    lines.append("### Error Handling")
    lines.append("")
    lines.append("| Test | Status | Latency | Fast |")
    lines.append("|:-----|:------:|--------:|:----:|")
    for er in errors:
        ok = "PASS" if er["ok"] else "FAIL"
        fast = "yes" if er["fast_response"] else "no"
        lines.append(f"| {er['test']} | {er['actual_status']} ({ok}) | {er['latency_ms']}ms | {fast} |")
    lines.append("")

    _render_load_section(lines, load, "Concurrent Load Test — trivial workload")
    _render_load_section(lines, midsize_load, "Concurrent Load Test — midsize workload")

    if "success_rate" in dist:
        lines.append("### Routing Distribution")
        lines.append("")
        lines.append(f"**Model:** {dist['model']} | **Batch:** {dist['batch_size']} requests")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|:-------|------:|")
        lines.append(f"| Success Rate | {dist['success_rate'] * 100:.1f}% |")
        lines.append(f"| p50 Latency | {dist['latency_p50_ms']:,}ms |")
        lines.append(f"| Avg Latency | {dist['latency_avg_ms']:,}ms |")
        if "agent_count" in dist:
            lines.append(f"| Total Agents | {dist['agent_count']} |")
        if "agents_with_traffic" in dist:
            lines.append(f"| Agents With Traffic | {dist['agents_with_traffic']} |")
        if "spread_score" in dist:
            lines.append(f"| Spread Score | {dist['spread_score']} (1.0 = even) |")
        lines.append("")

    lines.append("### Admin API Latency")
    lines.append("")
    lines.append("| Endpoint | Avg | p95 | Min | Max | Samples |")
    lines.append("|:---------|----:|----:|----:|----:|--------:|")
    for name, info in admin_lat.items():
        lines.append(
            f"| {name} | {info['avg_ms']}ms | {info.get('p95_ms', 0)}ms | "
            f"{info['min_ms']}ms | {info['max_ms']}ms | {info.get('sample_count', '?')} |"
        )
    lines.append("")

    if slo_evaluations:
        lines.append("### SLO Evaluation")
        lines.append("")
        strict = meta.get("strict", False)
        tempo_required = meta.get("tempo_required", False)
        mode_bits = []
        if strict:
            mode_bits.append("`STRICT`")
        if tempo_required:
            mode_bits.append("`TEMPO_REQUIRED`")
        if mode_bits:
            lines.append("**Mode:** " + " ".join(mode_bits))
            lines.append("")
        lines.append("| SLO | Budget | Actual | Status |")
        lines.append("|:----|-------:|-------:|:------:|")
        for name, info in slo_evaluations.items():
            ok = checks.get(name, False)
            icon = "PASS" if ok else "FAIL"
            unit = info.get("unit", "")
            threshold = info["threshold"]
            actual = info["actual"]
            t_str = f"{threshold}{unit}" if unit else str(threshold)
            a_str = f"{actual}{unit}" if unit else str(actual)
            lines.append(f"| {name} | {t_str} | {a_str} | {icon} |")
        lines.append("")

    if traces.get("enabled") and traces.get("phases"):
        phases = traces["phases"]
        lines.append("### Trace Timing Breakdown")
        lines.append("")
        lines.append(f"**Traces:** {traces['traces_fetched']}/{traces['traces_collected']} fetched from Tempo")
        lines.append("")

        phase_order = [
            "total", "queue_wait", "dispatch_policy", "routing_overhead",
            "dispatch", "forward_to_agent", "forward_to_backend",
        ]
        phase_labels = {
            "total": "Total (end-to-end)",
            "queue_wait": "Queue Wait",
            "dispatch_policy": "Policy Eval (router-only)",
            "routing_overhead": "Routing Overhead (total − backend)",
            "dispatch": "Dispatch (policy + forward)",
            "forward_to_agent": "Forward to Agent",
            "forward_to_backend": "Backend Processing",
        }
        ordered = [p for p in phase_order if p in phases] + [p for p in phases if p not in phase_order]

        lines.append("| Phase | Avg | p50 | p95 | Min | Max |")
        lines.append("|:------|----:|----:|----:|----:|----:|")
        for p in ordered:
            info = phases[p]
            label = phase_labels.get(p, p)
            lines.append(
                f"| {label} | {info['avg_ms']}ms | {info['p50_ms']}ms | "
                f"{info['p95_ms']}ms | {info['min_ms']}ms | {info['max_ms']}ms |"
            )
        lines.append("")

        if "total" in phases and "forward_to_backend" in phases:
            overhead = round(phases["total"]["avg_ms"] - phases["forward_to_backend"]["avg_ms"], 1)
            pct = round(overhead / phases["total"]["avg_ms"] * 100, 1) if phases["total"]["avg_ms"] > 0 else 0
            lines.append(f"**Routing overhead:** {overhead}ms avg ({pct}% of total)")
            lines.append("")
    elif traces.get("enabled") and not traces.get("phases"):
        diag = traces.get("diagnostics", {})
        lines.append("### Trace Timing Breakdown")
        lines.append("")
        lines.append(
            f"*Traces collected: {traces.get('traces_collected', 0)}, "
            f"fetched: {traces.get('traces_fetched', 0)} — no phase data extracted*"
        )
        lines.append("")
        tempo_status = "yes" if diag.get("tempo_reachable") else f"NO (HTTP {diag.get('tempo_status', '?')})"
        lines.append(f"Tempo reachable: {tempo_status}")
        if diag.get("errors"):
            lines.append("")
            lines.append("Errors:")
            for e in diag["errors"]:
                lines.append("- `" + str(e) + "`")
        lines.append("")
    elif not traces.get("enabled"):
        lines.append("### Trace Timing Breakdown")
        lines.append("")
        lines.append(f"*Skipped: {traces.get('reason', 'unknown')}*")
        lines.append("")

    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    router_url = args.router_url.rstrip("/")
    api_key = args.api_key
    admin_key = args.admin_key or api_key  # defaults to api_key if not provided
    commit = args.commit_sha
    timeout = args.timeout
    concurrency = args.concurrency
    tempo_url = os.environ.get("TEMPO_URL", "").strip().rstrip("/")
    tempo_basic_auth = os.environ.get("TEMPO_BASIC_AUTH", "")

    if not router_url or not api_key:
        print("Error: ROUTER_URL (-u) and LLM_API_KEY (-k) are required", file=sys.stderr)
        sys.exit(1)

    strict = _env_flag("STRICT")
    tempo_required = _env_flag("TEMPO_REQUIRED")

    print(
        f"Ops benchmark starting — commit={commit} concurrency={concurrency} "
        f"strict={strict} tempo_required={tempo_required}",
        file=sys.stderr,
    )

    step_count = "7" if tempo_url else "6"
    collected_trace_ids = []

    v1_headers = {"Authorization": f"Bearer {api_key}"}
    admin_headers = {"Authorization": f"Bearer {admin_key}"}

    health_results, models_data = run_health_snapshot(
        router_url, v1_headers, admin_headers, f"1/{step_count}", timeout)
    error_results = run_error_handling(
        router_url, v1_headers, models_data, f"2/{step_count}", timeout)
    load_results = run_load_test(
        router_url, v1_headers, admin_headers, models_data, concurrency,
        WORKLOADS[0], f"3/{step_count}", timeout, collected_trace_ids)
    midsize_results = run_load_test(
        router_url, v1_headers, admin_headers, models_data, concurrency,
        WORKLOADS[1], f"4/{step_count}", timeout, collected_trace_ids)
    total_agents = health_results["summary"]["total_agents"]
    distribution_results = run_routing_distribution(
        router_url, v1_headers, admin_headers, models_data, total_agents,
        f"5/{step_count}", timeout, collected_trace_ids)
    admin_latency_results = run_admin_latency(
        router_url, v1_headers, admin_headers, f"6/{step_count}", timeout)

    if tempo_url and collected_trace_ids:
        trace_timing = run_trace_timing(
            tempo_url, tempo_basic_auth, collected_trace_ids, f"7/{step_count}", timeout)
    elif not tempo_url:
        trace_timing = {"enabled": False, "reason": "TEMPO_URL not set"}
        print(f"  [7/{step_count}] Trace timing — skipped (TEMPO_URL not set)", file=sys.stderr)
    else:
        trace_timing = {"enabled": False, "reason": "no trace IDs collected"}
        print(f"  [7/{step_count}] Trace timing — skipped (no trace IDs collected)", file=sys.stderr)

    # Assemble checks
    checks = []
    checks.append(("router_healthy", health_results["liveness"]["ok"]))
    checks.append(("admin_healthy", health_results["admin_health"]["ok"]))
    checks.append(("agents_registered", health_results["summary"]["total_agents"] > 0))
    checks.append(("all_agents_healthy", health_results["summary"]["all_healthy"]))
    checks.append(("policy_loaded", health_results["policy"]["has_policy"]))
    checks.append(("models_discovered", health_results["models"]["model_count"] > 0))
    for er in error_results:
        checks.append((f"error_{er['test']}", er["ok"]))
    if "success_rate" in load_results:
        checks.append(("load_trivial_success", load_results["success_rate"] >= 0.8))
        checks.append(("no_slot_leak_trivial", not load_results.get("slot_leak_detected", True)))
    if "success_rate" in midsize_results:
        checks.append(("load_midsize_success", midsize_results["success_rate"] >= 0.8))
        checks.append(("no_slot_leak_midsize", not midsize_results.get("slot_leak_detected", True)))
    if "success_rate" in distribution_results:
        checks.append(("distribution_success", distribution_results["success_rate"] >= 0.8))

    slo_thresholds = load_slo_thresholds()
    slo_checks, slo_evaluations = evaluate_slos(
        slo_thresholds, load_results, midsize_results, admin_latency_results,
        trace_timing, tempo_required,
    )
    checks.extend(slo_checks)

    pass_count = sum(1 for _, ok in checks if ok)
    fail_count_total = len(checks) - pass_count

    output = {
        "meta": {
            "commit": commit,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "router_url": router_url,
            "type": "ops",
            "concurrency": concurrency,
            "strict": strict,
            "tempo_required": tempo_required,
            "slo_thresholds": slo_thresholds,
            "check_count": len(checks),
            "pass_count": pass_count,
            "fail_count": fail_count_total,
        },
        "checks": {name: ok for name, ok in checks},
        "slo_evaluations": slo_evaluations,
        "cluster_health": health_results,
        "error_handling": error_results,
        "concurrent_load": load_results,
        "midsize_load": midsize_results,
        "routing_distribution": distribution_results,
        "admin_api_latency": admin_latency_results,
        "trace_timing": trace_timing,
    }

    json_str = json.dumps(output, indent=2)
    if args.json_out:
        with open(args.json_out, "w") as fh:
            fh.write(json_str)
        print(f"JSON results written to {args.json_out}", file=sys.stderr)
    else:
        print(json_str)

    md = generate_markdown(output)
    if args.md_out:
        with open(args.md_out, "w") as fh:
            fh.write(md)
        print(f"Markdown summary written to {args.md_out}", file=sys.stderr)
    else:
        print(md, file=sys.stderr)

    print(
        f"Ops benchmark complete — {pass_count}/{len(checks)} checks passed",
        file=sys.stderr,
    )

    if strict and fail_count_total > 0:
        failed_names = [name for name, ok in checks if not ok]
        print(
            f"STRICT: {fail_count_total} check(s) failed — exiting non-zero: "
            f"{', '.join(failed_names)}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
