#!/usr/bin/env python3
"""Compare two benchmark result JSON files and output a markdown delta table."""
import json
import sys


def load_results(path):
    with open(path) as f:
        return json.load(f)


def build_index(data):
    """Index test results by (model, target_prompt_tokens, max_tokens)."""
    idx = {}
    for model_data in data.get("models", []):
        for t in model_data.get("tests", []):
            if t.get("status") != "ok":
                continue
            key = (t["model"], t["target_prompt_tokens"], t["max_tokens"])
            idx[key] = t
    return idx


# Absolute-delta noise floors. A delta only lights up 🔴/🟢 when it clears
# BOTH a percentage threshold (2%) AND the per-family floor below. This
# keeps tiny absolute changes (e.g. "+50% 🔴" on a 0.2ms → 0.3ms Tempo
# span, or +10% on a 480ms network-bound admin latency) from looking like
# alarms — they render as ⚪ with the percentage shown for info only.
NOISE_FLOOR = {
    "router_phase_ms": 5,            # Tempo span deltas; sub-ms is jitter
    "admin_latency_ms": 100,         # CI-runner network path adds ~50ms jitter
    "trivial_latency_ms": 500,       # cold-start / model-load variance
    "midsize_latency_ms": 2000,      # inference variance dominates
    "throughput_rps_trivial": 0.5,
    "throughput_rps_midsize": 0.05,
    "success_rate": 0.02,
}


def delta_str(old_val, new_val, unit="", higher_is_better=True, min_abs_delta=0):
    if old_val == 0:
        return "N/A"
    pct = ((new_val - old_val) / old_val) * 100
    abs_delta = abs(new_val - old_val)
    sign = "+" if pct >= 0 else ""
    significant = abs(pct) > 2 and abs_delta >= min_abs_delta
    good = (pct >= 0) == higher_is_better
    icon = "🟢" if good and significant else ("🔴" if not good and significant else "⚪")
    return f"{sign}{pct:.1f}% {icon}"


def compare_ops(prev, curr):
    """Compare two ops benchmark result JSON files and return markdown lines."""
    prev_meta = prev.get("meta", {})
    curr_meta = curr.get("meta", {})

    lines = [
        "",
        "---",
        "",
        "## Ops Benchmark Comparison",
        "",
        f"**Previous:** `{prev_meta.get('commit', '?')[:12]}` ({prev_meta.get('timestamp', '?')})",
        f"**Current:** `{curr_meta.get('commit', '?')[:12]}` ({curr_meta.get('timestamp', '?')})",
        "",
    ]

    # Checks delta
    prev_checks = prev.get("checks", {})
    curr_checks = curr.get("checks", {})
    all_check_names = sorted(set(list(prev_checks.keys()) + list(curr_checks.keys())))
    if all_check_names:
        lines.append("### Health Checks")
        lines.append("")
        lines.append("| Check | Previous | Current |")
        lines.append("|:------|:--------:|:-------:|")
        for name in all_check_names:
            p = "PASS" if prev_checks.get(name) else ("FAIL" if name in prev_checks else "—")
            c = "PASS" if curr_checks.get(name) else ("FAIL" if name in curr_checks else "—")
            lines.append(f"| {name} | {p} | {c} |")
        lines.append("")

    # Cluster state delta
    prev_health = prev.get("cluster_health", {}).get("summary", {})
    curr_health = curr.get("cluster_health", {}).get("summary", {})
    if prev_health or curr_health:
        lines.append("### Cluster State")
        lines.append("")
        lines.append("| Metric | Previous | Current |")
        lines.append("|:-------|:--------:|:-------:|")
        for key in ["total_agents", "healthy_agents", "queue_length"]:
            pv = prev_health.get(key, "—")
            cv = curr_health.get(key, "—")
            lines.append(f"| {key} | {pv} | {cv} |")
        lines.append("")

    # Concurrent load delta — compare every workload present in both runs.
    # Noise floors are per-workload because trivial + midsize have very
    # different measurement variance.
    load_metrics = [
        ("success_rate", True, lambda v: f"{v * 100:.1f}%"),
        ("requests_per_second", True, lambda v: f"{v}"),
        ("latency_p50_ms", False, lambda v: f"{v:,}ms"),
        ("latency_p95_ms", False, lambda v: f"{v:,}ms"),
        ("latency_p99_ms", False, lambda v: f"{v:,}ms"),
    ]
    load_floors = {
        "trivial": {
            "success_rate": NOISE_FLOOR["success_rate"],
            "requests_per_second": NOISE_FLOOR["throughput_rps_trivial"],
            "latency_p50_ms": NOISE_FLOOR["trivial_latency_ms"],
            "latency_p95_ms": NOISE_FLOOR["trivial_latency_ms"],
            "latency_p99_ms": NOISE_FLOOR["trivial_latency_ms"],
        },
        "midsize": {
            "success_rate": NOISE_FLOOR["success_rate"],
            "requests_per_second": NOISE_FLOOR["throughput_rps_midsize"],
            "latency_p50_ms": NOISE_FLOOR["midsize_latency_ms"],
            "latency_p95_ms": NOISE_FLOOR["midsize_latency_ms"],
            "latency_p99_ms": NOISE_FLOOR["midsize_latency_ms"],
        },
    }

    def render_load_delta(title, prev_load, curr_load, workload):
        if "success_rate" not in prev_load or "success_rate" not in curr_load:
            return
        # If the previous run had zero successes, its latency and throughput
        # numbers reflect fast failures rather than real work — comparing them
        # produces misleading deltas (e.g. "-83%" when the old run was broken).
        prev_had_successes = prev_load.get("success_rate", 0) > 0
        floors = load_floors[workload]
        lines.append(f"### {title}")
        lines.append("")
        if not prev_had_successes:
            lines.append(
                "*Previous run had 0% success rate — latency / throughput "
                "deltas suppressed (they would compare fast failures vs. real work).*"
            )
            lines.append("")
        lines.append("| Metric | Previous | Current | Delta |")
        lines.append("|:-------|:--------:|:-------:|:-----:|")
        for key, higher_better, fmt in load_metrics:
            pv = prev_load.get(key, 0)
            cv = curr_load.get(key, 0)
            if key != "success_rate" and not prev_had_successes:
                d = "N/A"
            elif pv:
                d = delta_str(pv, cv, higher_is_better=higher_better,
                              min_abs_delta=floors.get(key, 0))
            else:
                d = "N/A"
            lines.append(f"| {key} | {fmt(pv)} | {fmt(cv)} | {d} |")
        lines.append("")

    render_load_delta(
        "Concurrent Load Test — trivial workload",
        prev.get("concurrent_load", {}), curr.get("concurrent_load", {}),
        "trivial",
    )
    render_load_delta(
        "Concurrent Load Test — midsize workload",
        prev.get("midsize_load", {}), curr.get("midsize_load", {}),
        "midsize",
    )

    # SLO evaluation delta — show budget, actual (prev/curr), and status transitions.
    prev_slo = prev.get("slo_evaluations", {})
    curr_slo = curr.get("slo_evaluations", {})
    if prev_slo or curr_slo:
        prev_checks_map = prev.get("checks", {})
        curr_checks_map = curr.get("checks", {})
        all_slo_keys = sorted(set(list(prev_slo.keys()) + list(curr_slo.keys())))
        lines.append("### SLO Evaluation")
        lines.append("")
        lines.append("| SLO | Budget | Prev Actual | Curr Actual | Prev | Curr |")
        lines.append("|:----|-------:|------------:|------------:|:----:|:----:|")
        for name in all_slo_keys:
            info = curr_slo.get(name) or prev_slo.get(name, {})
            unit = info.get("unit", "")
            threshold = info.get("threshold", "?")
            pa = prev_slo.get(name, {}).get("actual", "—")
            ca = curr_slo.get(name, {}).get("actual", "—")
            t_str = f"{threshold}{unit}" if unit else str(threshold)
            pa_str = f"{pa}{unit}" if unit and pa != "—" else str(pa)
            ca_str = f"{ca}{unit}" if unit and ca != "—" else str(ca)
            p_status = "PASS" if prev_checks_map.get(name) else ("FAIL" if name in prev_checks_map else "—")
            c_status = "PASS" if curr_checks_map.get(name) else ("FAIL" if name in curr_checks_map else "—")
            lines.append(f"| {name} | {t_str} | {pa_str} | {ca_str} | {p_status} | {c_status} |")
        lines.append("")

    # Admin API latency delta — compare p95 (what the SLO gates on) and avg.
    prev_admin = prev.get("admin_api_latency", {})
    curr_admin = curr.get("admin_api_latency", {})
    all_endpoints = sorted(set(list(prev_admin.keys()) + list(curr_admin.keys())))
    if all_endpoints:
        lines.append("### Admin API Latency")
        lines.append("")
        lines.append("| Endpoint | Prev p95 | Curr p95 | Δ p95 | Prev Avg | Curr Avg | Δ Avg |")
        lines.append("|:---------|:--------:|:--------:|:-----:|:--------:|:--------:|:-----:|")
        for ep in all_endpoints:
            prev_info = prev_admin.get(ep, {})
            curr_info = curr_admin.get(ep, {})
            # Fall back to max_ms for records written before p95 was captured.
            p_p95 = prev_info.get("p95_ms", prev_info.get("max_ms", 0))
            c_p95 = curr_info.get("p95_ms", curr_info.get("max_ms", 0))
            p_avg = prev_info.get("avg_ms", 0)
            c_avg = curr_info.get("avg_ms", 0)
            floor = NOISE_FLOOR["admin_latency_ms"]
            d_p95 = (delta_str(p_p95, c_p95, higher_is_better=False, min_abs_delta=floor)
                     if p_p95 else "N/A")
            d_avg = (delta_str(p_avg, c_avg, higher_is_better=False, min_abs_delta=floor)
                     if p_avg else "N/A")
            lines.append(f"| {ep} | {p_p95}ms | {c_p95}ms | {d_p95} | {p_avg}ms | {c_avg}ms | {d_avg} |")
        lines.append("")

    # Trace timing delta
    prev_traces = prev.get("trace_timing", {})
    curr_traces = curr.get("trace_timing", {})
    prev_phases = prev_traces.get("phases", {})
    curr_phases = curr_traces.get("phases", {})
    if prev_phases and curr_phases:
        all_phases = sorted(set(list(prev_phases.keys()) + list(curr_phases.keys())))
        phase_labels = {
            "total": "Total (end-to-end)",
            "queue_wait": "Queue Wait",
            "dispatch_policy": "Policy Eval (router-only)",
            "routing_overhead": "Routing Overhead (total − backend)",
            "dispatch": "Dispatch (policy + forward)",
            "forward_to_agent": "Forward to Agent",
            "forward_to_backend": "Backend Processing",
        }
        phase_order = [
            "total", "queue_wait", "dispatch_policy", "routing_overhead",
            "dispatch", "forward_to_agent", "forward_to_backend",
        ]
        ordered = [p for p in phase_order if p in all_phases] + [p for p in all_phases if p not in phase_order]

        lines.append("### Trace Timing Breakdown")
        lines.append("")
        lines.append("| Phase | Prev Avg | Curr Avg | Delta | Prev p95 | Curr p95 | Delta |")
        lines.append("|:------|:--------:|:--------:|:-----:|:--------:|:--------:|:-----:|")
        # Router-phase spans use a tighter floor; backend-dominated phases
        # (total, dispatch, forward_to_agent, forward_to_backend) reuse the
        # midsize latency floor because inference variance dominates them.
        router_phases = {"queue_wait", "dispatch_policy", "routing_overhead"}
        for phase in ordered:
            label = phase_labels.get(phase, phase)
            p_avg = prev_phases.get(phase, {}).get("avg_ms", 0)
            c_avg = curr_phases.get(phase, {}).get("avg_ms", 0)
            p_p95 = prev_phases.get(phase, {}).get("p95_ms", 0)
            c_p95 = curr_phases.get(phase, {}).get("p95_ms", 0)
            floor = (NOISE_FLOOR["router_phase_ms"]
                     if phase in router_phases
                     else NOISE_FLOOR["midsize_latency_ms"])
            d_avg = (delta_str(p_avg, c_avg, higher_is_better=False, min_abs_delta=floor)
                     if p_avg else "N/A")
            d_p95 = (delta_str(p_p95, c_p95, higher_is_better=False, min_abs_delta=floor)
                     if p_p95 else "N/A")
            lines.append(f"| {label} | {p_avg}ms | {c_avg}ms | {d_avg} | {p_p95}ms | {c_p95}ms | {d_p95} |")
        lines.append("")

    return "\n".join(lines)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <previous.json> <current.json>", file=sys.stderr)
        sys.exit(1)

    prev = load_results(sys.argv[1])
    curr = load_results(sys.argv[2])

    # Dispatch based on benchmark type
    if prev.get("meta", {}).get("type") == "ops" or curr.get("meta", {}).get("type") == "ops":
        print(compare_ops(prev, curr))
        return

    prev_idx = build_index(prev)
    curr_idx = build_index(curr)

    prev_meta = prev.get("meta", {})
    curr_meta = curr.get("meta", {})

    lines = [
        "",
        "---",
        "",
        "## Comparison with Previous Benchmark",
        "",
        f"**Previous:** `{prev_meta.get('commit', '?')[:12]}` ({prev_meta.get('timestamp', '?')})",
        f"**Current:** `{curr_meta.get('commit', '?')[:12]}` ({curr_meta.get('timestamp', '?')})",
        "",
    ]

    # Group by model
    models = sorted(set(k[0] for k in list(prev_idx.keys()) + list(curr_idx.keys())))

    for model in models:
        lines.append(f"### {model}")
        lines.append("")
        lines.append("| Prompt | Max Tokens | Prev tok/s | Curr tok/s | Delta | Prev Latency | Curr Latency | Delta |")
        lines.append("|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|")

        keys = sorted(
            [k for k in set(list(prev_idx.keys()) + list(curr_idx.keys())) if k[0] == model],
            key=lambda k: (k[1], k[2]),
        )

        for key in keys:
            p = prev_idx.get(key)
            c = curr_idx.get(key)
            if not p and not c:
                continue

            prompt = key[1]
            max_tok = key[2]
            prev_tps = p["tokens_per_second"] if p else 0
            curr_tps = c["tokens_per_second"] if c else 0
            prev_lat = p["latency_ms"] if p else 0
            curr_lat = c["latency_ms"] if c else 0

            tps_delta = delta_str(prev_tps, curr_tps, higher_is_better=True) if p and c else "NEW"
            lat_delta = delta_str(prev_lat, curr_lat, higher_is_better=False) if p and c else "NEW"

            prev_tps_s = f"{prev_tps}" if p else "—"
            curr_tps_s = f"{curr_tps}" if c else "—"
            prev_lat_s = f"{prev_lat:,}ms" if p else "—"
            curr_lat_s = f"{curr_lat:,}ms" if c else "—"

            lines.append(
                f"| {prompt:,} | {max_tok:,} | {prev_tps_s} | {curr_tps_s} | {tps_delta} | {prev_lat_s} | {curr_lat_s} | {lat_delta} |"
            )

        # Summary row
        prev_tests = [prev_idx[k] for k in keys if k in prev_idx]
        curr_tests = [curr_idx[k] for k in keys if k in curr_idx]
        if prev_tests and curr_tests:
            prev_peak = max(t["tokens_per_second"] for t in prev_tests)
            curr_peak = max(t["tokens_per_second"] for t in curr_tests)
            prev_avg = sum(t["tokens_per_second"] for t in prev_tests) / len(prev_tests)
            curr_avg = sum(t["tokens_per_second"] for t in curr_tests) / len(curr_tests)
            lines.append("")
            lines.append(
                f"**Peak:** {prev_peak} → {curr_peak} tok/s ({delta_str(prev_peak, curr_peak)}) | "
                f"**Avg:** {prev_avg:.1f} → {curr_avg:.1f} tok/s ({delta_str(prev_avg, curr_avg)})"
            )

        lines.append("")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
