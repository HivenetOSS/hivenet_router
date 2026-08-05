#!/usr/bin/env python3
"""Sustained load test — 2-3 req/s for 5 minutes with varied prompts."""

import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROUTER_URL = os.environ["ROUTER_URL"]
API_KEY = os.environ["LLM_API_KEY"]
MODEL = "openai/gpt-oss-20b"
DURATION_S = 600  # 10 minutes
RPS_MIN, RPS_MAX = 10, 10
MAX_TOKENS = 300

PROMPTS = [
    "What is the capital of France?",
    "Explain quantum computing in one sentence.",
    "What is 42 * 17?",
    "Name three programming languages.",
    "What color is the sky on Mars?",
    "Who wrote Romeo and Juliet?",
    "What is the speed of light?",
    "Describe a black hole briefly.",
    "What is the largest ocean?",
    "How many legs does a spider have?",
    "What year did the moon landing happen?",
    "What is photosynthesis?",
    "Name the smallest planet in our solar system.",
    "What is the boiling point of water in Celsius?",
    "Who painted the Mona Lisa?",
    "What is an algorithm?",
    "How far is the Sun from Earth?",
    "What is DNA?",
    "Name a famous mathematician.",
    "What causes thunder?",
]

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


def send_request(prompt):
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS,
    }).encode()
    req = Request(f"{ROUTER_URL}/v1/chat/completions", data=body, headers=HEADERS, method="POST")
    start = time.time()
    try:
        with urlopen(req, timeout=60) as r:
            r.read()
            return {"status": r.status, "latency_ms": int((time.time() - start) * 1000), "ok": True}
    except HTTPError as e:
        e.read()
        return {"status": e.code, "latency_ms": int((time.time() - start) * 1000), "ok": False}
    except Exception as e:
        return {"status": 0, "latency_ms": int((time.time() - start) * 1000), "ok": False, "error": str(e)}


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0
    k = max(0, int(len(sorted_vals) * p / 100) - 1)
    return sorted_vals[k]


def main():
    print(f"Sustained load test — {RPS_MIN}-{RPS_MAX} req/s for {DURATION_S}s", file=sys.stderr)
    print(f"Target: {ROUTER_URL} | Model: {MODEL}", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    results = []
    start_time = time.time()
    total_sent = 0
    executor = ThreadPoolExecutor(max_workers=50)
    futures = []

    while time.time() - start_time < DURATION_S:
        loop_start = time.time()
        # pick how many requests this second (2 or 3)
        n = random.randint(RPS_MIN, RPS_MAX)
        for _ in range(n):
            prompt = random.choice(PROMPTS)
            futures.append(executor.submit(send_request, prompt))
            total_sent += 1

        # collect any completed futures
        done = [f for f in futures if f.done()]
        for f in done:
            results.append(f.result())
            futures.remove(f)

        elapsed = time.time() - start_time
        ok_so_far = sum(1 for r in results if r["ok"])
        if int(elapsed) % 30 == 0 and int(elapsed) > 0:
            print(f"  [{int(elapsed)}s] sent={total_sent} completed={len(results)} ok={ok_so_far}", file=sys.stderr)

        # sleep to maintain ~1 second pacing
        sleep_time = max(0, 1.0 - (time.time() - loop_start))
        if sleep_time > 0:
            time.sleep(sleep_time)

    # drain remaining futures
    print(f"  Draining {len(futures)} in-flight requests...", file=sys.stderr)
    for f in as_completed(futures):
        results.append(f.result())

    # compute stats
    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    latencies = sorted(r["latency_ms"] for r in results if r["ok"])
    all_latencies = sorted(r["latency_ms"] for r in results)
    wall_time = time.time() - start_time

    # error breakdown
    error_codes = {}
    for r in results:
        if not r["ok"]:
            code = r.get("status", 0)
            error_codes[code] = error_codes.get(code, 0) + 1

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"## Sustained Load Test Results\n", file=sys.stderr)
    print(f"| Metric | Value |", file=sys.stderr)
    print(f"|:-------|------:|", file=sys.stderr)
    print(f"| Duration | {wall_time:.1f}s |", file=sys.stderr)
    print(f"| Total Sent | {len(results)} |", file=sys.stderr)
    print(f"| Success | {ok_count} ({ok_count/len(results)*100:.1f}%) |", file=sys.stderr)
    print(f"| Failed | {fail_count} |", file=sys.stderr)
    print(f"| Avg RPS | {len(results)/wall_time:.2f} |", file=sys.stderr)
    if latencies:
        print(f"| p50 Latency | {percentile(latencies, 50)}ms |", file=sys.stderr)
        print(f"| p95 Latency | {percentile(latencies, 95)}ms |", file=sys.stderr)
        print(f"| p99 Latency | {percentile(latencies, 99)}ms |", file=sys.stderr)
        print(f"| Min Latency | {latencies[0]}ms |", file=sys.stderr)
        print(f"| Max Latency | {latencies[-1]}ms |", file=sys.stderr)

    if error_codes:
        print(f"\n### Error Breakdown\n", file=sys.stderr)
        print(f"| Status | Count |", file=sys.stderr)
        print(f"|:-------|------:|", file=sys.stderr)
        for code, count in sorted(error_codes.items()):
            print(f"| {code} | {count} |", file=sys.stderr)

    # JSON output
    print(json.dumps({
        "duration_s": round(wall_time, 1),
        "total_requests": len(results),
        "success_count": ok_count,
        "fail_count": fail_count,
        "success_rate": round(ok_count / len(results), 3),
        "avg_rps": round(len(results) / wall_time, 2),
        "latency_p50_ms": percentile(latencies, 50),
        "latency_p95_ms": percentile(latencies, 95),
        "latency_p99_ms": percentile(latencies, 99),
        "latency_min_ms": latencies[0] if latencies else 0,
        "latency_max_ms": latencies[-1] if latencies else 0,
        "error_codes": error_codes,
    }, indent=2))


if __name__ == "__main__":
    main()
