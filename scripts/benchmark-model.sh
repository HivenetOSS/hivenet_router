#!/usr/bin/env bash
# benchmark-model.sh — Raw LLM model benchmark (bypasses router/agent)
#
# Hits vLLM directly via kubectl exec to measure pure model performance
# without network overhead, router queuing, or libp2p transport.
#
# Usage:
#   ./scripts/benchmark-model.sh
#   ./scripts/benchmark-model.sh -n hivenet-llm-dev -o results.json -m summary.md
#
# Environment variables:
#   NAMESPACE        — Kubernetes namespace (default: hivenet-llm-dev)
#   COMMIT_SHA       — git commit SHA for metadata

set -euo pipefail

NAMESPACE="${NAMESPACE:-hivenet-llm-dev}"
COMMIT_SHA="${COMMIT_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
JSON_OUT=""
MD_OUT=""
TIMEOUT=120

while getopts "n:o:m:c:t:" opt; do
  case $opt in
    n) NAMESPACE="$OPTARG" ;;
    o) JSON_OUT="$OPTARG" ;;
    m) MD_OUT="$OPTARG" ;;
    c) COMMIT_SHA="$OPTARG" ;;
    t) TIMEOUT="$OPTARG" ;;
    *) echo "Usage: $0 [-n namespace] [-o results.json] [-m summary.md] [-c commit] [-t timeout]" >&2; exit 1 ;;
  esac
done

# ── Discover vLLM pods ───────────────────────────────────────────────────────
echo "Discovering vLLM pods in namespace $NAMESPACE..." >&2
PODS=$(kubectl get pods -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -v router)

if [[ -z "$PODS" ]]; then
  echo "Error: no vLLM pods found in namespace $NAMESPACE" >&2
  exit 1
fi

# ── Test matrix ──────────────────────────────────────────────────────────────
# prompt_target_tokens:max_completion_tokens
TEST_CASES=(
  "100:50"
  "100:200"
  "100:500"
  "100:1000"
  "500:500"
  "1000:500"
  "2000:500"
  "5000:500"
  "10000:500"
  "20000:500"
)

BASE_PROMPT="Explain in great detail the history, technical foundations, trade-offs, and real-world applications of distributed consensus algorithms including Raft, Paxos, PBFT, and their modern variants. Cover the CAP theorem, FLP impossibility result, and how systems like etcd, ZooKeeper, and CockroachDB implement these. "

# ── Run tests ────────────────────────────────────────────────────────────────
RESULTS=$(python3 -c "
import subprocess, json, time, sys

namespace = '$NAMESPACE'
commit = '$COMMIT_SHA'
timeout = int('$TIMEOUT')
base_prompt = '$BASE_PROMPT'

pods_raw = '''$PODS'''.strip().split('\n')
test_cases = [tuple(map(int, tc.split(':'))) for tc in '''$(IFS=$'\n'; echo "${TEST_CASES[*]}")'''.strip().split('\n')]

def make_prompt(target_tokens):
    target_chars = target_tokens * 4
    repeats = max(1, target_chars // len(base_prompt))
    return (base_prompt * repeats)[:target_chars]

def get_model(pod):
    \"\"\"Discover the model name from vLLM's /v1/models endpoint.\"\"\"
    result = subprocess.run(
        ['kubectl', 'exec', '-n', namespace, pod, '-c', 'vllm', '--',
         'curl', '-sf', '--max-time', '5', 'http://localhost:8000/v1/models'],
        capture_output=True, text=True
    )
    try:
        data = json.loads(result.stdout)
        return data['data'][0]['id']
    except:
        return None

def run_test(pod, model, prompt_size, max_tokens):
    prompt = make_prompt(prompt_size)
    payload = json.dumps({
        'model': model,
        'messages': [{'role': 'user', 'content': prompt}],
        'max_tokens': max_tokens
    })
    # Use kubectl exec to hit vLLM directly on localhost:8000
    start = time.time()
    result = subprocess.run(
        ['kubectl', 'exec', '-n', namespace, pod, '-c', 'vllm', '--',
         'curl', '-s', '--max-time', str(timeout),
         '-X', 'POST', 'http://localhost:8000/v1/chat/completions',
         '-H', 'Content-Type: application/json',
         '-d', payload],
        capture_output=True, text=True, timeout=timeout + 30
    )
    elapsed_ms = int((time.time() - start) * 1000)
    try:
        resp = json.loads(result.stdout)
        if 'error' in resp:
            return {'status': 'error', 'error': resp['error'].get('message', str(resp['error'])),
                    'model': model, 'pod': pod, 'target_prompt_tokens': prompt_size,
                    'max_tokens': max_tokens, 'latency_ms': elapsed_ms}
        u = resp['usage']
        comp = u['completion_tokens']
        tps = comp / (elapsed_ms / 1000.0) if elapsed_ms > 0 and comp > 0 else 0
        # Estimate TTFT: total latency minus (completion_tokens / steady_state_rate)
        # Steady state ≈ last portion of generation
        return {
            'status': 'ok', 'model': model, 'pod': pod,
            'target_prompt_tokens': prompt_size,
            'prompt_tokens': u['prompt_tokens'],
            'completion_tokens': comp,
            'total_tokens': u['total_tokens'],
            'max_tokens': max_tokens,
            'latency_ms': elapsed_ms,
            'tokens_per_second': round(tps, 1),
            'finish_reason': resp['choices'][0]['finish_reason'] if resp.get('choices') else 'unknown'
        }
    except Exception as e:
        return {'status': 'error', 'error': str(e), 'model': model, 'pod': pod,
                'target_prompt_tokens': prompt_size, 'max_tokens': max_tokens,
                'latency_ms': elapsed_ms}

all_results = []
for pod in pods_raw:
    pod = pod.strip()
    if not pod:
        continue
    model = get_model(pod)
    if not model:
        print(f'  Skipping {pod} — cannot discover model', file=sys.stderr)
        continue
    print(f'  Testing {model} on {pod} (direct vLLM)...', file=sys.stderr)
    model_results = []
    for prompt_size, max_tok in test_cases:
        print(f'    prompt~{prompt_size} max_tokens={max_tok}...', end='', file=sys.stderr, flush=True)
        r = None
        for attempt in range(3):
            r = run_test(pod, model, prompt_size, max_tok)
            if r['status'] == 'ok':
                break
            print(f' retry {attempt+1}/3...', end='', file=sys.stderr, flush=True)
            time.sleep(5)
        model_results.append(r)
        if r['status'] == 'ok':
            print(f' {r[\"latency_ms\"]}ms {r[\"tokens_per_second\"]} tok/s', file=sys.stderr)
        else:
            print(f' ERROR: {r[\"error\"][:60]}', file=sys.stderr)
    all_results.append({'model': model, 'pod': pod, 'tests': model_results})

output = {
    'meta': {
        'commit': commit,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'namespace': namespace,
        'type': 'model',
        'test_count': sum(len(m['tests']) for m in all_results),
        'pass_count': sum(1 for m in all_results for t in m['tests'] if t['status'] == 'ok'),
        'fail_count': sum(1 for m in all_results for t in m['tests'] if t['status'] != 'ok'),
    },
    'models': all_results
}
print(json.dumps(output, indent=2))
")

# ── Write JSON output ────────────────────────────────────────────────────────
if [[ -n "$JSON_OUT" ]]; then
  echo "$RESULTS" > "$JSON_OUT"
  echo "JSON results written to $JSON_OUT" >&2
else
  echo "$RESULTS"
fi

# ── Generate Markdown summary ────────────────────────────────────────────────
MD=$(echo "$RESULTS" | python3 -c "
import json, sys

data = json.load(sys.stdin)
meta = data['meta']

lines = []
lines.append(f'## Model Benchmark Results (Direct vLLM)')
lines.append(f'')
lines.append(f'**Commit:** \`{meta[\"commit\"]}\` | **Date:** {meta[\"timestamp\"]} | **Tests:** {meta[\"pass_count\"]}/{meta[\"test_count\"]} passed')
lines.append(f'')

for model_data in data['models']:
    model = model_data['model']
    pod = model_data.get('pod', '?')
    tests = [t for t in model_data['tests'] if t['status'] == 'ok']
    if not tests:
        lines.append(f'### {model} ({pod}) — all tests failed')
        continue

    peak = max(tests, key=lambda t: t['tokens_per_second'])
    largest_prompt = max(tests, key=lambda t: t.get('prompt_tokens', 0))

    lines.append(f'### {model}')
    lines.append(f'**Pod:** \`{pod}\` | **Peak:** {peak[\"tokens_per_second\"]} tok/s | **At max prompt:** {largest_prompt[\"tokens_per_second\"]} tok/s')
    lines.append(f'')
    lines.append(f'| Prompt | Completion | Total | Latency | Throughput |')
    lines.append(f'|:-:|:-:|:-:|:-:|:-:|')
    for t in tests:
        lines.append(f'| {t[\"prompt_tokens\"]:,} | {t[\"completion_tokens\"]:,} | {t[\"total_tokens\"]:,} | {t[\"latency_ms\"]:,}ms | {t[\"tokens_per_second\"]} tok/s |')
    lines.append(f'')

print('\n'.join(lines))
")

if [[ -n "$MD_OUT" ]]; then
  echo "$MD" > "$MD_OUT"
  echo "Markdown summary written to $MD_OUT" >&2
else
  echo "$MD" >&2
fi

echo "Model benchmark complete" >&2
