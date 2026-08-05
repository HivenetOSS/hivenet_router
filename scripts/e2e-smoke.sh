#!/usr/bin/env bash
# e2e-smoke.sh — build the real router + agent binaries, wire them to a stub
# OpenAI backend, and drive one inference request end-to-end.
#
#   ./scripts/e2e-smoke.sh
#
# What it proves in ~30 seconds:
#   - the binaries build and start with their documented flags
#   - an agent with ZERO network configuration (no inbound ports, no public
#     IP) registers and serves inference — the router forwards requests back
#     over the agent's own libp2p connection
#   - non-streaming and streaming completions flow through the full stack
#
# No GPU or model needed: the "backend" is a tiny Python stub that answers
# every chat completion with "PONG". This tests the plumbing, not the model.
#
# Requirements: go, curl, python3. Exits 0 on PASS, 1 on FAIL.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
JWT_SECRET="smoke-test-secret-0123456789abcdef" # ≥32 bytes required

RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; NC=$'\033[0m'
fail() { echo "${RED}FAIL${NC}: $*"; echo "--- router.log (tail) ---"; tail -20 "$WORK/router.log" 2>/dev/null || true; echo "--- agent.log (tail) ---"; tail -20 "$WORK/agent.log" 2>/dev/null || true; exit 1; }
step() { echo "==> $*"; }

cleanup() {
    # Preserve the real exit status (PASS/FAIL) — cleanup must never change it.
    local status=$?
    kill "${AGENT_PID:-}" "${ROUTER_PID:-}" "${BACKEND_PID:-}" 2>/dev/null || true
    # Wait for the children to actually exit before removing $WORK. Otherwise
    # BadgerDB is still flushing into $WORK/badger while `rm -rf` runs, the dir
    # gets repopulated mid-delete, and rm fails with "Directory not empty".
    wait "${AGENT_PID:-}" "${ROUTER_PID:-}" "${BACKEND_PID:-}" 2>/dev/null || true
    rm -rf "$WORK" 2>/dev/null || true
    exit "$status"
}
trap cleanup EXIT

# Pick all ports in one process, holding every socket open until the last is
# bound — sequential bind/close calls can hand back the same port twice.
read -r HTTP_PORT GRPC_PORT P2P_PORT METRICS_PORT BACKEND_PORT < <(python3 -c '
import socket
socks = [socket.socket() for _ in range(5)]
for s in socks: s.bind(("127.0.0.1", 0))
print(*(s.getsockname()[1] for s in socks))
for s in socks: s.close()')

# ── Build ────────────────────────────────────────────────────────────────────
step "Building router and agent binaries"
(cd "$ROOT" && go build -o "$WORK/router" ./cmd/router && go build -o "$WORK/agent" ./cmd/agent) 2>/dev/null \
    || fail "go build failed (rerun 'go build ./...' manually to see errors)"

# ── Stub backend ─────────────────────────────────────────────────────────────
step "Starting stub OpenAI backend on :$BACKEND_PORT"
python3 - "$BACKEND_PORT" > "$WORK/backend.log" 2>&1 <<'PYEOF' &
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

class Stub(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        stream = json.loads(body or b"{}").get("stream", False)
        if stream:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream"); self.end_headers()
            # Flush each chunk so clients see incremental SSE frames, as a
            # real streaming backend would deliver them.
            for tok in "PONG":
                chunk = {"id": "chatcmpl-stub", "object": "chat.completion.chunk",
                         "created": 1, "model": "stub-model",
                         "choices": [{"index": 0, "delta": {"content": tok}, "finish_reason": None}]}
                self.wfile.write(b"data: " + json.dumps(chunk).encode() + b"\n\n")
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        else:
            resp = {"id": "chatcmpl-stub", "object": "chat.completion", "created": 1,
                    "model": "stub-model",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "PONG"},
                                 "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}
            out = json.dumps(resp).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out))); self.end_headers()
            self.wfile.write(out)

HTTPServer(("127.0.0.1", int(sys.argv[1])), Stub).serve_forever()
PYEOF
BACKEND_PID=$!

# ── Router ───────────────────────────────────────────────────────────────────
step "Starting router (http :$HTTP_PORT, grpc :$GRPC_PORT, p2p :$P2P_PORT)"
# This smoke test drives the inference path, not admin auth, so allow the
# default no-auth admin surface instead of configuring admin keys.
HIVENET_ROUTER_JWT_SECRET="$JWT_SECRET" HIVENET_ROUTER_ALLOW_INSECURE_ADMIN=true "$WORK/router" \
    --http-port ":$HTTP_PORT" --grpc-port ":$GRPC_PORT" \
    --p2p-port "$P2P_PORT" --p2p-listen-addr 127.0.0.1 \
    --metrics-port ":$METRICS_PORT" \
    --disk-db-path "$WORK/badger" > "$WORK/router.log" 2>&1 &
ROUTER_PID=$!

for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$HTTP_PORT/v1/models" > /dev/null 2>&1 && break
    kill -0 "$ROUTER_PID" 2>/dev/null || fail "router exited during startup"
    sleep 0.2
done
curl -sf "http://127.0.0.1:$HTTP_PORT/v1/models" > /dev/null || fail "router HTTP API never became ready"

# ── Agent ────────────────────────────────────────────────────────────────────
# Note what is NOT here: no --p2p-announce-addr, no --p2p-listen-port, no
# port mapping. Agents are outbound-only.
step "Starting agent (no address configuration — outbound only)"
HIVENET_ROUTER_JWT_SECRET="$JWT_SECRET" "$WORK/agent" \
    --engine custom --model stub-model \
    --backend-url "http://127.0.0.1:$BACKEND_PORT" \
    --health-url "http://127.0.0.1:$BACKEND_PORT/health" \
    --router-grpc "127.0.0.1:$GRPC_PORT" \
    --router-p2p "/ip4/127.0.0.1/tcp/$P2P_PORT" \
    --identity-path "$WORK/agent_identity.key" \
    --capacity 2 --region smoke > "$WORK/agent.log" 2>&1 &
AGENT_PID=$!

step "Waiting for agent registration"
for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$HTTP_PORT/v1/models" 2>/dev/null | grep -q '"healthy":1' && break
    kill -0 "$AGENT_PID" 2>/dev/null || fail "agent exited during startup"
    sleep 0.3
done
curl -sf "http://127.0.0.1:$HTTP_PORT/v1/models" | grep -q '"healthy":1' || fail "agent never registered as healthy"

# ── Drive ────────────────────────────────────────────────────────────────────
step "Sending chat completion through the router"
RESPONSE=$(curl -sf -m 30 "http://127.0.0.1:$HTTP_PORT/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d '{"model":"stub-model","messages":[{"role":"user","content":"ping"}],"max_tokens":10}') \
    || fail "completion request failed"
echo "$RESPONSE" | grep -q '"PONG"' || fail "unexpected completion response: $RESPONSE"
echo "    response: $RESPONSE"

step "Sending streaming completion"
STREAM=$(curl -sf -N -m 30 "http://127.0.0.1:$HTTP_PORT/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d '{"model":"stub-model","messages":[{"role":"user","content":"ping"}],"max_tokens":10,"stream":true}') \
    || fail "streaming request failed"
echo "$STREAM" | grep -q "^data: " || fail "no SSE chunks in streaming response"
echo "$STREAM" | grep -q "\[DONE\]" || fail "stream ended without [DONE]"

echo ""
echo "${GREEN}PASS${NC}: full stack works — auth, registration (no agent addresses), forward, streaming."
