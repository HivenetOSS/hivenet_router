#!/usr/bin/env bash
#
# preflight.sh — run every CI check locally before committing/pushing.
#
# Mirrors .github/workflows/ci.yml (jobs: test, smoke, lint, govulncheck,
# license-check) so a green run here means a green run in CI. Runs ALL checks
# even if an earlier one fails, then prints a summary and exits non-zero if any
# failed. DCO sign-off (dco.yml) is a per-commit concern, not checked here.
#
# Usage: ./scripts/preflight.sh
#
# Requirements: go, curl, python3, jq; golangci-lint optional (skipped with a
# warning if not installed — CI still enforces it).

set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# go-nvml (agent) and the -race detector require cgo, exactly as CI sets it.
export CGO_ENABLED="${CGO_ENABLED:-1}"

BOLD=$'\033[1m'; RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; NC=$'\033[0m'
declare -a RESULTS=()
FAILED=0

# run "<name>" <command...> — runs a check, records PASS/FAIL/SKIP, keeps going.
run() {
    local name="$1"; shift
    echo "${BOLD}==> ${name}${NC}"
    if "$@"; then
        RESULTS+=("${GREEN}PASS${NC}  ${name}")
    else
        RESULTS+=("${RED}FAIL${NC}  ${name}")
        FAILED=1
    fi
    echo
}

skip() { RESULTS+=("${YELLOW}SKIP${NC}  $1"); echo "${YELLOW}==> SKIP${NC} $1"; echo; }

# ── lint: gofmt (exact CI logic) ──────────────────────────────────────────────
check_gofmt() {
    local unformatted; unformatted="$(gofmt -l .)"
    if [ -n "$unformatted" ]; then
        echo "${RED}Not gofmt-formatted (run 'gofmt -w .'):${NC}"; echo "$unformatted"; return 1
    fi
}

# ── build / vet / test ────────────────────────────────────────────────────────
run "build            (go build ./...)"          go build ./...
run "vet              (go vet ./...)"            go vet ./...
run "gofmt            (gofmt -l .)"              check_gofmt

# ── golangci-lint (optional locally; CI pins v2.12.2) ─────────────────────────
if command -v golangci-lint >/dev/null 2>&1; then
    run "golangci-lint    (golangci-lint run)"   golangci-lint run
else
    skip "golangci-lint    (not installed — CI enforces v2.12.2)"
fi

run "test -race       (go test ./... -race)"     go test ./... -race -count=1
run "e2e smoke        (scripts/e2e-smoke.sh)"    ./scripts/e2e-smoke.sh
run "govulncheck      (scripts/govulncheck.sh)"  ./scripts/govulncheck.sh
run "license-check    (scripts/check-licenses.sh)" ./scripts/check-licenses.sh

# ── summary ───────────────────────────────────────────────────────────────────
echo "${BOLD}──────── preflight summary ────────${NC}"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo

if [ "$FAILED" -ne 0 ]; then
    echo "${RED}${BOLD}PREFLIGHT FAILED${NC} — fix the above before committing." >&2
    exit 1
fi
echo "${GREEN}${BOLD}PREFLIGHT OK${NC} — all CI checks pass locally. Safe to commit."
echo "Reminder: commit with DCO sign-off (git commit -s), author must match Signed-off-by."
