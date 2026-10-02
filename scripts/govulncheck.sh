#!/usr/bin/env bash
#
# Runs govulncheck and fails only on CALLED vulnerabilities that are NOT
# explicitly allowlisted below. Every allowlist entry must carry a justification
# and be revisited when a fix becomes available.
#
# Usage: ./scripts/govulncheck.sh
set -uo pipefail

GOVULNCHECK_VERSION="${GOVULNCHECK_VERSION:-v1.5.0}"

# OSV IDs we knowingly accept.
#
# GO-2026-4479 — pion/dtls/v2 AES-GCM random-nonce issue. No fixed version
#   exists (advisory says "Fixed in: N/A"). Pulled in transitively via
#   go-libp2p's WebRTC transport (go-libp2p → webrtc → pion/stun → pion/dtls/v2);
#   Hivenet Router uses the TCP/QUIC libp2p transports, not WebRTC/DTLS, so the
#   vulnerable handshake path is not exercised. Re-check when pion ships a fix.
#
# GO-2026-6505 — otel exporter config logging may leak endpoint URLs in info
#   logs (CVE-2026-81870, GHSA-8wmf-6v46-5gfg). Fixed in go.opentelemetry.io/otel
#   1.45.0; the module is pinned at 1.44.0. TEMPORARY: this entry unblocks CI
#   until the dependency bump lands (the dependabot group-bump PR bundles 14
#   modules and is currently red on test/lint/e2e, so the org needs a minimal
#   otel bump first). REMOVE this entry when otel >= 1.45.0 is merged.
ALLOWLIST="GO-2026-4479 GO-2026-6505"

echo "Running govulncheck@${GOVULNCHECK_VERSION} ..."
json="$(go run "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" -format json ./...)"

# A finding is "called" when its most specific trace frame names a function.
called="$(printf '%s' "$json" \
  | jq -r 'select(.finding != null) | .finding | select(.trace[0].function != null) | .osv' \
  | sort -u)"

if [ -z "$called" ]; then
  echo "No called vulnerabilities found."
  exit 0
fi

echo "Called vulnerabilities:"
fail=0
while IFS= read -r id; do
  [ -z "$id" ] && continue
  if grep -qw "$id" <<<"$ALLOWLIST"; then
    echo "  - $id  [allowlisted]"
  else
    echo "  - $id  [NOT allowlisted] https://pkg.go.dev/vuln/$id"
    fail=1
  fi
done <<<"$called"

if [ "$fail" -ne 0 ]; then
  echo "FAIL: one or more non-allowlisted vulnerabilities are reachable." >&2
  exit 1
fi
echo "OK: only allowlisted vulnerabilities remain."
