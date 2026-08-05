#!/usr/bin/env sh
# Collect the license texts of every third-party dependency statically linked
# into the given Go packages (e.g. ./cmd/router) and write them to $OUT.
#
# Go links dependencies into the final binary, so their license texts do not
# otherwise appear in the shipped image. Apache-2.0 and the MIT/BSD licenses of
# our dependencies require those texts to travel with the binary distribution;
# this script produces the bundle the Dockerfiles copy into the runtime image.
#
# Usage: OUT=/licenses scripts/collect-licenses.sh ./cmd/router
set -eu

OUT="${OUT:-/licenses}"
GO_LICENSES_VERSION="${GO_LICENSES_VERSION:-v1.6.0}"

[ "$#" -gt 0 ] || { echo "usage: OUT=<dir> $0 <pkg>..." >&2; exit 1; }

go install "github.com/google/go-licenses@${GO_LICENSES_VERSION}"
GO_LICENSES="$(go env GOPATH)/bin/go-licenses"

# go-base36 ships a dual Apache-2.0 / MIT "Permissive License Stack" that
# go-licenses cannot classify, so we skip it here and copy it in by hand.
"$GO_LICENSES" save "$@" \
    --save_path="$OUT" \
    --ignore hivenet_router \
    --ignore github.com/multiformats/go-base36

# Resolve the exact version in the build graph so we copy the license that
# matches the linked dependency, not whatever version happens to sit in the
# module cache.
b36ver="$(go list -m -f '{{.Version}}' github.com/multiformats/go-base36 2>/dev/null || true)"
if [ -n "${b36ver:-}" ]; then
    b36="$(go env GOMODCACHE)/github.com/multiformats/go-base36@${b36ver}"
    if [ -f "$b36/LICENSE.md" ]; then
        mkdir -p "$OUT/github.com/multiformats/go-base36"
        cp "$b36/LICENSE.md" "$OUT/github.com/multiformats/go-base36/LICENSE.md"
    fi
fi

echo "Collected $(find "$OUT" -type f | wc -l) license files into $OUT"
