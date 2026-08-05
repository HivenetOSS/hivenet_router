#!/usr/bin/env sh
# Fails if any dependency statically linked into the binaries carries a license
# that is incompatible with redistributing Hivenet Router under Apache 2.0.
#
# We ship static Go binaries, so every dependency's license travels with them
# (see scripts/collect-licenses.sh). This gate keeps a future contributor from
# pulling in a GPL/LGPL/AGPL — or otherwise unrecognised — dependency.
#
# Blocked:  forbidden (AGPL, ...), restricted (GPL/LGPL family), unknown.
# Allowed:  notice/permissive (Apache-2.0, MIT, BSD, ISC) and reciprocal (MPL-2.0,
#           which we only link, not modify).
#
# Usage: scripts/check-licenses.sh
set -eu

GO_LICENSES_VERSION="${GO_LICENSES_VERSION:-v1.6.0}"

go install "github.com/google/go-licenses@${GO_LICENSES_VERSION}"
GO_LICENSES="$(go env GOPATH)/bin/go-licenses"

# go-base36 ships a dual Apache-2.0 / MIT "Permissive License Stack" that
# go-licenses classifies as "unknown"; it is compatible, so it is whitelisted.
"$GO_LICENSES" check ./cmd/router ./cmd/agent \
    --disallowed_types=forbidden,restricted,unknown \
    --ignore hivenet_router \
    --ignore github.com/multiformats/go-base36

echo "Dependency licenses are Apache-2.0-compatible. ✅"
