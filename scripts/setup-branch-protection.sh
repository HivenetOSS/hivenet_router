#!/usr/bin/env bash
#
# Applies the branch-protection rule for `main`:
#   - Require 1 approving review, and require review from Code Owners
#     (the senior pool in .github/CODEOWNERS).
#   - Require the CI `test`, `lint`, `govulncheck`, `license-check`, and DCO
#     `sign-off` checks to pass, on an up-to-date branch.
#   - Dismiss stale approvals when new commits are pushed.
#   - Block force-pushes and branch deletion.
#
# Run once (and re-run to update). Requires:
#   - the GitHub CLI `gh` authenticated as a repo ADMIN  (gh auth login)
#
# Usage:
#   ./scripts/setup-branch-protection.sh
#
set -euo pipefail

# Defaults target this repo explicitly (so the rules can't accidentally land on
# whatever repo your shell happens to be in), but both are overridable:
#   REPO=owner/other BRANCH=develop ./scripts/setup-branch-protection.sh
REPO="${REPO:-HivenetOSS/hivenet_router}"
BRANCH="${BRANCH:-main}"

# Required status checks. Each context is a CI *job name* ("test", "lint",
# "sign-off") -- NOT "CI / test"; the "<workflow> / <job>" form is only the
# PR-tab display label, and requiring it would hang merges on a check that never
# reports. "lint" covers gofmt + golangci-lint; "sign-off" is the DCO check
# (.github/workflows/dco.yml) that every commit carry a Signed-off-by trailer.
# NOTE: only require "sign-off" once dco.yml is on ${BRANCH} -- a required check
# that isn't yet on the default branch leaves every PR waiting on a status that
# never reports. To confirm the exact strings after CI has run once on a PR:
#   gh api "repos/${REPO}/commits/${BRANCH}/check-runs" -q '.check_runs[].name'
# "govulncheck" blocks on known vulnerabilities; "license-check" blocks on a
# dependency with an Apache-2.0-incompatible license (.github/workflows/ci.yml).
gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "repos/${REPO}/branches/${BRANCH}/protection" \
  --input - <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["test", "lint", "sign-off", "govulncheck", "license-check"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "require_code_owner_reviews": true,
    "dismiss_stale_reviews": true
  },
  "restrictions": null,
  "required_linear_history": false,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true
}
JSON

echo "Branch protection applied to ${REPO}@${BRANCH}."
