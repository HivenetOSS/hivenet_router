# Contributing to Hivenet Router

Thanks for your interest in improving Hivenet Router! This guide covers how to build,
test, and submit changes. By participating you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report bugs** — open an issue with clear reproduction steps, the version/commit,
  and relevant router/agent logs.
- **Request features** — open an issue describing the use case before writing code,
  so we can agree on the approach.
- **Improve docs** — documentation lives under [`docs/`](docs/); fixes and clarifications
  are always welcome.
- **Send pull requests** — see below.

## Development setup

Hivenet Router is a single Go module. You need **Go 1.25+**.

```bash
git clone https://github.com/HivenetOSS/hivenet_router.git
cd hivenet_router
go build ./...          # build everything
go build -o bin/hivenet-router ./cmd/router/
go build -o bin/hivenet-agent  ./cmd/agent/
```

## Running the tests

```bash
go test ./...                                   # full suite
go test -count=1 ./...                          # force a fresh (uncached) run
go test -coverpkg=./internal/...,./proto ./...  # with coverage
```

Tests live under [`test/`](test/) as black-box packages (`package <pkg>_test`) that
import `hivenet_router/internal/...`. When adding a feature, add tests alongside it and
prefer table-driven cases with a short doc comment explaining the scenario.

## Code style

- Run `gofmt -w` (or `go fmt ./...`), `go vet ./...`, and `golangci-lint run` before
  committing — the same checks run in CI and must pass. Lint config lives in `.golangci.yml`.
- CI also runs `govulncheck ./...` to catch known vulnerabilities in dependencies; you can
  run it locally with `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`.
- Match the surrounding code: naming, comment density, and idioms.
- Keep changes focused; unrelated refactors belong in a separate PR.

### Regenerating protobuf

The gRPC/protobuf Go files in `proto/` are generated — **edit `proto/auth.proto`, not
the `*.pb.go` files**, then regenerate (requires `protoc`, `protoc-gen-go`,
`protoc-gen-go-grpc`):

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/auth.proto
```

## Pull request process

1. Create a topic branch off `main` (`feat/…`, `fix/…`, `docs/…`). External contributors
   should fork first; maintainers can branch directly in the repo.
2. Make your change with tests; ensure `go build ./...`, `go vet ./...`,
   `golangci-lint run`, and `go test ./...` all pass locally.
   Sign off every commit (`git commit -s`) — see [Sign-off (DCO)](#sign-off-dco) below.
3. Open the PR and fill in the description template that appears — what changed, why,
   and how you verified it.
4. Keep the branch up to date with `main` and respond to review feedback.

Before a PR can be merged, GitHub enforces two gates automatically:

- **CI must be green** — the `test` job (build, vet, `go test ./... -race`) has to pass.
- **Review required** — at least one approval from the maintainer pool defined in
  [`.github/CODEOWNERS`](.github/CODEOWNERS). New commits dismiss stale approvals.

## Releases

Images are published to Docker Hub as `<namespace>/router` and `<namespace>/agent`:

- **Nightly** — every merge into `main` rebuilds and pushes `:nightly` and
  `:nightly-<shortsha>`.
- **Stable** — a maintainer publishes a GitHub Release with a `vX.Y.Z` tag, which builds
  and pushes `:vX.Y.Z`, `:vX.Y`, and `:latest`.

## Reporting security issues

**Do not open a public issue for security vulnerabilities.** See [`SECURITY.md`](SECURITY.md)
— email the maintainers at hivenetrouter@antimatter.com with details and we will coordinate a fix
and disclosure.

## Sign-off (DCO)

Instead of a CLA, Hivenet Router uses the Developer Certificate of Origin (DCO) — see
the [`DCO`](DCO) file. It is a lightweight statement that you wrote the
contribution, or otherwise have the right to submit it under the project's license. You certify it by adding
a `Signed-off-by` line to each commit — do this automatically with the `-s` flag:

```bash
git commit -s -m "fix: correct heartbeat backoff"
```

This appends a trailer using your `git` name and email, which must be real:

```
Signed-off-by: Jane Doe <jane@example.com>
```

Every commit in a pull request must be signed off; a DCO check runs in CI and
blocks the merge if any commit is missing the trailer. To fix an existing branch:

```bash
git rebase --signoff main      # sign off all commits on the branch
git push --force-with-lease
```

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE), the same license as the project. New source files
should carry the standard two-line SPDX header used throughout the tree:

```go
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA
```

This is enforced by `golangci-lint` (the `goheader` linter), so a new `.go`
file without it fails the `lint` check. Generated files (e.g. `*.pb.go`) are
exempt automatically.
