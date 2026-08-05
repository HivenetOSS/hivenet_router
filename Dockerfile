# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Download dependencies first (cached layer unless go.mod/go.sum change)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build the router binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /hivenet-router ./cmd/router/

# Collect the license texts of all statically-linked dependencies so they
# travel with the image (Go links deps into the binary — see the script).
RUN apk --no-cache add git && OUT=/licenses ./scripts/collect-licenses.sh ./cmd/router

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM alpine:3.21

# ca-certificates required for TLS connections (gRPC, libp2p)
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /hivenet-router .

# Third-party dependency licenses (Apache-2.0/MIT/BSD attribution for the
# statically-linked deps). The project's own LICENSE is Apache-2.0.
COPY --from=builder /licenses /licenses

# Public-facing ports:
#   8888  — HTTP API (OpenAI-compatible, client requests)
#   8902  — gRPC auth (agent authentication)
#   8903  — libp2p P2P (agent registration, heartbeats, inference)
#   2112  — Prometheus metrics (internal, scraped by Prometheus)
EXPOSE 8888 8902 8903 2112

ENTRYPOINT ["/app/hivenet-router"]
