#!/usr/bin/env bash
# setup-router-host.sh — one-shot VM setup for the Hivenet Router
#
# Installs everything a CPU-only VM needs to run the router container:
#   1. Docker + Compose plugin
#   2. Firewall rules for published router ports
#
# Usage:
#   chmod +x scripts/setup-router-host.sh
#   sudo ./scripts/setup-router-host.sh
#
# Supported: Ubuntu 20.04 / 22.04 / 24.04 and Debian 11 / 12

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[setup]${NC} $*"; }
step()    { echo -e "\n${CYAN}══ $* ${NC}"; }
error()   { echo -e "${RED}[setup] ERROR:${NC} $*" >&2; exit 1; }

# ── Root check ────────────────────────────────────────────────────────────────
[[ $EUID -eq 0 ]] || error "Please run as root: sudo ./scripts/setup-router-host.sh"

INVOKE_USER="${SUDO_USER:-$USER}"

# ── OS check ──────────────────────────────────────────────────────────────────
. /etc/os-release
[[ "$ID" == "ubuntu" || "$ID" == "debian" ]] \
    || error "This script supports Ubuntu and Debian only. Got: $ID"

# ── Initial package index refresh + base tools ───────────────────────────────
apt-get update -q
apt-get install -y ca-certificates curl gnupg

# ─────────────────────────────────────────────────────────────────────────────
step "1/3  Docker + Compose plugin"
# ─────────────────────────────────────────────────────────────────────────────
if command -v docker &>/dev/null && docker compose version &>/dev/null 2>&1; then
    info "Docker already installed: $(docker --version)"
    info "Docker Compose already installed: $(docker compose version)"
else
    info "Installing Docker..."
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL "https://download.docker.com/linux/${ID}/gpg" \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
        | tee /etc/apt/sources.list.d/docker.list > /dev/null

    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    systemctl enable --now docker
    info "Docker installed: $(docker --version)"
    info "Docker Compose installed: $(docker compose version)"
fi

# Add invoking user to the docker group so they don't need sudo for compose
DOCKER_GROUP_ADDED=false
if ! groups "$INVOKE_USER" | grep -q '\bdocker\b'; then
    usermod -aG docker "$INVOKE_USER"
    DOCKER_GROUP_ADDED=true
    info "Added '$INVOKE_USER' to the docker group. Changes take effect on next login."
else
    info "User '$INVOKE_USER' is already in the docker group."
fi

# ─────────────────────────────────────────────────────────────────────────────
step "2/3  Firewall — router published ports"
# ─────────────────────────────────────────────────────────────────────────────
# The router exposes (see docker-compose.yml):
#   8888  — HTTP API (clients send POST /v1/chat/completions)
#   8902  — gRPC auth (agents connect here to authenticate)
#   8903  — libp2p (agent registration, heartbeats, inference forwarding)
#   3000  — Grafana dashboard UI
#
# Internal-only ports (NOT published, scraped via Docker network):
#   2112  — Prometheus metrics (router, scraped by prometheus container)
#   9090  — Prometheus query API (default, commented out in docker-compose.yml)
#   3100  — Loki log ingestion
#   9080  — Promtail HTTP API
#
# If a firewall is detected, rules are added for the published ports.
# For production, restrict 8902/8903 to your agent subnets and access
# Grafana (:3000) via SSH tunnel instead of opening it publicly.
if command -v ufw &>/dev/null && ufw status 2>/dev/null | grep -q "Status: active"; then
    for port_proto in "8888/tcp" "8902/tcp" "8903/tcp" "3000/tcp"; do
        if ufw status | grep -qE "${port_proto}.*ALLOW"; then
            info "Port ${port_proto} already open in ufw."
        else
            ufw allow "$port_proto"
            info "Opened ${port_proto} in ufw."
        fi
    done
elif command -v iptables &>/dev/null; then
    for port in 8888 8902 8903 3000; do
        if iptables -C INPUT -p tcp --dport "$port" -j ACCEPT &>/dev/null 2>&1; then
            info "Port ${port}/tcp already open in iptables."
        else
            iptables -A INPUT -p tcp --dport "$port" -j ACCEPT
            info "Opened ${port}/tcp in iptables."
        fi
    done
    warn "iptables rules are not persistent. Install 'iptables-persistent' or use ufw."
else
    warn "No firewall detected (ufw/iptables)."
    warn "Open the following ports in your cloud provider's security group:"
    warn "  8888/tcp — HTTP API (clients)"
    warn "  8902/tcp — gRPC auth (agents)"
    warn "  8903/tcp — libp2p (agents + inference)"
    warn "  3000/tcp — Grafana dashboard (or access via SSH tunnel)"
fi
warn "Also restrict 8902/tcp and 8903/tcp to your agent subnets in production."

# ─────────────────────────────────────────────────────────────────────────────
step "3/3  Verify Docker"
# ─────────────────────────────────────────────────────────────────────────────
docker run --rm hello-world &>/dev/null \
    || warn "hello-world container failed — Docker may need a moment to fully start."

# ─────────────────────────────────────────────────────────────────────────────
# Done — print launch instructions
# ─────────────────────────────────────────────────────────────────────────────
PUBLIC_IP=$(curl -fsSL --connect-timeout 5 --max-time 10 https://ifconfig.me 2>/dev/null || echo "<YOUR_PUBLIC_IP>")

echo ""
echo -e "${GREEN}══ Setup complete ════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Public IP detected: ${CYAN}${PUBLIC_IP}${NC}"
echo ""
echo "  Start the router:"
echo ""
echo -e "  ${CYAN}cd /path/to/hivenet_router"
echo "  docker compose up --build -d${NC}"
echo ""
echo "  Verify:"
echo -e "  ${CYAN}docker compose logs -f router${NC}"
echo ""
echo "  The router will be reachable at:"
echo -e "  - HTTP API:    http://${PUBLIC_IP}:8888"
echo -e "  - gRPC auth:   ${PUBLIC_IP}:8902"
echo -e "  - libp2p:      ${PUBLIC_IP}:8903"
echo -e "  - Grafana:     http://${PUBLIC_IP}:3000"
echo ""
if [[ "$DOCKER_GROUP_ADDED" == "true" ]]; then
    warn "Log out and back in (or run 'newgrp docker') for the docker group to take effect."
fi
echo -e "${GREEN}══════════════════════════════════════════════════════════════════${NC}"
