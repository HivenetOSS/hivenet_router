#!/usr/bin/env bash
# setup-agent-host.sh — full one-shot VM setup for the Hivenet Agent
#
# Installs and configures everything a GPU VM needs to run the agent container:
#   1. Docker + Compose plugin
#   2. NVIDIA Container Toolkit (GPU passthrough into Docker)
#   3. nvidia-cuda-toolkit (nvcc + CUDA headers for host-side compilation)
#   4. python3.12-dev (CPython headers required by native Python extensions)
#
# The agent makes outbound-only connections to the router (gRPC 50051 and
# libp2p 9000); it needs no inbound ports and no firewall ingress rules.
#
# Usage:
#   chmod +x scripts/setup-agent-host.sh
#   sudo ./scripts/setup-agent-host.sh
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
[[ $EUID -eq 0 ]] || error "Please run as root: sudo ./scripts/setup-agent-host.sh"

# Keep track of the user who invoked sudo (for group membership)
INVOKE_USER="${SUDO_USER:-$USER}"

# ── OS check ──────────────────────────────────────────────────────────────────
. /etc/os-release
[[ "$ID" == "ubuntu" || "$ID" == "debian" ]] \
    || error "This script supports Ubuntu and Debian only. Got: $ID"

# ── Pre-flight: broken package check ─────────────────────────────────────────
if dpkg --audit 2>/dev/null | grep -q .; then
    # If nvidia-smi works the driver is already functional — the broken packages
    # are likely a failed DKMS *rebuild* of pre-compiled modules. Warn and continue.
    # Otherwise hard-fail so apt-get install doesn't abort mid-script.
    if command -v nvidia-smi &>/dev/null && nvidia-smi &>/dev/null 2>&1; then
        warn "dpkg has broken packages (nvidia-dkms build failure) but nvidia-smi works."
        warn "The pre-compiled driver modules are loaded and functional — continuing."
        warn "To clean up the dpkg state run:"
        warn "  sudo dpkg --remove --force-remove-reinstreq nvidia-dkms-590"
        warn "  sudo dpkg --remove --force-remove-reinstreq nvidia-driver-590"
    else
        error "dpkg reports broken or unconfigured packages.\n       Fix with:\n         sudo dpkg --configure -a\n         sudo apt-get install -f\n       Then re-run this script."
    fi
fi

# ── Initial package index refresh + base tools ───────────────────────────────
# Run once here so that prerequisite packages can be found on fresh/lean images
# before any repo is added. curl and gnupg are needed to add the Docker and
# NVIDIA apt repositories.
apt-get update -q
apt-get install -y ca-certificates curl gnupg

# ─────────────────────────────────────────────────────────────────────────────
step "1/4  NVIDIA driver"
# ─────────────────────────────────────────────────────────────────────────────
if ! command -v nvidia-smi &>/dev/null; then
    error "nvidia-smi not found. Install the NVIDIA driver for your GPU before running this script.\n       See: https://docs.nvidia.com/datacenter/tesla/tesla-installation-notes/"
fi
info "NVIDIA driver found:"
nvidia-smi --query-gpu=name,driver_version --format=csv,noheader | while IFS=',' read -r name ver; do
    info "  GPU: $(echo "$name" | xargs)  |  driver: $(echo "$ver" | xargs)"
done

# ─────────────────────────────────────────────────────────────────────────────
step "2/4  Docker + Compose plugin"
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
step "3/4  NVIDIA Container Toolkit"
# ─────────────────────────────────────────────────────────────────────────────
if docker info 2>/dev/null | grep -q "Runtimes:.*nvidia"; then
    info "NVIDIA Container Toolkit already configured — skipping."
else
    info "Installing NVIDIA Container Toolkit..."
    curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
        | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

    curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
        | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
        | tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

    apt-get update
    apt-get install -y nvidia-container-toolkit

    info "Configuring Docker runtime..."
    nvidia-ctk runtime configure --runtime=docker
    systemctl restart docker

    info "Verifying GPU visibility inside Docker..."
    docker run --rm --gpus all nvidia/cuda:12.6.0-base-ubuntu24.04 nvidia-smi &>/dev/null \
        || error "GPU not visible inside Docker after toolkit install. Check 'docker run --rm --gpus all nvidia/cuda:12.6.0-base-ubuntu24.04 nvidia-smi' manually."
    info "GPU is visible inside Docker containers."
fi

# No firewall step: the agent only makes outbound connections to the router
# (gRPC 50051, libp2p 9000) — inference requests come back over the libp2p
# connection the agent opened, so no inbound ports need to be opened.

# ─────────────────────────────────────────────────────────────────────────────
step "4/4  Dev packages (nvidia-cuda-toolkit, python3.12-dev)"
# ─────────────────────────────────────────────────────────────────────────────
# These packages are required when the inference engine (vLLM, llama.cpp) needs
# to compile native extensions on the host (e.g. flash-attention, bitsandbytes).
# If you are running a pre-built Docker image that bundles all extensions, you
# can skip this step — it will not affect the agent itself.
PKGS_TO_INSTALL=()

if dpkg -l nvidia-cuda-toolkit 2>/dev/null | grep -q '^ii'; then
    info "nvidia-cuda-toolkit already installed — skipping."
else
    PKGS_TO_INSTALL+=(nvidia-cuda-toolkit)
fi

if dpkg -l python3.12-dev 2>/dev/null | grep -q '^ii'; then
    info "python3.12-dev already installed — skipping."
else
    PKGS_TO_INSTALL+=(python3.12-dev)
fi

if [[ ${#PKGS_TO_INSTALL[@]} -gt 0 ]]; then
    info "Installing: ${PKGS_TO_INSTALL[*]}"
    apt-get install -y "${PKGS_TO_INSTALL[@]}"
    info "Installed: ${PKGS_TO_INSTALL[*]}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Done — print launch instructions
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}══ Setup complete ════════════════════════════════════════════════${NC}"
echo ""
echo "  Start the agent (outbound-only — no inbound ports required):"
echo ""
echo -e "  ${CYAN}cd /path/to/hivenet_router"
echo "  ROUTER_GRPC=<ROUTER_IP>:50051 \\"
echo "    ROUTER_P2P=/ip4/<ROUTER_IP>/tcp/9000 \\"
echo "    HIVENET_ROUTER_JWT_SECRET=<same-secret-as-router> \\"
echo "    AGENT_REGION=EU-Primary \\"
echo "    docker compose -f docker-compose.agent.yml up --build -d${NC}"
echo ""
echo "  Verify the connection:"
echo -e "  ${CYAN}docker compose -f docker-compose.agent.yml logs -f agent${NC}"
echo ""
if [[ "$DOCKER_GROUP_ADDED" == "true" ]]; then
    warn "Log out and back in (or run 'newgrp docker') for the docker group to take effect."
fi
echo -e "${GREEN}══════════════════════════════════════════════════════════════════${NC}"
