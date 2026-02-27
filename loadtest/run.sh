#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Load .env defaults
set -a
# shellcheck disable=SC1091
source "$SCRIPT_DIR/.env"
set +a

VM_NAME="${VM_NAME:-obs-loadtest}"
VM_CPUS="${VM_CPUS:-2}"
VM_MEMORY="${VM_MEMORY:-4G}"
VM_DISK="${VM_DISK:-20G}"

# Extra args forwarded to load_test.py
LOADTEST_ARGS="${*:-}"

# Reports are written to a VM-local path (not the mounted dir) to avoid
# SSHFS mount write issues on macOS.
VM_REPORT_DIR="/tmp/loadtest-reports"

# ── Helpers ──────────────────────────────────────────────────────────────

vm_exec() {
    multipass exec "$VM_NAME" -- bash -c "$1"
}

vm_running() {
    multipass info "$VM_NAME" 2>/dev/null | grep -q "State.*Running"
}

cleanup() {
    echo -e "\n${YELLOW}[cleanup] Stopping docker compose inside VM...${NC}"
    vm_exec "cd /home/ubuntu/project/loadtest && docker compose down -v" 2>/dev/null || true

    echo -e "${YELLOW}Delete the VM '$VM_NAME'? [y/N] (N = stop VM but keep for reuse)${NC}"
    read -r answer
    if [[ "$answer" =~ ^[Yy]$ ]]; then
        multipass delete "$VM_NAME" && multipass purge
        echo -e "${GREEN}VM deleted. All resources freed.${NC}"
    else
        echo -e "${YELLOW}Stopping VM (frees CPU/RAM but keeps disk for faster re-runs)...${NC}"
        multipass stop "$VM_NAME" 2>/dev/null || true
        echo -e "${GREEN}VM stopped. Re-run ./run.sh to reuse it, or delete with:${NC}"
        echo -e "${GREEN}  multipass delete $VM_NAME && multipass purge${NC}"
    fi
}
trap cleanup EXIT

# ── Step 1: Check prerequisites ──────────────────────────────────────────

echo -e "${GREEN}[1/8] Checking prerequisites...${NC}"
if ! command -v multipass &>/dev/null; then
    echo -e "${RED}multipass not found. Install it:${NC}"
    echo "  macOS:  brew install multipass"
    echo "  Linux:  sudo snap install multipass"
    exit 1
fi

# ── Step 2: Launch VM ────────────────────────────────────────────────────

echo -e "${GREEN}[2/8] Launching Multipass VM '${VM_NAME}' (${VM_CPUS} CPUs, ${VM_MEMORY} RAM, ${VM_DISK} disk)...${NC}"
if multipass info "$VM_NAME" &>/dev/null; then
    echo "  VM '$VM_NAME' already exists."
    if ! vm_running; then
        echo "  Starting stopped VM..."
        multipass start "$VM_NAME"
    fi
else
    multipass launch \
        --name "$VM_NAME" \
        --cpus "$VM_CPUS" \
        --memory "$VM_MEMORY" \
        --disk "$VM_DISK" \
        --cloud-init "$SCRIPT_DIR/cloud-init.yml" \
        22.04
    echo "  Waiting for cloud-init to finish..."
    multipass exec "$VM_NAME" -- cloud-init status --wait
fi

# Verify docker is available
echo "  Verifying Docker inside VM..."
MAX_RETRIES=30
RETRY=0
until vm_exec "docker info >/dev/null 2>&1"; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo -e "${RED}Docker not available in VM after ${MAX_RETRIES} retries${NC}"
        exit 1
    fi
    sleep 2
done
echo -e "${GREEN}  Docker is ready inside VM.${NC}"

# ── Step 3: Copy project into VM ─────────────────────────────────────────

echo -e "${GREEN}[3/8] Copying project into VM...${NC}"
# Mount the project root into the VM (read-only is fine for source code)
multipass mount "$PROJECT_ROOT" "$VM_NAME":/home/ubuntu/project 2>/dev/null || {
    # If mount fails (e.g. not supported), fall back to transfer
    echo "  Mount failed, using file transfer instead..."
    vm_exec "mkdir -p /home/ubuntu/project"
    multipass transfer "$PROJECT_ROOT/Dockerfile" "$VM_NAME":/home/ubuntu/project/Dockerfile
    multipass transfer "$PROJECT_ROOT/go.mod" "$VM_NAME":/home/ubuntu/project/go.mod
    multipass transfer "$PROJECT_ROOT/go.sum" "$VM_NAME":/home/ubuntu/project/go.sum
    (cd "$PROJECT_ROOT" && tar czf - cmd internal modules db loadtest) | \
        multipass exec "$VM_NAME" -- bash -c "cd /home/ubuntu/project && tar xzf -"
}
echo -e "${GREEN}  Project synced to VM.${NC}"

# ── Step 4: Build backend image inside VM ─────────────────────────────────

echo -e "${GREEN}[4/8] Building backend Docker image inside VM...${NC}"
vm_exec "cd /home/ubuntu/project && docker build -t observability-backend:loadtest -f Dockerfile ."

# ── Step 5: Start infrastructure inside VM ────────────────────────────────

echo -e "${GREEN}[5/8] Starting infrastructure (docker compose up) inside VM...${NC}"
vm_exec "cd /home/ubuntu/project/loadtest && docker compose up -d --build --wait --wait-timeout 180"

# ── Step 6: Wait for backend ──────────────────────────────────────────────

echo -e "${GREEN}[6/8] Waiting for backend to be healthy inside VM...${NC}"
MAX_RETRIES=60
RETRY=0
until vm_exec "curl -sf http://localhost:8080/api/v1/status >/dev/null 2>&1"; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo -e "${RED}Backend not healthy after ${MAX_RETRIES} retries. Logs:${NC}"
        vm_exec "cd /home/ubuntu/project/loadtest && docker compose logs backend --tail 50"
        exit 1
    fi
    echo "  Waiting... ($RETRY/$MAX_RETRIES)"
    sleep 2
done
echo -e "${GREEN}  Backend is healthy!${NC}"

# ── Step 7: Run load test inside VM ──────────────────────────────────────

echo -e "${GREEN}[7/8] Setting up Python venv and running load test inside VM...${NC}"
vm_exec "
    mkdir -p $VM_REPORT_DIR
    cd /home/ubuntu/project/loadtest
    python3 -m venv /tmp/loadtest-venv 2>/dev/null || true
    . /tmp/loadtest-venv/bin/activate
    pip install -q requests
    python3 load_test.py --backend http://localhost:8080 --report-dir $VM_REPORT_DIR $LOADTEST_ARGS
"

# ── Step 8: Collect report ────────────────────────────────────────────────

echo -e "${GREEN}[8/8] Collecting report from VM...${NC}"
mkdir -p "$SCRIPT_DIR/reports"

REMOTE_REPORT=$(vm_exec "ls -t ${VM_REPORT_DIR}/report_*.json 2>/dev/null | head -1" || true)
if [ -n "$REMOTE_REPORT" ]; then
    REPORT_NAME=$(basename "$REMOTE_REPORT")
    multipass transfer "$VM_NAME":"$REMOTE_REPORT" "$SCRIPT_DIR/reports/$REPORT_NAME"
    FILE_SIZE=$(wc -c < "$SCRIPT_DIR/reports/$REPORT_NAME" | tr -d ' ')
    if [ "$FILE_SIZE" -gt 0 ]; then
        echo -e "${GREEN}Report saved to: $SCRIPT_DIR/reports/$REPORT_NAME ($FILE_SIZE bytes)${NC}"
        echo ""
        echo "View it with:  cat $SCRIPT_DIR/reports/$REPORT_NAME | python3 -m json.tool"
    else
        echo -e "${YELLOW}Report file transferred but is empty. Dumping from VM:${NC}"
        vm_exec "cat ${REMOTE_REPORT}"
    fi
else
    echo -e "${YELLOW}No report file found in VM.${NC}"
fi

# cleanup runs via trap EXIT
