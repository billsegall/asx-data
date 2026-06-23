#!/bin/bash
# Trigger Kronos predictions + backtest on the GPU analysis machine.
# Called from harri crontab; SSHes to GPU host and runs sync.sh there.
#
# Requires in asx-data/.env:
#   REALITI_HOST=user@hostname   (SSH target for GPU machine)
#   REMOTE_DIR=~/code/asx/asx-data  (optional, default shown)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

[[ -f "$REPO_ROOT/.env" ]] && source "$REPO_ROOT/.env"

: "${REALITI_HOST:?'REALITI_HOST not set in asx-data/.env'}"
REMOTE_DIR="${REMOTE_DIR:-~/code/asx/asx-data}"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/kronos_$(date +%Y%m%d_%H%M%S).log"

SSH_KEY="${REALITI_SSH_KEY:-$HOME/.ssh/id_ed25519_VMs}"

REMOTE_LOG="/tmp/kronos_sync_$(date +%Y%m%d_%H%M%S).log"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Triggering Kronos refresh on $REALITI_HOST" | tee "$LOG"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Remote log: $REALITI_HOST:$REMOTE_LOG" | tee -a "$LOG"

# Launch sync.sh detached — nohup + </dev/null so SSH exits immediately.
# WSL network drops the SSH session if we stay attached for the full run.
ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=30 \
    -o ServerAliveInterval=10 -o ServerAliveCountMax=3 "$REALITI_HOST" \
    "cd $REMOTE_DIR && nohup bash analysis/sync.sh > '$REMOTE_LOG' 2>&1 </dev/null &"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Job launched — waiting 6 minutes for completion..." | tee -a "$LOG"
sleep 360

# Fetch the remote log regardless of exit status
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Fetching remote log..." | tee -a "$LOG"
ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=30 \
    -o ServerAliveInterval=10 -o ServerAliveCountMax=3 "$REALITI_HOST" \
    "cat '$REMOTE_LOG' 2>/dev/null || echo '(remote log not found)'" | tee -a "$LOG"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done" | tee -a "$LOG"
