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

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Triggering Kronos refresh on $REALITI_HOST" | tee "$LOG"
ssh -o BatchMode=yes -o ConnectTimeout=30 "$REALITI_HOST" \
    "cd $REMOTE_DIR && ./analysis/sync.sh 2>&1" | tee -a "$LOG"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done" | tee -a "$LOG"
