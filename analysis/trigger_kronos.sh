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

STAMP=$(date +%Y%m%d_%H%M%S)
REMOTE_LOG="/tmp/kronos_sync_$STAMP.log"
# realiti copies its log here when the job finishes (see run_and_report.sh)
ARRIVED_LOG="$LOG_DIR/kronos_sync_$STAMP.log"
WAIT_LIMIT=1800     # seconds; the job usually takes 5-8 minutes

source "$SCRIPT_DIR/wake_realiti.sh"
if ! ensure_realiti_up | tee -a "$LOG"; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Aborting — realiti unreachable" | tee -a "$LOG"
    exit 1
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Triggering Kronos refresh on $REALITI_HOST" | tee -a "$LOG"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Remote log: $REALITI_HOST:$REMOTE_LOG" | tee -a "$LOG"

# Launch sync.sh detached. `-f` tells the SSH CLIENT ITSELF to background
# after auth (requires stdin from /dev/null, hence the redirect on the
# remote command) -- this is the textbook-correct way to launch a detached
# remote job. A trailing `&` on just the remote command string does NOT
# reliably do this: even with nohup + all three fds redirected away from
# the remote job, the outer ssh session can still block waiting for the
# channel to close for reasons that don't depend on the backgrounded job's
# own fds at all. Confirmed empirically against production: `... &` alone
# hung indefinitely on a trivial 120s remote sleep (reproduced live, no
# outer 60s wrapper timeout arriving before a full return); `ssh -f` on the
# identical command returned in 0.08s. This is very likely what's actually
# been behind the growing multi-minute-to-indefinite delay between
# "Triggering" and "Job launched" in recent kronos_trigger.log entries --
# not the WSL network itself, despite the comment this replaces assuming so.
ssh -f -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=30 \
    -o ServerAliveInterval=10 -o ServerAliveCountMax=3 "$REALITI_HOST" \
    "cd $REMOTE_DIR && nohup bash analysis/run_and_report.sh '$REMOTE_LOG' '$ARRIVED_LOG' > /dev/null 2>&1 </dev/null"

# Wait for realiti to push its log, rather than pulling it: reads from
# realiti over Tailscale stall past ~1KB (MTU problem, see run_and_report.sh),
# which made every pull here fail even though the job itself had finished.
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Job launched — waiting up to $((WAIT_LIMIT / 60)) minutes for its log..." | tee -a "$LOG"
waited=0
while [[ ! -f "$ARRIVED_LOG" ]] && (( waited < WAIT_LIMIT )); do
    sleep 30
    waited=$((waited + 30))
done
if [[ -f "$ARRIVED_LOG" ]]; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Log arrived after ${waited}s:" | tee -a "$LOG"
    tee -a "$LOG" < "$ARRIVED_LOG"
    if grep -q "sync.sh exit status [1-9]" "$ARRIVED_LOG"; then
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] sync.sh FAILED on realiti — see log above" | tee -a "$LOG" >&2
    fi
else
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] No log from realiti after $((WAIT_LIMIT / 60)) minutes — the job may still be running or have died. Its log on realiti: $REMOTE_LOG" | tee -a "$LOG" >&2
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done" | tee -a "$LOG"
