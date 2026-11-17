#!/bin/bash
# Run sync.sh on the GPU machine, logging to a file, then copy that log to
# harri. Launched detached by trigger_kronos.sh, which waits for the log to
# arrive instead of fetching it.
#
# Why push rather than pull: realiti's WSL eth0 has MTU 1280, the same as the
# Tailscale tunnel, so full-size tunnel packets from realiti are dropped and
# harri's ssh reads from realiti stall once more than ~1KB comes back.
# realiti reaches harri ($ASX_SERVER) over the LAN, which isn't affected.
#
# Usage: run_and_report.sh <local log path> <destination path on harri>

LOG="$1"
DEST="$2"
: "${LOG:?usage: run_and_report.sh LOG DEST}" "${DEST:?usage: run_and_report.sh LOG DEST}"

cd "$(dirname "$0")/.."

bash analysis/sync.sh > "$LOG" 2>&1
echo "[run_and_report] sync.sh exit status $?" >> "$LOG"

source .env
# rsync writes to a temp file and renames, so harri never sees a partial log
rsync -q -e "ssh -o BatchMode=yes -o ConnectTimeout=30" "$LOG" "$ASX_SERVER:$DEST"
