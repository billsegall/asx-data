#!/bin/bash
# Sync DB from harri, run analysis locally (GPU), push results back.
# Usage: ./analysis/sync.sh [--skip-pull] [--skip-push]

set -e
HARRI=bill@harri.segall.net
REMOTE_BASE=~/code/asx-data
LOCAL_DB=stockdb/stockdb.db
RESULTS_DIR=analysis/results

SKIP_PULL=0
SKIP_PUSH=0
for arg in "$@"; do
    [[ "$arg" == "--skip-pull" ]] && SKIP_PULL=1
    [[ "$arg" == "--skip-push" ]] && SKIP_PUSH=1
done

cd "$(dirname "$0")/.."

if [[ $SKIP_PULL -eq 0 ]]; then
    echo "==> Pulling stockdb.db from harri..."
    rsync -avz --progress "$HARRI:$REMOTE_BASE/stockdb/stockdb.db" "$LOCAL_DB"
else
    echo "==> Skipping pull (--skip-pull)"
fi

echo ""
echo "==> Running predictions (GPU)..."
python3 -m analysis.cli.run_predictions --db "$LOCAL_DB" --output-dir "$RESULTS_DIR"

if [[ $SKIP_PUSH -eq 0 ]]; then
    echo ""
    echo "==> Pushing results to harri..."
    rsync -avz "$RESULTS_DIR/" "$HARRI:$REMOTE_BASE/analysis/results/"
    echo "==> Done. Results live at /api/analysis/signals etc."
else
    echo "==> Skipping push (--skip-push)"
fi
