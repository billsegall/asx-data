#!/bin/bash
# Sync DB from remote server, run analysis locally (GPU), push results back.
# Usage: ./analysis/sync.sh [--skip-pull] [--skip-push]
#
# Required env var (or set in .env):
#   ASX_SERVER=user@your-server  (e.g. bill@192.168.1.10)

set -e

if [[ -f "$(dirname "$0")/../.env" ]]; then
    # shellcheck disable=SC1091
    source "$(dirname "$0")/../.env"
fi

HARRI=${ASX_SERVER:?'ASX_SERVER env var not set (e.g. user@your-server)'}
REMOTE_BASE=~/code/asx/asx-data
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
    echo "==> Pulling stockdb.db from $HARRI..."
    rsync -avz --progress "$HARRI:$REMOTE_BASE/stockdb/stockdb.db" "$LOCAL_DB"
else
    echo "==> Skipping pull (--skip-pull)"
fi

echo ""
echo "==> Running predictions (GPU)..."
python3 -m analysis.cli.run_predictions --db "$LOCAL_DB" --output-dir "$RESULTS_DIR"

echo ""
echo "==> Running correlation analysis (GPU)..."
python3 -m analysis.cli.run_correlations \
    --db "$LOCAL_DB" \
    --output-dir "$RESULTS_DIR" \
    --max-lag 20 --min-r 0.15 --market-adjust

echo ""
echo "==> Running per-industry correlation analysis (GPU)..."
python3 -m analysis.cli.run_industry_correlations \
    --db "$LOCAL_DB" \
    --output-db "$RESULTS_DIR/correlations.db" \
    --max-lag 20 --min-r 0.15 --market-adjust \
    --min-symbols 5

echo ""
echo "==> Running EOFY tax-loss/gain correlation analysis..."
python3 -m analysis.cli.run_eofy_correlation \
    --db "$LOCAL_DB" \
    --output-dir "$RESULTS_DIR" \
    --min-years 5

echo ""
echo "==> Running EOFY sub-window correlation analysis..."
python3 -m analysis.cli.run_eofy_window_compare \
    --db "$LOCAL_DB" \
    --eofy-db "$RESULTS_DIR/eofy_correlation.db" \
    --min-years 5

echo ""
echo "==> Running warrant analysis..."
python3 -m analysis.cli.run_warrants \
    --db "$LOCAL_DB" \
    --output-dir "$RESULTS_DIR"

echo ""
echo "==> Running Kronos backtest..."
python3 -m analysis.cli.run_kronos_backtest \
    --db "$LOCAL_DB" \
    --output-dir "$RESULTS_DIR"

if [[ $SKIP_PUSH -eq 0 ]]; then
    echo ""
    # Failures here must NOT be fatal to the script (no bare `set -e`-tripping
    # commands). History of this section, two separate bugs found and fixed:
    #
    # 1) (2026-06 - 2026-09-18) Used pull_results.py's 600-byte dd+base64-
    #    per-SSH-round-trip chunking, with $HARRI pulling FROM realiti. A
    #    single large unrelated file (eofy_correlation.db, ~16MB) failed to
    #    transfer that way three nights running, and its non-zero exit
    #    killed the whole script under `set -e`, silently skipping the
    #    Kronos import below even though predictions_kronos.json (tiny, and
    #    everything Kronos actually needs) had already landed.
    #
    # 2) (2026-09-19 - 2026-09-25) "Fixed" #1 by switching to plain rsync,
    #    but kept the same $HARRI-pulls-FROM-realiti direction -- which
    #    means THIS script (already running on realiti) was asking $HARRI to
    #    open a SECOND, nested connection back to realiti to pull from.
    #    That double-hop reproducibly stalled/errored ("rsync error: error
    #    in rsync protocol data stream", "Broken pipe") every single night,
    #    a 100% regression that silently broke Kronos for five consecutive
    #    trading days before being caught -- because the non-fatal error
    #    handling from fix #1 correctly kept the script alive, so nothing
    #    ever looked "broken" in the log, it just quietly kept re-importing
    #    yesterday's already-in-DB predictions and reporting nothing new.
    #
    # Fix: push directly, single-hop, FROM realiti (where this script
    # already runs) TO $HARRI -- no nested/looped-back connection at all.
    # Verified directly against production: 21MB, full results dir, ~1s.
    echo "==> Pushing results to $HARRI..."
    if ! rsync -avz "$RESULTS_DIR/" "$HARRI:$REMOTE_BASE/analysis/results/"; then
        echo "WARNING: push to $HARRI failed -- some result files may be stale. Kronos import (below) still attempted independently." >&2
    fi
    echo "==> Importing Kronos predictions to history DB on $HARRI..."
    if ! ssh "$HARRI" "cd $REMOTE_BASE && test -f analysis/results/predictions_kronos.json && python3 -m analysis.cli.import_kronos_predictions \
        --db stockdb/stockdb.db \
        --json analysis/results/predictions_kronos.json"; then
        echo "WARNING: Kronos import failed, or predictions_kronos.json is missing/stale on $HARRI." >&2
    fi
    echo "==> Done. Results live at /api/analysis/signals etc."
else
    echo "==> Skipping push (--skip-push)"
fi
