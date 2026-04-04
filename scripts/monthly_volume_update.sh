#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Recalculate volume brackets
echo "Recalculating volume brackets..."
python3 scripts/recalculate_volume_brackets.py

# Signal the backend to reload config
echo "Signaling backend to reload volume config..."
systemctl status asx-backend >/dev/null 2>&1 && sudo systemctl reload asx-backend || echo "Backend not running"

echo "Volume brackets updated."
