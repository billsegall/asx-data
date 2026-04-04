#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Recalculate volume brackets
echo "Recalculating volume brackets..."
python3 scripts/recalculate_volume_brackets.py

# Restart the backend service
echo "Restarting backend service..."
sudo systemctl restart asx-backend

echo "Volume brackets updated and backend restarted."
