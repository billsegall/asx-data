#!/bin/bash
set -e
cd /home/bill/code/asx/asx-data
git fetch origin
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/master)
if [ "$LOCAL" != "$REMOTE" ]; then
    git pull --ff-only origin master
    sudo systemctl restart asx-backend
    echo "$(date): deployed $REMOTE"
fi
