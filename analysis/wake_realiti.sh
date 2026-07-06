#!/bin/bash
# Ensure realiti (GPU dev machine) is reachable over SSH before running a
# remote job on it, waking it via Wake-on-LAN magic packet if it's down.
#
# realiti runs as a WSL2 guest inside a Windows host; when the Windows host
# sleeps/reboots, the WSL guest goes down with it and comes back up ~automatically
# on boot. WoL targets the Windows host's physical LAN NIC, not the WSL guest
# (which has no real MAC of its own) — reachability is confirmed via SSH to the
# Tailscale hostname once the host (and then WSL) has booted.
#
# Usage: source this file, then call ensure_realiti_up
#
# Requires in asx-data/.env (or exported):
#   REALITI_HOST=user@hostname
# Optional overrides:
#   REALITI_MAC=74:56:3c:ba:8a:c8   (harri LAN MAC for realiti's Windows host;
#                                    found via `ip neigh` matching the Tailscale
#                                    "direct" endpoint IP for realiti-wsl)
#   REALITI_BROADCAST=10.0.1.255    (harri's LAN broadcast address)
#   REALITI_SSH_KEY=~/.ssh/id_ed25519_VMs
#   REALITI_WAKE_TIMEOUT=300        (seconds to wait for realiti to come up)

REALITI_MAC="${REALITI_MAC:-74:56:3c:ba:8a:c8}"
REALITI_BROADCAST="${REALITI_BROADCAST:-10.0.1.255}"
REALITI_SSH_KEY="${REALITI_SSH_KEY:-$HOME/.ssh/id_ed25519_VMs}"
REALITI_WAKE_TIMEOUT="${REALITI_WAKE_TIMEOUT:-300}"

_realiti_ssh_probe() {
    ssh -i "$REALITI_SSH_KEY" -o BatchMode=yes -o ConnectTimeout=8 \
        -o StrictHostKeyChecking=accept-new "$REALITI_HOST" "true" 2>/dev/null
}

# Confirms realiti is up over SSH, sending a WoL magic packet and polling
# if it's not. Returns 0 once reachable, 1 if it never comes up within
# REALITI_WAKE_TIMEOUT seconds.
ensure_realiti_up() {
    : "${REALITI_HOST:?REALITI_HOST not set}"

    if _realiti_ssh_probe; then
        echo "[wake_realiti] $REALITI_HOST already up"
        return 0
    fi

    echo "[wake_realiti] $REALITI_HOST not reachable — sending magic packet to $REALITI_MAC via $REALITI_BROADCAST"
    wakeonlan -i "$REALITI_BROADCAST" "$REALITI_MAC"

    local waited=0
    local interval=15
    while (( waited < REALITI_WAKE_TIMEOUT )); do
        sleep "$interval"
        waited=$(( waited + interval ))
        if _realiti_ssh_probe; then
            echo "[wake_realiti] $REALITI_HOST up after ${waited}s"
            return 0
        fi
        echo "[wake_realiti] still down after ${waited}s..."
    done

    echo "[wake_realiti] ERROR: $REALITI_HOST did not come up within ${REALITI_WAKE_TIMEOUT}s" >&2
    return 1
}
