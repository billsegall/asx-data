#!/usr/bin/env python3
"""
Pull analysis/results/ from a remote GPU machine to local via SSH ControlMaster.
Used when the remote→local direction has a small per-channel data limit (~800 bytes)
that prevents rsync from working for large files.

Usage:
    python3 analysis/pull_results.py --remote user@host --local analysis/results/
"""
import argparse
import base64
import os
import subprocess
import sys
import tempfile
import time

CHUNK_BYTES = 600  # raw bytes per SSH channel (keeps base64 output < 800 bytes)
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=15"]


def ssh_run(ctrl_sock, remote, cmd, timeout=15):
    r = subprocess.run(
        ["ssh", "-S", ctrl_sock, remote, cmd],
        capture_output=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def fetch_file(ctrl_sock, remote, remote_path, local_path, size):
    n_chunks = (size + CHUNK_BYTES - 1) // CHUNK_BYTES
    with open(local_path, "wb") as out:
        for i in range(n_chunks):
            tmp = f"{local_path}.chunk{i}.b64"
            subprocess.run(
                f'ssh -S {ctrl_sock} {remote} '
                f'"dd if={remote_path} bs={CHUNK_BYTES} skip={i} count=1 2>/dev/null | base64 -w0" > {tmp}',
                shell=True, timeout=30, check=True)
            data = base64.b64decode(open(tmp, "rb").read())
            out.write(data)
            os.unlink(tmp)
    return os.path.getsize(local_path)


def main():
    p = argparse.ArgumentParser(description="Pull results from GPU machine via SSH chunks")
    p.add_argument("--remote", required=True, help="SSH address of remote machine")
    p.add_argument("--remote-dir", default="~/code/asx/asx-data/analysis/results",
                   help="Remote results directory")
    p.add_argument("--local-dir", default="analysis/results",
                   help="Local results directory")
    args = p.parse_args()

    ctrl_sock = f"/tmp/ssh_ctrl_{args.remote.replace('@', '_').replace('.', '_')}"
    remote = args.remote

    print(f"==> Connecting to {remote}...")
    kex_opt = "ecdh-sha2-nistp521" if "tailb1cff" in remote or "wsl" in remote else None
    extra = ["-o", f"KexAlgorithms={kex_opt}"] if kex_opt else []
    subprocess.run(
        ["ssh"] + extra + SSH_OPTS + ["-M", "-S", ctrl_sock, "-o", "ControlPersist=120", remote, "true"],
        timeout=20, check=True)

    # List remote files with sizes
    rc, out, _ = ssh_run(ctrl_sock, remote,
        f"find {args.remote_dir} -maxdepth 1 -type f -printf '%s %f\\n'")
    if rc != 0:
        print("Failed to list remote files", file=sys.stderr)
        sys.exit(1)

    files = []
    for line in out.decode().strip().splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            files.append((int(parts[0]), parts[1]))

    if not files:
        print("No files found", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.local_dir, exist_ok=True)
    total_bytes = 0
    t0 = time.time()

    for size, fname in sorted(files):
        remote_path = f"{args.remote_dir}/{fname}"
        local_path = os.path.join(args.local_dir, fname)
        if size <= CHUNK_BYTES:
            # Tiny file: single chunk
            tmp = local_path + ".b64"
            subprocess.run(
                f'ssh -S {ctrl_sock} {remote} "base64 -w0 {remote_path}" > {tmp}',
                shell=True, timeout=15, check=True)
            with open(local_path, "wb") as f:
                f.write(base64.b64decode(open(tmp, "rb").read()))
            os.unlink(tmp)
        else:
            fetch_file(ctrl_sock, remote, remote_path, local_path, size)
        total_bytes += size
        print(f"  {fname} ({size:,} bytes)")

    subprocess.run(["ssh", "-S", ctrl_sock, "-O", "exit", remote],
                   capture_output=True)
    print(f"==> Done: {len(files)} files, {total_bytes:,} bytes in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
