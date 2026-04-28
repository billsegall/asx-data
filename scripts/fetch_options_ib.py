#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_options_ib.py — Refresh asx_options metadata from IB Gateway warrants.

Reads option symbols from stockdb asx_options (the authoritative list),
queries IB once per underlying, matches by localSymbol, and updates
expiry / strike / share_name.

Usage:
  python3 fetch_options_ib.py [--db PATH] [--host HOST] [--port PORT]

Env vars:
  STOCKDB  (default: ../../asx-data/stockdb/stockdb.db relative to this script)
"""

import argparse
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

DEFAULT_DB = Path(__file__).parent.parent / 'stockdb' / 'stockdb.db'
IB_HOST    = '127.0.0.1'
IB_PORT    = 4001
CLIENT_ID  = 56


def option_to_underlying(sym: str) -> str | None:
    m = re.match(r'^(.+)O[A-Z]$', sym)
    if m:
        return m.group(1)
    m = re.match(r'^(.+)O$', sym)
    if m:
        return m.group(1)
    return None


def fetch_warrants(ib, underlying: str) -> list:
    from ib_insync import Contract
    c = Contract(symbol=underlying, secType='WAR', exchange='ASX', currency='AUD')
    try:
        return ib.reqContractDetails(c)
    except Exception as e:
        print(f"  WARN IB error for {underlying}: {e}", file=sys.stderr)
        return []


def format_expiry(raw: str) -> str:
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',        default=os.environ.get('STOCKDB', str(DEFAULT_DB)))
    parser.add_argument('--host',      default=IB_HOST)
    parser.add_argument('--port',      type=int, default=IB_PORT)
    parser.add_argument('--client-id', type=int, default=CLIENT_ID)
    args = parser.parse_args()

    try:
        from ib_insync import IB
    except ImportError:
        sys.exit("ERROR: ib_insync not installed. pip install ib_insync")

    db = sqlite3.connect(args.db, timeout=30)
    db.row_factory = sqlite3.Row

    # Load existing options — share_symbol is already set
    rows_db = db.execute(
        "SELECT option_symbol, share_symbol FROM asx_options ORDER BY share_symbol, option_symbol"
    ).fetchall()
    print(f"Options in {args.db}: {len(rows_db)}")

    # Group by underlying (share_symbol), fall back to derivation if missing
    by_underlying: dict[str, list[str]] = defaultdict(list)
    for row in rows_db:
        sym   = row['option_symbol']
        under = row['share_symbol'] or option_to_underlying(sym)
        if under:
            by_underlying[under.strip()].append(sym)

    print(f"Unique underlyings to query: {len(by_underlying)}")

    ib = IB()
    ib.connect(args.host, args.port, clientId=args.client_id, timeout=15)
    print(f"Connected to IB Gateway {args.host}:{args.port}")

    # Phase 1: query IB — collect results in memory
    updates = []   # (expiry, exercise, share_name, note, fetched_at, option_symbol)
    queried = found = 0

    for underlying, tracked_syms in sorted(by_underlying.items()):
        queried += 1
        details = fetch_warrants(ib, underlying)
        tracked_set = set(tracked_syms)

        for d in details:
            ct        = d.contract
            local_sym = ct.localSymbol
            if not local_sym or local_sym not in tracked_set:
                continue

            expiry     = format_expiry(ct.lastTradeDateOrContractMonth or '')
            exercise   = ct.strike or 0.0
            share_name = d.longName or underlying
            note       = ct.right or None

            if not expiry or not exercise:
                print(f"  SKIP {local_sym}: missing expiry or strike")
                continue

            updates.append((expiry, exercise, share_name, note, local_sym))
            found += 1
            print(f"  {local_sym}: {share_name} | strike={exercise} | expiry={expiry} | right={note}")

        time.sleep(0.05)

    ib.disconnect()

    # Phase 2: write all results in one transaction
    print(f"\nUpdating {len(updates)} warrants in database...")
    db.executemany("""
        UPDATE asx_options
        SET expiry=?, exercise=?, share_name=?, note=?, fetched_at=datetime('now')
        WHERE option_symbol=?
    """, updates)
    db.commit()

    print(f"Done. Queried {queried} underlyings, updated {found}/{len(rows_db)} warrants.")
    db.close()


if __name__ == '__main__':
    main()
