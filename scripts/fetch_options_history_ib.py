#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_options_history_ib.py — Backfill historical EOD prices for ASX warrants from IB.

Fetches 3 months of daily OHLCV bars per warrant and inserts into endofday using
INSERT OR IGNORE (preserves existing records).

IB rate limit: 60 historical requests / 10 min → sleeps 12s between requests.

Usage:
  python3 fetch_options_history_ib.py [--db /path/to/stockdb.db]
                                       [--host HOST] [--port PORT]
                                       [--duration "3 M"]

Env vars:
  STOCKDB   (default: ../stockdb/stockdb.db)
"""

import argparse
import asyncio
import logging
import math
import os
import random
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent.parent
DB_PATH     = SCRIPT_DIR / "stockdb" / "stockdb.db"
IB_HOST     = '127.0.0.1'
IB_PORT     = 4001
REQUEST_GAP = 12   # seconds between historical requests (60/10min limit with headroom)
CHUNK_SIZE  = 50   # contracts per qualifyContracts call
AEST        = timezone(timedelta(hours=10))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def date_to_aest_ts(bar_date) -> int:
    """bar.date (datetime.date object or 'YYYYMMDD' string) → midnight AEST Unix timestamp."""
    from datetime import date as _date
    if isinstance(bar_date, _date):
        return int(datetime(bar_date.year, bar_date.month, bar_date.day, 0, 0, 0, tzinfo=AEST).timestamp())
    s = str(bar_date).replace('-', '')[:8]
    return int(datetime(int(s[:4]), int(s[4:6]), int(s[6:8]), 0, 0, 0, tzinfo=AEST).timestamp())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',       default=os.environ.get('STOCKDB', str(DB_PATH)))
    parser.add_argument('--host',     default=IB_HOST)
    parser.add_argument('--port',     type=int, default=IB_PORT)
    parser.add_argument('--duration', default='3 M',
                        help='IB duration string, e.g. "3 M" or "6 M"')
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    symbols = [r['option_symbol'] for r in conn.execute(
        "SELECT option_symbol FROM asx_options ORDER BY option_symbol"
    ).fetchall()]
    conn.close()

    log.info(f"Symbols to process: {len(symbols)}")

    asyncio.set_event_loop(asyncio.new_event_loop())
    from ib_insync import IB, Contract

    ib = IB()
    ib.connect(args.host, args.port, clientId=random.randint(200, 299), readonly=True, timeout=10)

    # Phase 1: qualify all contracts in chunks (no rate limit on qualifyContracts)
    qualified_map = {}  # localSymbol -> contract
    for i in range(0, len(symbols), CHUNK_SIZE):
        chunk = symbols[i:i + CHUNK_SIZE]
        contracts = [
            Contract(secType='WAR', localSymbol=sym, exchange='ASX', currency='AUD')
            for sym in chunk
        ]
        for c in ib.qualifyContracts(*contracts):
            if c.conId:
                qualified_map[c.localSymbol] = c
        log.info(f"Qualify batch {i // CHUNK_SIZE + 1}: {len(qualified_map)} total so far")

    log.info(f"Qualified {len(qualified_map)}/{len(symbols)} symbols")

    # Phase 2: fetch historical data, one symbol at a time (rate-limited)
    inserted = 0
    no_data  = 0

    conn = sqlite3.connect(db_path)
    try:
        qualified_syms = [s for s in symbols if s in qualified_map]
        for i, sym in enumerate(qualified_syms):
            contract = qualified_map[sym]

            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr=args.duration,
                    barSizeSetting='1 day',
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1,
                )
            except Exception as e:
                log.warning(f"{sym}: reqHistoricalData error: {e}")
                no_data += 1
                time.sleep(REQUEST_GAP)
                continue

            if not bars:
                log.debug(f"{sym}: no bars returned")
                no_data += 1
                time.sleep(REQUEST_GAP)
                continue

            bar_count = 0
            for bar in bars:
                close = bar.close
                if close is None or (isinstance(close, float) and math.isnan(close)) or close <= 0:
                    continue
                ts = date_to_aest_ts(bar.date)
                volume = int(bar.volume) if bar.volume else 0
                cur = conn.execute(
                    "INSERT OR IGNORE INTO endofday (symbol, date, open, high, low, close, volume)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (sym, ts, bar.open, bar.high, bar.low, close, volume)
                )
                inserted += cur.rowcount
                bar_count += 1

            conn.commit()
            log.info(f"[{i+1}/{len(qualified_syms)}] {sym}: {bar_count} bars inserted")

            if i < len(qualified_syms) - 1:
                time.sleep(REQUEST_GAP)

    finally:
        conn.close()
        try:
            ib.disconnect()
        except Exception:
            pass

    missing = [s for s in symbols if s not in qualified_map]
    log.info(f"Done. Inserted {inserted} rows across {len(qualified_syms)} symbols.")
    if missing:
        log.warning(f"Not in IB ({len(missing)}): {', '.join(missing[:30])}")
    return 0


if __name__ == '__main__':
    exit(main())
