#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_asx_shares.py — Refresh symbols.shares from ASX's own shares-on-issue figure.

symbols.shares drives every market cap (shares × last close). It used to be
derived from a hand-downloaded ListCorp snapshot (mcap / price), which goes
stale as companies issue or buy back shares. This script asks ASX directly
(Markit Digital key-statistics → numOfShares) for each current symbol.

A sample check (2026-11) found ASX matched the stale snapshot for 16/60
symbols and Yahoo's shares_outstanding for 55/60; Yahoo lags heavy issuers.

Only symbols ASX returns a positive figure for are updated; anything else
(ETFs without a count, delisted codes, request errors) keeps its value.

Usage:
  python3 fetch_asx_shares.py [--db PATH] [--delay SECONDS] [--dry-run]
  python3 fetch_asx_shares.py --symbols FDR BHP CBA
"""

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

import requests

DB_PATH = Path(__file__).parent.parent / 'stockdb' / 'stockdb.db'
URL = 'https://asx.api.markitdigital.com/asx-research/1.0/companies/{}/key-statistics'
DELAY = 0.4

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def fetch_shares(session, symbol):
    """ASX shares on issue for symbol, or None if unavailable."""
    for attempt in range(3):
        try:
            resp = session.get(URL.format(symbol), timeout=15)
        except requests.RequestException as e:
            log.warning('%s: %s', symbol, e)
            time.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 404:
            return None
        if resp.status_code == 429 or resp.status_code >= 500:
            time.sleep(10 * (attempt + 1))
            continue
        if not resp.ok:
            return None
        n = (resp.json().get('data') or {}).get('numOfShares')
        return n if isinstance(n, (int, float)) and n > 0 else None
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('--db', default=str(DB_PATH))
    ap.add_argument('--delay', type=float, default=DELAY)
    ap.add_argument('--symbols', nargs='+')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=60)
    if args.symbols:
        syms = [s.upper() for s in args.symbols]
    else:
        syms = [r[0] for r in conn.execute(
            "SELECT symbol FROM symbols WHERE current = 1 AND exchange = 'ASX' "
            "AND industry NOT IN ('Delisted', 'Index') ORDER BY symbol")]
    old = dict(conn.execute('SELECT symbol, shares FROM symbols'))
    conn.close()  # no connection (or lock) held during the ~15 min of requests
    log.info('Fetching ASX shares on issue for %d symbols', len(syms))

    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    updated = unchanged = missing = 0
    big = []
    writes = []
    for i, sym in enumerate(syms, 1):
        n = fetch_shares(session, sym)
        if n is None:
            missing += 1
        else:
            prev = old.get(sym) or 0
            if prev and abs(n / prev - 1) < 1e-6:
                unchanged += 1
            else:
                updated += 1
                if not prev or abs(n / prev - 1) >= 0.5:
                    big.append((sym, prev, n))
                writes.append((n, sym))
        if i % 200 == 0:
            log.info('  %d/%d: %d updated, %d unchanged, %d no figure',
                     i, len(syms), updated, unchanged, missing)
        time.sleep(args.delay)
    if writes and not args.dry_run:
        # One short write transaction at the end
        conn = sqlite3.connect(args.db, timeout=60)
        with conn:
            conn.executemany('UPDATE symbols SET shares = ? WHERE symbol = ?', writes)
        conn.close()

    log.info('Done%s: %d updated, %d unchanged, %d with no ASX figure',
             ' (dry run)' if args.dry_run else '', updated, unchanged, missing)
    if big:
        log.info('%d changed by 50%%+ or previously empty:', len(big))
        for sym, prev, n in big[:40]:
            log.info('  %s: %s -> %d', sym, f'{prev:,.0f}' if prev else 'none', n)
    if syms and missing > 0.5 * len(syms):
        log.error('More than half the symbols had no ASX figure — source may be down')
        sys.exit(1)


if __name__ == '__main__':
    main()
