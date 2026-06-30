#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_currencies.py — Fetch FX currency pair prices via yfinance.

Two tables in stockdb.db:
  currency_meta   — one row per pair (current price, 24h change)
  currency_prices — daily close history per pair

First run: use --backfill to load full history (default 2 years).
Subsequent runs: incremental (fetches only since last stored date per pair).

Usage:
  python3 fetch_currencies.py [--db PATH] [--backfill]
  python3 fetch_currencies.py --pair AUDUSD --backfill
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

SCRIPT_DIR    = Path(__file__).parent
DEFAULT_DB    = SCRIPT_DIR.parent / 'stockdb' / 'stockdb.db'
BACKFILL_DAYS = 730   # 2 years
DEFAULT_DAYS  = 30    # when no history exists yet
YF_DELAY      = 0.5   # seconds between yfinance calls

PAIRS = [
    # AUD pairs
    {'id': 'AUDUSD', 'base': 'AUD', 'quote': 'USD', 'yf_symbol': 'AUDUSD=X', 'group': 'AUD Pairs'},
    {'id': 'AUDEUR', 'base': 'AUD', 'quote': 'EUR', 'yf_symbol': 'AUDEUR=X', 'group': 'AUD Pairs'},
    {'id': 'AUDGBP', 'base': 'AUD', 'quote': 'GBP', 'yf_symbol': 'AUDGBP=X', 'group': 'AUD Pairs'},
    {'id': 'AUDJPY', 'base': 'AUD', 'quote': 'JPY', 'yf_symbol': 'AUDJPY=X', 'group': 'AUD Pairs'},
    {'id': 'AUDNZD', 'base': 'AUD', 'quote': 'NZD', 'yf_symbol': 'AUDNZD=X', 'group': 'AUD Pairs'},
    {'id': 'AUDCNY', 'base': 'AUD', 'quote': 'CNY', 'yf_symbol': 'AUDCNY=X', 'group': 'AUD Pairs'},
    {'id': 'AUDCAD', 'base': 'AUD', 'quote': 'CAD', 'yf_symbol': 'AUDCAD=X', 'group': 'AUD Pairs'},
    {'id': 'AUDSGD', 'base': 'AUD', 'quote': 'SGD', 'yf_symbol': 'AUDSGD=X', 'group': 'AUD Pairs'},
    # Major pairs
    {'id': 'EURUSD', 'base': 'EUR', 'quote': 'USD', 'yf_symbol': 'EURUSD=X', 'group': 'Majors'},
    {'id': 'GBPUSD', 'base': 'GBP', 'quote': 'USD', 'yf_symbol': 'GBPUSD=X', 'group': 'Majors'},
    {'id': 'USDJPY', 'base': 'USD', 'quote': 'JPY', 'yf_symbol': 'USDJPY=X', 'group': 'Majors'},
    {'id': 'USDCAD', 'base': 'USD', 'quote': 'CAD', 'yf_symbol': 'USDCAD=X', 'group': 'Majors'},
    {'id': 'USDCHF', 'base': 'USD', 'quote': 'CHF', 'yf_symbol': 'USDCHF=X', 'group': 'Majors'},
    {'id': 'NZDUSD', 'base': 'NZD', 'quote': 'USD', 'yf_symbol': 'NZDUSD=X', 'group': 'Majors'},
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=__import__('sys').stdout,
)
log = logging.getLogger(__name__)


def init_tables(conn: sqlite3.Connection) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS currency_meta (
            id              TEXT PRIMARY KEY,
            base            TEXT NOT NULL,
            quote           TEXT NOT NULL,
            yf_symbol       TEXT NOT NULL,
            group_name      TEXT,
            price           REAL,
            change_pct_24h  REAL,
            updated_at      TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS currency_prices (
            id      TEXT    NOT NULL,
            date    INTEGER NOT NULL,
            close   REAL    NOT NULL,
            PRIMARY KEY (id, date)
        )
    ''')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_currency_prices ON currency_prices(id, date)'
    )
    conn.commit()


def _df_closes(df: pd.DataFrame) -> 'pd.Series':
    """Extract Close series, flattening MultiIndex columns if needed."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df['Close'].dropna()


def fetch_meta(conn: sqlite3.Connection) -> None:
    """Fetch current price and 24h change for all pairs via yfinance."""
    import yfinance as yf
    now = datetime.now(timezone.utc).isoformat()
    for pair in PAIRS:
        try:
            df = yf.download(pair['yf_symbol'], period='5d', interval='1d',
                             auto_adjust=True, progress=False)
            closes = _df_closes(df)
            if len(closes) < 2:
                log.warning(f"  {pair['id']}: not enough data for 24h change")
                continue
            last_close = float(closes.iloc[-1])
            prev_close = float(closes.iloc[-2])
            change_pct = (last_close - prev_close) / prev_close * 100 if prev_close else None
            conn.execute('''
                INSERT INTO currency_meta
                    (id, base, quote, yf_symbol, group_name, price, change_pct_24h, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    price          = excluded.price,
                    change_pct_24h = excluded.change_pct_24h,
                    updated_at     = excluded.updated_at
            ''', (pair['id'], pair['base'], pair['quote'], pair['yf_symbol'],
                  pair['group'], last_close, change_pct, now))
            log.info(f"  {pair['id']}: {last_close:.5g} ({change_pct:+.3f}%)")
        except Exception as e:
            log.warning(f"  {pair['id']}: meta fetch failed: {e}")
        time.sleep(YF_DELAY)
    conn.commit()


def last_stored_date(conn: sqlite3.Connection, pair_id: str) -> str | None:
    row = conn.execute(
        'SELECT MAX(date) FROM currency_prices WHERE id = ?', (pair_id,)
    ).fetchone()
    ts = row[0]
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')


def fetch_history(conn: sqlite3.Connection, pair: dict, start_date: str) -> int:
    """Fetch daily closes for one pair since start_date. Returns row count stored."""
    import yfinance as yf
    try:
        df = yf.download(pair['yf_symbol'], start=start_date, interval='1d',
                         auto_adjust=True, progress=False)
    except Exception as e:
        log.warning(f"  {pair['id']}: history fetch failed: {e}")
        return 0
    if df is None or len(df) == 0:
        return 0
    closes = _df_closes(df)
    rows = []
    for dt, close in closes.items():
        try:
            if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                ts = int(dt.timestamp())
            else:
                ts = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
            rows.append((pair['id'], ts, float(close)))
        except Exception as e:
            log.debug(f"{pair['id']} row error: {e}")
    if rows:
        conn.executemany(
            'INSERT OR IGNORE INTO currency_prices (id, date, close) VALUES (?,?,?)',
            rows
        )
        conn.commit()
    return len(rows)


def run() -> None:
    parser = argparse.ArgumentParser(description='Fetch FX currency pair prices')
    parser.add_argument('--db',       default=str(DEFAULT_DB), help='Path to stockdb.db')
    parser.add_argument('--backfill', action='store_true',     help=f'Fetch {BACKFILL_DAYS} days of history')
    parser.add_argument('--pair',     default=None,            help='Only fetch this pair (e.g. AUDUSD)')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=30)
    init_tables(conn)

    # Step 1: fetch current prices + 24h change
    log.info('Fetching current FX prices...')
    fetch_meta(conn)

    # Step 2: fetch close history
    backfill_start = (datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)).strftime('%Y-%m-%d')
    default_start  = (datetime.now(timezone.utc) - timedelta(days=DEFAULT_DAYS)).strftime('%Y-%m-%d')
    pairs_to_fetch = [p for p in PAIRS if args.pair is None or p['id'] == args.pair.upper()]

    log.info('Fetching FX history...')
    ok = fail = 0
    for pair in pairs_to_fetch:
        last = last_stored_date(conn, pair['id'])
        if args.backfill or last is None:
            start = backfill_start if args.backfill else default_start
        else:
            next_day = (datetime.fromisoformat(last) + timedelta(days=1)).strftime('%Y-%m-%d')
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            if next_day > today:
                log.debug(f"{pair['id']}: already up to date")
                ok += 1
                continue
            start = next_day

        n = fetch_history(conn, pair, start)
        if n > 0:
            log.info(f"  {pair['id']}: {n} rows from {start}")
            ok += 1
        else:
            log.warning(f"  {pair['id']}: no history returned from {start}")
            fail += 1
        time.sleep(YF_DELAY)

    log.info(f'Done. {ok} ok, {fail} failed')
    conn.close()


if __name__ == '__main__':
    run()
