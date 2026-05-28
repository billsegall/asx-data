#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_crypto.py — Fetch top-N crypto prices from CoinGecko + yfinance.

Two tables in stockdb.db:
  crypto_meta   — one row per coin (rank, name, price, market cap, 24h stats)
  crypto_prices — daily OHLCV history per coin

First run: use --backfill to load full history (default 2 years).
Subsequent runs: incremental (fetches only since last stored date per coin).

Usage:
  python3 fetch_crypto.py [--db PATH] [--top 100] [--backfill]
  python3 fetch_crypto.py --symbol BTC --backfill
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

SCRIPT_DIR   = Path(__file__).parent
DEFAULT_DB   = SCRIPT_DIR.parent / 'stockdb' / 'stockdb.db'
BACKFILL_DAYS = 730  # 2 years
DEFAULT_DAYS  = 30   # incremental default when no history yet

COINGECKO_URL = 'https://api.coingecko.com/api/v3/coins/markets'
YF_DELAY = 0.5  # seconds between yfinance calls

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=__import__('sys').stdout,
)
log = logging.getLogger(__name__)


def init_tables(conn: sqlite3.Connection) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS crypto_meta (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            cg_id           TEXT,
            yf_symbol       TEXT,
            rank            INTEGER,
            price           REAL,
            change_pct_24h  REAL,
            market_cap      REAL,
            volume_24h      REAL,
            updated_at      TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id      TEXT    NOT NULL,
            date    INTEGER NOT NULL,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL NOT NULL,
            volume  REAL,
            PRIMARY KEY (id, date)
        )
    ''')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_crypto_prices ON crypto_prices(id, date)'
    )
    conn.commit()


def fetch_coingecko(top: int) -> list[dict]:
    """Fetch top-N coins by market cap from CoinGecko free API."""
    coins = []
    per_page = min(top, 250)
    pages = (top + per_page - 1) // per_page
    for page in range(1, pages + 1):
        params = {
            'vs_currency': 'usd',
            'order':       'market_cap_desc',
            'per_page':    per_page,
            'page':        page,
            'sparkline':   'false',
            'price_change_percentage': '24h',
        }
        try:
            r = requests.get(COINGECKO_URL, params=params, timeout=15)
            r.raise_for_status()
            coins.extend(r.json())
        except Exception as e:
            log.error(f'CoinGecko page {page} failed: {e}')
            break
        if len(coins) >= top:
            break
        time.sleep(1.5)  # CoinGecko rate limit
    return coins[:top]


def upsert_meta(conn: sqlite3.Connection, coins: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    for coin in coins:
        symbol = (coin.get('symbol') or '').upper()
        conn.execute('''
            INSERT INTO crypto_meta
                (id, name, cg_id, yf_symbol, rank, price, change_pct_24h, market_cap, volume_24h, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name           = excluded.name,
                cg_id          = excluded.cg_id,
                rank           = excluded.rank,
                price          = excluded.price,
                change_pct_24h = excluded.change_pct_24h,
                market_cap     = excluded.market_cap,
                volume_24h     = excluded.volume_24h,
                updated_at     = excluded.updated_at
        ''', (
            symbol,
            coin.get('name', symbol),
            coin.get('id', ''),
            symbol + '-USD',
            coin.get('market_cap_rank'),
            coin.get('current_price'),
            coin.get('price_change_percentage_24h'),
            coin.get('market_cap'),
            coin.get('total_volume'),
            now,
        ))
    conn.commit()
    log.info(f'Updated crypto_meta for {len(coins)} coins')


def last_stored_date(conn: sqlite3.Connection, crypto_id: str) -> str | None:
    row = conn.execute(
        'SELECT MAX(date) FROM crypto_prices WHERE id = ?', (crypto_id,)
    ).fetchone()
    ts = row[0]
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')


def fetch_yf_ohlcv(yf_symbol: str, start_date: str) -> list[tuple]:
    """Fetch OHLCV from yfinance. Returns [(unix_ts, open, high, low, close, volume), ...]."""
    import yfinance as yf
    try:
        df = yf.download(yf_symbol, start=start_date, auto_adjust=True, progress=False)
    except Exception as e:
        log.warning(f'{yf_symbol}: download failed: {e}')
        return []

    if df is None or len(df) == 0:
        return []

    # yfinance >=0.2 returns MultiIndex columns; flatten to single level
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    rows = []
    for dt, row in df.iterrows():
        try:
            if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                ts = int(dt.timestamp())
            else:
                ts = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
            rows.append((
                ts,
                float(row['Open'])   if 'Open'   in row and row['Open']   == row['Open'] else None,
                float(row['High'])   if 'High'   in row and row['High']   == row['High'] else None,
                float(row['Low'])    if 'Low'    in row and row['Low']    == row['Low']  else None,
                float(row['Close']),
                float(row['Volume']) if 'Volume' in row and row['Volume'] == row['Volume'] else None,
            ))
        except Exception as e:
            log.debug(f'{yf_symbol} row parse error: {e}')
    return rows


def store_prices(conn: sqlite3.Connection, crypto_id: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        'INSERT OR IGNORE INTO crypto_prices (id, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)',
        [(crypto_id,) + row for row in rows]
    )
    conn.commit()
    return len(rows)


def run() -> None:
    parser = argparse.ArgumentParser(description='Fetch crypto prices')
    parser.add_argument('--db',       default=str(DEFAULT_DB), help='Path to stockdb.db')
    parser.add_argument('--top',      type=int, default=100,   help='Top N coins by market cap')
    parser.add_argument('--backfill', action='store_true',     help=f'Fetch {BACKFILL_DAYS} days of history')
    parser.add_argument('--symbol',   default=None,            help='Only fetch this coin (e.g. BTC)')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=30)
    init_tables(conn)

    # Step 1: fetch top-N metadata from CoinGecko
    log.info(f'Fetching top {args.top} coins from CoinGecko...')
    coins = fetch_coingecko(args.top)
    if not coins:
        log.error('CoinGecko returned no data — aborting')
        return
    log.info(f'Got {len(coins)} coins from CoinGecko')
    upsert_meta(conn, coins)

    # Step 2: fetch OHLCV history from yfinance for each coin
    symbols_to_fetch = [
        (c['symbol'].upper(), c['symbol'].upper() + '-USD')
        for c in coins
        if (args.symbol is None or c['symbol'].upper() == args.symbol.upper())
    ]

    backfill_start = (datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)).strftime('%Y-%m-%d')
    default_start  = (datetime.now(timezone.utc) - timedelta(days=DEFAULT_DAYS)).strftime('%Y-%m-%d')

    ok = fail = 0
    for crypto_id, yf_symbol in symbols_to_fetch:
        last = last_stored_date(conn, crypto_id)
        if args.backfill or last is None:
            start = backfill_start if args.backfill else default_start
        else:
            start = (datetime.fromisoformat(last) + timedelta(days=1)).strftime('%Y-%m-%d')
            if start > datetime.now(timezone.utc).strftime('%Y-%m-%d'):
                log.debug(f'{crypto_id}: already up to date')
                ok += 1
                continue

        rows = fetch_yf_ohlcv(yf_symbol, start)
        if rows:
            n = store_prices(conn, crypto_id, rows)
            log.info(f'  {crypto_id}: {n} rows from {start}')
            ok += 1
        else:
            log.warning(f'  {crypto_id} ({yf_symbol}): no data from yfinance')
            fail += 1

        time.sleep(YF_DELAY)

    log.info(f'Done. {ok} ok, {fail} failed')
    conn.close()


if __name__ == '__main__':
    run()
