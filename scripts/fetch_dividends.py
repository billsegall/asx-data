#!/usr/bin/env python3
"""
fetch_dividends.py — Fetch historical dividend data for ASX stocks from Yahoo Finance.

Stores ex-dividend date and per-share amount in the dividends table.
Uses INSERT OR IGNORE so re-runs are safe — only new dividends are written.

Run monthly to pick up new dividends; first run backfills full history (~22 min for all symbols).

Usage:
  python3 fetch_dividends.py [--db /path/to/stockdb.db] [--symbol BHP] [--all] [--delay SECONDS]
"""

import argparse, sqlite3, time, logging
from datetime import datetime, timezone
from pathlib import Path

import yfinance

SCRIPT_DIR = Path(__file__).parent
DEFAULT_DB  = SCRIPT_DIR.parent / 'stockdb' / 'stockdb.db'
DELAY       = 0.3
LOG_EVERY   = 100

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def init_table(conn: sqlite3.Connection) -> None:
    conn.execute('''CREATE TABLE IF NOT EXISTS dividends (
        symbol   TEXT    NOT NULL,
        ex_date  INTEGER NOT NULL,
        amount   REAL    NOT NULL,
        currency TEXT    NOT NULL DEFAULT 'AUD',
        PRIMARY KEY (symbol, ex_date)
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_dividends_symbol ON dividends(symbol)')
    conn.commit()


def fetch_for_symbol(symbol: str) -> list[tuple]:
    """Fetch dividend history for one symbol. Returns list of (symbol, ex_date_unix, amount, currency)."""
    ticker = yfinance.Ticker(symbol + '.AX')
    divs = ticker.dividends
    if divs is None or len(divs) == 0:
        return []
    rows = []
    for dt, amount in divs.items():
        # Convert pandas Timestamp (possibly tz-aware) to Unix integer
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            ts = int(dt.timestamp())
        else:
            ts = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
        rows.append((symbol, ts, float(amount), 'AUD'))
    return rows


def run() -> None:
    parser = argparse.ArgumentParser(description='Fetch ASX dividend history from Yahoo Finance')
    parser.add_argument('--db',     default=str(DEFAULT_DB), help='Path to stockdb.db')
    parser.add_argument('--symbol', help='Single ASX symbol to fetch (for testing, e.g. BHP)')
    parser.add_argument('--all',    action='store_true', help='Include delisted symbols (current=0)')
    parser.add_argument('--delay',  type=float, default=DELAY, help='Seconds between requests')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    init_table(conn)

    if args.symbol:
        symbols = [args.symbol.strip().upper()]
    else:
        # XAO is the ASX200 index — no dividends, skip it
        EXCLUDE = {'XAO'}
        query = 'SELECT symbol FROM symbols' + ('' if args.all else ' WHERE current = 1')
        symbols = [r['symbol'] for r in conn.execute(query).fetchall() if r['symbol'] not in EXCLUDE]

    log.info(f'Fetching dividends for {len(symbols)} symbol(s)')
    new_count = skip_count = error_count = 0

    for i, symbol in enumerate(symbols):
        if i > 0 and i % LOG_EVERY == 0:
            log.info(f'  {i}/{len(symbols)} processed — new: {new_count}, no-data: {skip_count}, errors: {error_count}')
        try:
            rows = fetch_for_symbol(symbol)
            if not rows:
                skip_count += 1
                time.sleep(args.delay)
                continue
            cur = conn.executemany(
                'INSERT OR IGNORE INTO dividends (symbol, ex_date, amount, currency) VALUES (?, ?, ?, ?)',
                rows
            )
            conn.commit()
            new_count += cur.rowcount
        except Exception as e:
            log.warning(f'  {symbol}: {e}')
            error_count += 1
        time.sleep(args.delay)

    log.info(f'Done — new rows: {new_count}, no-data: {skip_count}, errors: {error_count}')
    conn.close()


if __name__ == '__main__':
    run()
