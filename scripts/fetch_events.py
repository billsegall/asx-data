#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_events.py — Fetch upcoming corporate events from Yahoo Finance and store in stockdb.db.

Fetches one symbol at a time (yfinance Ticker.info). Runs weekly — typically Friday
evening after fundamentals are updated.

Re-run safe: INSERT OR REPLACE upserts by (symbol, event_date, event_type), so
re-running replaces existing rows for the same event. Only events within the last 7 days
or in the future are stored.

Usage:
  python3 fetch_events.py [--db /path/to/stockdb.db] [--delay SECONDS] [--symbols SYM ...]
"""

import argparse
import datetime
import os
import sqlite3
import subprocess
import sys
import time

import yfinance as yf

DELAY = 0.4          # seconds between requests
LOG_EVERY = 100      # print progress every N symbols


def _int(val):
    """Return int or None."""
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def create_table(conn):
    """Create the events table and indexes if they don't exist. Returns True if already existed."""
    existing = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'"
    ).fetchone() is not None

    conn.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol       TEXT    NOT NULL,
            event_date   INTEGER NOT NULL,
            end_date     INTEGER,
            event_type   TEXT    NOT NULL,
            title        TEXT    NOT NULL,
            description  TEXT,
            is_estimate  INTEGER NOT NULL DEFAULT 0,
            source       TEXT    NOT NULL DEFAULT 'yfinance',
            fetched_at   TEXT    NOT NULL,
            UNIQUE (symbol, event_date, event_type)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_events_sym  ON events(symbol)')
    conn.commit()
    return existing


def fetch_events_for_symbol(symbol, info, cutoff_ts, now_str):
    """
    Extract event rows from a yfinance info dict.

    Returns a list of tuples:
      (symbol, event_date, end_date, event_type, title, description, is_estimate, source, fetched_at)
    """
    rows = []
    is_estimate = 1 if info.get('isEarningsDateEstimate') else 0

    # earnings result date
    earnings_ts = _int(info.get('earningsTimestampStart'))
    if earnings_ts is not None and earnings_ts >= cutoff_ts:
        rows.append((
            symbol,
            earnings_ts,
            None,                        # end_date
            'earnings',
            f'{symbol} Results',
            None,
            is_estimate,
            'yfinance',
            now_str,
        ))

    # earnings call date
    call_ts = _int(info.get('earningsCallTimestampStart'))
    if call_ts is not None and call_ts >= cutoff_ts:
        rows.append((
            symbol,
            call_ts,
            None,
            'earnings_call',
            f'{symbol} Earnings Call',
            None,
            is_estimate,
            'yfinance',
            now_str,
        ))

    # ex-dividend date
    ex_div_ts = _int(info.get('exDividendDate'))
    if ex_div_ts is not None and ex_div_ts >= cutoff_ts:
        rows.append((
            symbol,
            ex_div_ts,
            None,
            'ex_dividend',
            f'{symbol} Ex-Dividend',
            None,
            0,
            'yfinance',
            now_str,
        ))

    return rows


INSERT_SQL = '''
    INSERT OR REPLACE INTO events
        (symbol, event_date, end_date, event_type, title, description,
         is_estimate, source, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


def main():
    parser = argparse.ArgumentParser(description='Fetch corporate events from Yahoo Finance')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, '..', 'stockdb', 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db))
    parser.add_argument('--delay', type=float, default=DELAY,
                        help=f'Seconds between requests (default: {DELAY})')
    parser.add_argument('--symbols', nargs='+',
                        help='Fetch specific symbols only (default: all current)')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute('PRAGMA journal_mode=WAL')
    table_existed = create_table(conn)

    # Only keep events from the last 7 days onwards
    cutoff_ts = int((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).timestamp())
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    else:
        symbols = [r[0] for r in conn.execute(
            """SELECT s.symbol FROM symbols s
               WHERE s.current = 1
               AND EXISTS (
                   SELECT 1 FROM endofday e
                   WHERE e.symbol = s.symbol
                   AND e.date >= strftime('%s', 'now', '-30 days')
               )
               ORDER BY s.symbol"""
        ).fetchall()]

    total = len(symbols)
    print(f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  Fetching events for {total} symbols '
          f'(delay={args.delay}s, est. {total * args.delay / 60:.0f} min)')

    ok = skipped = errors = 0

    for i, symbol in enumerate(symbols, 1):
        ticker_str = f'{symbol}.AX'
        try:
            t = yf.Ticker(ticker_str)
            info = t.info
            if not info or info.get('quoteType') not in ('EQUITY', 'ETF', 'MUTUALFUND'):
                skipped += 1
                info = None
        except Exception:
            skipped += 1
            info = None

        if info is not None:
            rows = fetch_events_for_symbol(symbol, info, cutoff_ts, now_str)
            if rows:
                try:
                    for row in rows:
                        conn.execute(INSERT_SQL, row)
                    conn.commit()
                    ok += 1
                except Exception as e:
                    print(f'  ERROR inserting {symbol}: {e}', file=sys.stderr)
                    errors += 1
            else:
                skipped += 1

        if i % LOG_EVERY == 0 or i == total:
            pct = 100 * i / total
            print(f'  {i}/{total} ({pct:.0f}%)  ok={ok}  skipped={skipped}  errors={errors}')

        if i < total and args.delay > 0:
            time.sleep(args.delay)

    conn.close()
    print(f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  Done: {ok} upserted, '
          f'{skipped} skipped (no data/events), {errors} errors')

    if not table_existed:
        print('New events table created — restarting asx-backend...')
        try:
            subprocess.run(['sudo', 'systemctl', 'restart', 'asx-backend'], check=True)
            print('asx-backend restarted.')
        except Exception as e:
            print(f'WARNING: could not restart asx-backend: {e}', file=sys.stderr)
            print('Please run: sudo systemctl restart asx-backend')


if __name__ == '__main__':
    main()
