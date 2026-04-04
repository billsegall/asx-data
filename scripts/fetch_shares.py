#!/usr/bin/env python3
"""
fetch_shares.py — Fetch historical shares-on-issue from Yahoo Finance.

Uses yfinance get_shares_full() which gives a daily time series of shares
outstanding going back ~10 years. Stores annual year-end snapshots per symbol,
giving a compact but informative view of buyback/dilution trends.

Skips indices (e.g., XAO) which have no shares outstanding.
Re-run safe: INSERT OR REPLACE upserts by (symbol, year).

Usage:
  python3 fetch_shares.py [--db /path/to/stockdb.db] [--delay SECONDS]
  python3 fetch_shares.py --symbols BHP RIO CBA
"""

import argparse
import datetime
import os
import sys
import time
import sqlite3

import yfinance as yf
import pandas as pd


DELAY = 0.3
LOG_EVERY = 100
START_DATE = '2010-01-01'


def create_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS shares_history (
            symbol      TEXT NOT NULL,
            year        INTEGER NOT NULL,  -- calendar year (year-end snapshot)
            date        TEXT NOT NULL,     -- YYYY-MM-DD of the actual last data point
            shares      INTEGER NOT NULL,
            fetched_at  TEXT NOT NULL,
            PRIMARY KEY (symbol, year)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_shares_sym ON shares_history(symbol)')
    conn.commit()


def fetch_symbol(ticker_str):
    """Return dict of {year: (date_str, shares)} or None."""
    try:
        t = yf.Ticker(ticker_str)
        s = t.get_shares_full(start=START_DATE)
        if s is None or s.empty:
            return None
        # Take the last value in each calendar year
        annual = s.groupby(s.index.year).last()
        result = {}
        for year, shares in annual.items():
            if pd.notna(shares) and shares > 0:
                # Find the actual date of this last entry
                year_data = s[s.index.year == year]
                last_date = year_data.index[-1].date().isoformat()
                result[year] = (last_date, int(shares))
        return result if result else None
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description='Fetch historical shares outstanding')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, '..', 'stockdb', 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db))
    parser.add_argument('--delay', type=float, default=DELAY)
    parser.add_argument('--symbols', nargs='+',
                        help='Fetch specific symbols only (default: all current)')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute('PRAGMA journal_mode=WAL')
    create_table(conn)

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    else:
        symbols = [r[0] for r in conn.execute(
            """SELECT s.symbol FROM symbols s
               WHERE s.current = 1
               AND s.symbol NOT IN ('XAO')
               AND EXISTS (
                   SELECT 1 FROM endofday e
                   WHERE e.symbol = s.symbol
                   AND e.date >= strftime('%s', 'now', '-30 days')
               )
               ORDER BY s.symbol"""
        ).fetchall()]

    total = len(symbols)
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{now_str}  Fetching shares history for {total} symbols '
          f'(delay={args.delay}s, est. {total * args.delay / 60:.0f} min)')

    ok = skipped = errors = 0

    for i, symbol in enumerate(symbols, 1):
        ticker_str = f'{symbol}.AX'
        data = fetch_symbol(ticker_str)

        if data is None:
            skipped += 1
        else:
            try:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for year, (date_str, shares) in data.items():
                    conn.execute(
                        'INSERT OR REPLACE INTO shares_history (symbol, year, date, shares, fetched_at) VALUES (?,?,?,?,?)',
                        (symbol, year, date_str, shares, now)
                    )
                conn.commit()
                ok += 1
            except Exception as e:
                print(f'  ERROR inserting {symbol}: {e}', file=sys.stderr)
                errors += 1

        if i % LOG_EVERY == 0 or i == total:
            pct = 100 * i / total
            print(f'  {i}/{total} ({pct:.0f}%)  ok={ok}  skipped={skipped}  errors={errors}')

        if i < total and args.delay > 0:
            time.sleep(args.delay)

    conn.close()
    print(f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  Done: {ok} upserted, '
          f'{skipped} skipped, {errors} errors')


if __name__ == '__main__':
    main()
