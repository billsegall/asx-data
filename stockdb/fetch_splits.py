#!/usr/bin/env python3
"""
Fetch stock split/consolidation events from Yahoo Finance and store in corporate_events.

For any new split detected, re-downloads full adjusted history for that symbol so
pre-split prices are correctly adjusted in endofday.

Usage:
    python fetch_splits.py --db ../stockdb/stockdb.db [--symbols BHP CBA BTR] [--delay 0.1]
"""

import argparse, datetime, math, sqlite3, sys, time
from datetime import date, timedelta
import yfinance as yf
import pandas as pd

from holidays import is_asx_closed


def to_unix(d):
    """Convert a date to Unix timestamp (local midnight, matching stockdb.py convention)."""
    return time.mktime(datetime.datetime(d.year, d.month, d.day).timetuple())


def _ratio_to_desc(ratio):
    if not math.isfinite(ratio):
        return "Split"
    if ratio >= 1:
        return f"{round(ratio)}:1 Split"
    denom = round(1 / ratio)
    return f"1:{denom} Consolidation"


def _ratio_to_type(ratio):
    if not math.isfinite(ratio) or ratio >= 1:
        return 'split'
    return 'consolidation'


def redownload_history(sym, db_cursor, db):
    """Re-download full adjusted history for a symbol and INSERT OR REPLACE into endofday."""
    yf_ticker = '^AORD' if sym == 'XAO' else f'{sym}.AX'
    print(f"    Re-downloading full history for {sym}...", end='', flush=True)
    try:
        df = yf.download(
            yf_ticker,
            period='max',
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        print(f" error: {e}")
        return 0

    if df is None or df.empty:
        print(" no data")
        return 0

    # Single-ticker download returns flat columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df = df.dropna(subset=['Close'])

    rows = []
    for date_idx, row_data in df.iterrows():
        d = date_idx.date() if hasattr(date_idx, 'date') else date_idx
        ts = to_unix(d)
        try:
            o  = float(row_data['Open'])
            h  = float(row_data['High'])
            lo = float(row_data['Low'])
            cl = float(row_data['Close'])
            v  = int(row_data.get('Volume', 0))
        except (TypeError, ValueError):
            continue
        if any(math.isnan(x) for x in (o, h, lo, cl)):
            continue
        rows.append((sym, ts, o, h, lo, cl, v))

    if rows:
        db_cursor.execute('DELETE FROM endofday WHERE symbol = ?', (sym,))
        db_cursor.executemany('INSERT INTO endofday VALUES (?, ?, ?, ?, ?, ?, ?)', rows)
        db.commit()

    print(f" {len(rows)} rows")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description='Fetch stock splits from Yahoo Finance')
    parser.add_argument('--db', default='stockdb.db', help='sqlite3 database')
    parser.add_argument('--symbols', nargs='+', help='Symbols to check (default: all)')
    parser.add_argument('--delay', type=float, default=0.1,
                        help='Seconds to sleep between symbols (default: 0.1)')
    args = parser.parse_args()

    # Exit early if yesterday was a market holiday (nothing to check)
    yesterday = date.today() - timedelta(days=1)
    if is_asx_closed(yesterday):
        print(f'Yesterday ({yesterday}) was a market holiday. Exiting.')
        return

    db = sqlite3.connect(args.db)
    c = db.cursor()

    # Ensure table exists
    c.execute('''CREATE TABLE IF NOT EXISTS corporate_events (
        symbol      TEXT    NOT NULL,
        date        INTEGER NOT NULL,
        event_type  TEXT    NOT NULL,
        ratio       REAL    NOT NULL,
        description TEXT,
        PRIMARY KEY (symbol, date)
    )''')
    db.commit()

    current = {r[0] for r in c.execute(
        "SELECT symbol FROM symbols WHERE current = 1 AND industry != 'Delisted'"
    ).fetchall()}
    if args.symbols:
        symbols = [s.upper() for s in args.symbols if s.upper() in current]
    else:
        symbols = sorted(current)

    print(f"Checking {len(symbols)} symbols for split events...")

    new_events = 0
    redownloads = 0

    for i, sym in enumerate(symbols):
        yf_sym = '^AORD' if sym == 'XAO' else f'{sym}.AX'
        try:
            ticker = yf.Ticker(yf_sym)
            splits = ticker.splits  # pandas Series: {date: ratio}
        except Exception as e:
            print(f"  {sym}: error fetching splits: {e}")
            continue

        if splits is None or splits.empty:
            if args.delay > 0:
                time.sleep(args.delay)
            continue

        for split_date, ratio in splits.items():
            if not math.isfinite(ratio) or abs(ratio - 1.0) < 0.1:
                continue  # non-finite or near-1.0 events

            # Normalise date to Unix timestamp
            d = split_date.date() if hasattr(split_date, 'date') else split_date
            ts = int(to_unix(d))

            # Check if already in DB
            existing = c.execute(
                'SELECT 1 FROM corporate_events WHERE symbol = ? AND date = ?', (sym, ts)
            ).fetchone()

            if existing:
                continue

            desc = _ratio_to_desc(ratio)
            etype = _ratio_to_type(ratio)
            print(f"  {sym}: new {etype} on {d} (ratio={ratio:.4f}) — {desc}")

            # Re-download full adjusted history
            redownload_history(sym, c, db)
            redownloads += 1

            # Insert event
            c.execute(
                'INSERT OR REPLACE INTO corporate_events (symbol, date, event_type, ratio, description) '
                'VALUES (?, ?, ?, ?, ?)',
                (sym, ts, etype, float(ratio), desc)
            )
            db.commit()
            new_events += 1

        if args.delay > 0:
            time.sleep(args.delay)

        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(symbols)} symbols checked")

    db.close()
    print(f"\nDone: {new_events} new events found, {redownloads} symbols re-downloaded")


if __name__ == '__main__':
    main()
