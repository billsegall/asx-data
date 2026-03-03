#!/usr/bin/env python3
"""
Fetch daily EOD price data from Yahoo Finance and merge into endofday/endofmonth.

Run daily after market close (18:30 AEST = 08:30 UTC on weekdays) to keep
prices current between eoddata.com zip file deliveries.

Re-run safe: deletes rows >= start_date before inserting, so running twice
on the same day produces the same result.
"""

import argparse, datetime, math, sqlite3, sys, time
import yfinance as yf
import pandas as pd

BATCH_DELAY = 150  # seconds between batches; ~1 hr total for ~23 batches

BATCH_SIZE = 200


def to_unix(d):
    """Convert a date to Unix timestamp (local midnight, matching stockdb.py convention)."""
    return time.mktime(datetime.datetime(d.year, d.month, d.day).timetuple())


def main():
    parser = argparse.ArgumentParser(description='Fetch daily EOD data from Yahoo Finance')
    parser.add_argument('--db', default='stockdb.db', help='sqlite3 database')
    parser.add_argument('--delay', type=float, default=BATCH_DELAY,
                        help=f'Seconds to sleep between batches (default: {BATCH_DELAY})')
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    c = db.cursor()

    # Find the current max date in endofday
    row = c.execute('SELECT MAX(date) FROM endofday').fetchone()
    if not row or row[0] is None:
        print("Error: endofday is empty; run full rebuild first", file=sys.stderr)
        sys.exit(1)

    max_dt = datetime.date.fromtimestamp(row[0])
    start_dt = max_dt + datetime.timedelta(days=1)
    today = datetime.date.today()

    if start_dt > today:
        print(f"Already up to date (max date in endofday: {max_dt})")
        sys.exit(0)

    print(f"Fetching Yahoo Finance EOD: {start_dt} → {today}")

    # Delete any rows already in the target range (re-run idempotency)
    start_ts = to_unix(start_dt)
    c.execute('DELETE FROM endofday WHERE date >= ?', (start_ts,))
    n_deleted = c.rowcount
    if n_deleted:
        print(f"  Removed {n_deleted} existing rows from {start_dt} onwards (re-run cleanup)")

    # Build ticker list: all symbols from symbols table
    symbols = [r[0] for r in c.execute('SELECT symbol FROM symbols').fetchall()]
    ticker_map = {sym: ('^AORD' if sym == 'XAO' else f'{sym}.AX') for sym in symbols}
    reverse_map = {v: k for k, v in ticker_map.items()}
    tickers = list(ticker_map.values())

    print(f"  {len(symbols)} symbols, fetching {start_dt} to {today}")

    end_dt = today + datetime.timedelta(days=1)  # yfinance end is exclusive
    rows_to_insert = []
    affected_months = set()

    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        print(f"  Batch {bn}/{total_batches} ({len(batch)} tickers)...", end='', flush=True)

        try:
            df = yf.download(
                batch,
                start=start_dt.strftime('%Y-%m-%d'),
                end=end_dt.strftime('%Y-%m-%d'),
                auto_adjust=True,
                progress=False,
                group_by='column',
            )
        except Exception as e:
            print(f" error: {e}")
            continue

        if df is None or df.empty:
            print(" no data")
            continue

        # Normalise to MultiIndex (price_field, ticker) if single-ticker returned flat columns
        if not isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_tuples([(col, batch[0]) for col in df.columns])

        # Stack ticker level → index becomes (date, ticker), columns become price fields
        try:
            stacked = df.stack(level=1, future_stack=True)
        except TypeError:
            stacked = df.stack(level=1)  # older pandas

        stacked = stacked.dropna(subset=['Close'])

        batch_rows = 0
        for (date_idx, ticker), row_data in stacked.iterrows():
            sym = reverse_map.get(str(ticker))
            if sym is None:
                continue
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
            rows_to_insert.append((sym, ts, o, h, lo, cl, v))
            affected_months.add(d.strftime('%Y-%m'))
            batch_rows += 1

        print(f" {batch_rows} rows")

        if args.delay > 0 and (i + BATCH_SIZE) < len(tickers):
            time.sleep(args.delay)

    if rows_to_insert:
        c.executemany('INSERT INTO endofday VALUES (?, ?, ?, ?, ?, ?, ?)', rows_to_insert)

    # Refresh endofmonth for all months touched by new data
    if affected_months:
        months = sorted(affected_months)
        ph = ','.join('?' * len(months))
        # Delete stale endofmonth entries for these months
        c.execute(
            f"DELETE FROM endofmonth WHERE strftime('%Y-%m', datetime(date,'unixepoch')) IN ({ph})",
            months,
        )
        # Re-insert: last trading day close per (symbol, month)
        c.execute(
            f"""INSERT INTO endofmonth(symbol, date, close)
                SELECT e.symbol, e.date, e.close
                FROM endofday e
                JOIN (
                    SELECT symbol,
                           strftime('%Y-%m', datetime(date,'unixepoch')) AS ym,
                           MAX(date) AS max_date
                    FROM endofday
                    WHERE strftime('%Y-%m', datetime(date,'unixepoch')) IN ({ph})
                    GROUP BY symbol, ym
                ) m ON e.symbol = m.symbol AND e.date = m.max_date
            """,
            months + months,
        )

    db.commit()
    db.close()

    print(f"\nDone: {len(rows_to_insert)} rows inserted for {start_dt} → {today}")


if __name__ == '__main__':
    main()
