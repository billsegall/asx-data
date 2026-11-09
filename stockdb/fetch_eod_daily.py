#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
Fetch daily EOD price data from Yahoo Finance and merge into endofday/endofmonth.

Run daily after market close to keep prices current between eoddata.com zip
file deliveries.

Each run re-fetches the last LOOKBACK_DAYS (or from the last stored date, if
that is older) and upserts: missing rows are inserted, rows that differ from
Yahoo's bar are updated. So a symbol Yahoo skipped one night is picked up on
a later run, and a bar stored before Yahoo finalised it is corrected. Today's
bar is only fetched once the ASX session is over (SESSION_DONE, Sydney time).
Re-run safe: a second run changes nothing unless Yahoo's data changed.

Prices are Yahoo's Close (split-adjusted, not dividend-adjusted), matching
the rest of endofday.
"""

import argparse, datetime, math, sqlite3, sys, time, warnings, logging
from zoneinfo import ZoneInfo
import yfinance as yf
import pandas as pd

from exchanges import is_market_closed, yf_ticker as exchange_yf_ticker
import symbol_status

# Suppress yfinance HTTP warnings and logs
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

BATCH_DELAY = 150  # seconds between batches; ~1 hr total for ~23 batches

BATCH_SIZE = 200

LOOKBACK_DAYS = 7  # calendar days re-fetched every run (about five trading days)
SYDNEY = ZoneInfo('Australia/Sydney')
SESSION_DONE = datetime.time(16, 30)  # closing auction over, Yahoo's daily bar final


def to_unix(d):
    """Convert a date to Unix timestamp (local midnight, matching stockdb.py convention)."""
    return time.mktime(datetime.datetime(d.year, d.month, d.day).timetuple())


def is_trading_day(d):
    return d.weekday() < 5 and not is_market_closed(d, 'ASX')


def last_complete_day(now=None):
    """Latest date whose ASX session is over (Sydney time); may be a non-trading day."""
    now = now or datetime.datetime.now(SYDNEY)
    d = now.date()
    return d if now.time() >= SESSION_DONE else d - datetime.timedelta(days=1)


def download_rows(tickers, reverse_map, start_dt, end_dt):
    """yf.download one batch -> [(symbol, ts, open, high, low, close, volume)].

    Returns None if the download failed or came back empty. end_dt is exclusive.
    Bars on weekends and ASX holidays (Yahoo sometimes has them) are dropped,
    as are bars outside [start_dt, end_dt): Yahoo can append today's live,
    unfinished bar even when end_dt excludes today.
    """
    try:
        df = yf.download(
            tickers,
            start=start_dt.strftime('%Y-%m-%d'),
            end=end_dt.strftime('%Y-%m-%d'),
            auto_adjust=False,  # Close is split-adjusted only; Adj Close would add dividends
            progress=False,
            group_by='column',
        )
    except Exception as e:
        print(f" error: {e}")
        return None

    if df is None or df.empty:
        return None

    # Normalise to MultiIndex (price_field, ticker) if single-ticker returned flat columns
    if not isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.MultiIndex.from_tuples([(col, tickers[0]) for col in df.columns])

    # Stack ticker level → index becomes (date, ticker), columns become price fields
    try:
        stacked = df.stack(level=1, future_stack=True)
    except TypeError:
        stacked = df.stack(level=1)  # older pandas

    stacked = stacked.dropna(subset=['Close'])

    rows = []
    for (date_idx, ticker), row_data in stacked.iterrows():
        sym = reverse_map.get(str(ticker))
        if sym is None:
            continue
        d = date_idx.date() if hasattr(date_idx, 'date') else date_idx
        if not (start_dt <= d < end_dt) or not is_trading_day(d):
            continue
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
        rows.append((sym, to_unix(d), o, h, lo, cl, v))
    return rows


def refresh_endofmonth(c, affected_months):
    """Rebuild endofmonth (last close per symbol per month) for the given 'YYYY-MM' months.

    Months are local (dates are stored as local midnight), not UTC: in UTC a
    row for the 1st sits in the previous month and would become its month-end.
    """
    if not affected_months:
        return
    months = sorted(affected_months)
    ph = ','.join('?' * len(months))
    # Delete stale endofmonth entries for these months
    c.execute(
        f"DELETE FROM endofmonth WHERE strftime('%Y-%m', datetime(date,'unixepoch','localtime')) IN ({ph})",
        months,
    )
    # Re-insert: last trading day close per (symbol, month)
    c.execute(
        f"""INSERT INTO endofmonth(symbol, date, close)
            SELECT e.symbol, e.date, e.close
            FROM endofday e
            JOIN (
                SELECT symbol,
                       strftime('%Y-%m', datetime(date,'unixepoch','localtime')) AS ym,
                       MAX(date) AS max_date
                FROM endofday
                WHERE strftime('%Y-%m', datetime(date,'unixepoch','localtime')) IN ({ph})
                GROUP BY symbol, ym
            ) m ON e.symbol = m.symbol AND e.date = m.max_date
        """,
        months,
    )


def _same_bar(old, new):
    """True if a stored (o, h, l, c, v) matches Yahoo's to float precision."""
    if old[4] != new[4]:
        return False
    return all(a is not None and abs(a - b) <= 1e-6 * max(1.0, abs(b)) for a, b in zip(old[:4], new[:4]))


PROBE_DAYS = 14  # look-back window when checking switched-off listed codes for trades


def reenable_listed(db, today, delay, include_today=True):
    """Switch back on codes that are on the ASX official list, current=0, and trading again.

    Nothing else ever sets current back to 1 once a code is switched off (no
    EOD for 90 days, 10 consecutive misses, renamed away), so without this a
    relisting (WMC, after years suspended), a suspension that resumes, or a
    new listing that was on the official list before it first traded would
    stay invisible for good. Probes those codes for any trade with volume in
    the last PROBE_DAYS; each one that has traded is switched on and its
    missing history since its last stored EOD is backfilled.

    include_today=False stops at the day before `today`, so no unfinished
    intraday bar is stored. Callers normally pass last_complete_day() as
    `today` instead.

    Returns the set of 'YYYY-MM' months that received rows.
    """
    c = db.cursor()
    official = symbol_status.official_codes()
    if official is None:
        print("  Re-enable check skipped: ASX official symbol list unavailable", file=sys.stderr)
        return set()
    candidates = symbol_status.listed_inactive(c, official)
    if not candidates:
        return set()

    ticker_map = {sym: exchange_yf_ticker(sym) for sym in candidates}
    reverse_map = {v: k for k, v in ticker_map.items()}
    tickers = list(ticker_map.values())
    end_dt = today + datetime.timedelta(days=1) if include_today else today  # exclusive
    probe_start = today - datetime.timedelta(days=PROBE_DAYS)
    print(f"  Re-enable check: {len(candidates)} listed symbol(s) switched off, probing since {probe_start}")

    trading = set()
    for i in range(0, len(tickers), BATCH_SIZE):
        rows = download_rows(tickers[i:i + BATCH_SIZE], reverse_map, probe_start, end_dt) or []
        trading.update(r[0] for r in rows if r[6] > 0)  # volume > 0: suspended codes can carry flat zero-volume rows
        if delay > 0 and (i + BATCH_SIZE) < len(tickers):
            time.sleep(delay)
    if not trading:
        print("  Re-enable check: none trading")
        return set()

    # Backfill each from the day after its last stored EOD (or all Yahoo has, if none)
    last = {}
    for sym in trading:
        r = c.execute('SELECT MAX(date) FROM endofday WHERE symbol = ?', (sym,)).fetchone()
        last[sym] = r[0] or 0
    earliest = min(last.values())
    backfill_start = (datetime.date.fromtimestamp(earliest) + datetime.timedelta(days=1)
                      if earliest else datetime.date(2000, 1, 1))
    syms = sorted(trading)
    inserts = []
    for i in range(0, len(syms), BATCH_SIZE):
        batch = [ticker_map[s] for s in syms[i:i + BATCH_SIZE]]
        rows = download_rows(batch, reverse_map, backfill_start, end_dt) or []
        inserts.extend(r for r in rows if r[1] > last[r[0]])
        if delay > 0 and (i + BATCH_SIZE) < len(syms):
            time.sleep(delay)

    c.executemany(
        'INSERT OR IGNORE INTO endofday (symbol, date, open, high, low, close, volume) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)', inserts)
    ph = ','.join('?' * len(syms))
    c.execute(f'UPDATE symbols SET current = 1 WHERE symbol IN ({ph})', syms)
    try:
        c.execute(f'DELETE FROM eod_fetch_failures WHERE symbol IN ({ph})', syms)
    except sqlite3.OperationalError:
        pass  # table not created yet
    db.commit()
    print(f"  Re-enable check: switched on {len(syms)} symbol(s), backfilled {len(inserts)} row(s): "
          + ' '.join(syms), file=sys.stderr)
    return {datetime.date.fromtimestamp(r[1]).strftime('%Y-%m') for r in inserts}


def main():
    parser = argparse.ArgumentParser(description='Fetch daily EOD data from Yahoo Finance')
    parser.add_argument('--db', default='stockdb.db', help='sqlite3 database')
    parser.add_argument('--delay', type=float, default=BATCH_DELAY,
                        help=f'Seconds to sleep between batches (default: {BATCH_DELAY})')
    parser.add_argument('--no-reenable', action='store_true',
                        help='Skip the check that switches listed, trading-again symbols back on')
    parser.add_argument('--reenable-only', action='store_true',
                        help='Only run that check (and backfill), not the daily fetch')
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    c = db.cursor()

    end_day = last_complete_day()

    if args.reenable_only:
        refresh_endofmonth(c, reenable_listed(db, end_day, args.delay))
        db.commit()
        db.close()
        return

    # Build ticker list first: current symbols only, excluding exchange-listed
    # options/warrants (their names contain "OPTION" or "WARRANT"). These are
    # separate from the intraday options fetched by fetch_options_eod.py.
    symbols = [r[0] for r in c.execute(
        "SELECT symbol FROM symbols WHERE current = 1"
        " AND name NOT LIKE '%OPTION%' AND name NOT LIKE '%WARRANT%'"
    ).fetchall()]

    # Find max date using only the symbols we will fetch. Using MAX(endofday)
    # without this filter previously caused options data (inserted by
    # fetch_options_eod.py with future-dated timestamps) to make the script
    # think regular stock data was already up to date.
    if not symbols:
        print("Error: no symbols to fetch", file=sys.stderr)
        sys.exit(1)
    ph = ','.join('?' * len(symbols))
    row = c.execute(f'SELECT MAX(date) FROM endofday WHERE symbol IN ({ph})', symbols).fetchone()
    if not row or row[0] is None:
        print("Error: endofday is empty; run full rebuild first", file=sys.stderr)
        sys.exit(1)

    max_dt = datetime.date.fromtimestamp(row[0])
    start_dt = min(max_dt + datetime.timedelta(days=1),
                   end_day - datetime.timedelta(days=LOOKBACK_DAYS))
    print(f"Fetching Yahoo Finance EOD: {start_dt} → {end_day} (last stored: {max_dt})")

    # Rows already stored for these symbols in the window, to upsert against.
    # Only rows of fetched symbols are ever touched (option rows share the table).
    wanted = set(symbols)
    existing = {}
    for sym, ts, *vals in c.execute(
            'SELECT symbol, date, open, high, low, close, volume FROM endofday WHERE date >= ?',
            (to_unix(start_dt),)):
        if sym in wanted:
            existing[(sym, int(ts))] = tuple(vals)

    ticker_map = {sym: exchange_yf_ticker(sym) for sym in symbols}
    reverse_map = {v: k for k, v in ticker_map.items()}
    tickers = list(ticker_map.values())

    print(f"  {len(symbols)} symbols")

    end_dt = end_day + datetime.timedelta(days=1)  # yfinance end is exclusive
    n_inserted = n_updated = 0
    affected_months = set()
    returned = {}  # date -> symbols with a bar that day

    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        print(f"  Batch {bn}/{total_batches} ({len(batch)} tickers)...", end='', flush=True)

        rows = download_rows(batch, reverse_map, start_dt, end_dt)
        if rows is None:
            print(" no data")
            continue
        inserts, updates = [], []
        for sym, ts, o, h, lo, cl, v in rows:
            d = datetime.date.fromtimestamp(ts)
            returned.setdefault(d, set()).add(sym)
            old = existing.get((sym, int(ts)))
            if old is None:
                # Earlier days only gain real trades: Yahoo fills flat zero-volume
                # bars in later that the nightly run never stored.
                if d <= max_dt and v <= 0:
                    continue
                inserts.append((sym, ts, o, h, lo, cl, v))
            elif not _same_bar(old, (o, h, lo, cl, v)):
                updates.append((o, h, lo, cl, v, sym, ts))
            else:
                continue
            affected_months.add(d.strftime('%Y-%m'))

        # Commit batch immediately to reduce memory footprint
        c.executemany(
            'INSERT OR IGNORE INTO endofday (symbol, date, open, high, low, close, volume) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)', inserts)
        c.executemany(
            'UPDATE endofday SET open = ?, high = ?, low = ?, close = ?, volume = ? '
            'WHERE symbol = ? AND date = ?', updates)
        db.commit()
        n_inserted += len(inserts)
        n_updated += len(updates)

        print(f" {len(rows)} rows: {len(inserts)} new, {len(updates)} updated")

        if args.delay > 0 and (i + BATCH_SIZE) < len(tickers):
            time.sleep(args.delay)

    # Miss tracking looks at the latest day Yahoo had any bars for, and only when
    # that day is new: re-runs and holidays must not count the same day twice.
    latest = max(returned) if returned else None
    new_day = latest is not None and latest > max_dt
    returned_symbols = returned.get(latest, set())
    missed_symbols = set(symbols) - returned_symbols

    # Codes on the official list that were switched off but are trading again
    if not args.no_reenable:
        affected_months |= reenable_listed(db, end_day, args.delay)

    # Refresh endofmonth for all months touched by new data
    refresh_endofmonth(c, affected_months)

    db.commit()

    # Refresh current flags: clear current=1 for symbols with no EOD data in the past 90 days.
    # Only touches current=1 rows; switching symbols back on is reenable_listed's job.
    one_year_ago = time.time() - 90 * 24 * 3600
    c.execute('''UPDATE symbols SET current = 0
                 WHERE current = 1
                 AND symbol NOT IN (
                     -- no DISTINCT: lets SQLite range-scan idx_endofday_date_symbol
                     SELECT symbol FROM endofday WHERE date > ?
                 )''', (one_year_ago,))
    db.commit()

    # Track consecutive fetch failures — only for a newly loaded trading day.
    # Symbols that return nothing for 10+ consecutive trading days are reported to stderr.
    if new_day:
        c.execute('''CREATE TABLE IF NOT EXISTS eod_fetch_failures (
            symbol            TEXT PRIMARY KEY,
            consecutive_misses INTEGER NOT NULL DEFAULT 0,
            first_miss_date   TEXT NOT NULL,
            last_miss_date    TEXT NOT NULL,
            exchange          TEXT NOT NULL DEFAULT 'ASX'
        )''')
        today_str = latest.isoformat()

        # Reset counters for symbols that returned data today
        if returned_symbols:
            ph = ','.join('?' * len(returned_symbols))
            c.execute(f'DELETE FROM eod_fetch_failures WHERE symbol IN ({ph})',
                      list(returned_symbols))

        # Increment counters for symbols with no data today
        for sym in missed_symbols:
            c.execute('''
                INSERT INTO eod_fetch_failures(symbol, consecutive_misses, first_miss_date, last_miss_date)
                VALUES (?, 1, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    consecutive_misses = consecutive_misses + 1,
                    last_miss_date     = excluded.last_miss_date
            ''', (sym, today_str, today_str))

        db.commit()

        # Report any symbols at or above the threshold, but skip symbols that
        # traded within the last 7 days (illiquid stocks that just didn't trade today).
        MISS_THRESHOLD = 10
        one_week_ago = time.time() - 7 * 24 * 3600
        flagged = c.execute(
            'SELECT f.symbol, f.consecutive_misses, f.first_miss_date'
            ' FROM eod_fetch_failures f'
            ' WHERE f.consecutive_misses >= ?'
            ' AND NOT EXISTS ('
            '  SELECT 1 FROM endofday e WHERE e.symbol = f.symbol AND e.date > ?'
            ' )'
            ' ORDER BY f.consecutive_misses DESC',
            (MISS_THRESHOLD, one_week_ago)
        ).fetchall()
        if flagged:
            print(f"\n{len(flagged)} symbol(s) with {MISS_THRESHOLD}+ consecutive fetch failures"
                  " (possibly delisted/suspended):", file=sys.stderr)
            for sym, misses, first in flagged:
                print(f"  {sym}: {misses} consecutive misses (since {first})", file=sys.stderr)
            flagged_syms = [sym for sym, _, _ in flagged]
            ph = ','.join('?' * len(flagged_syms))
            c.execute(f'UPDATE symbols SET current = 0 WHERE symbol IN ({ph})', flagged_syms)
            print(f"  → marked {len(flagged_syms)} symbol(s) current=0", file=sys.stderr)
            db.commit()

    db.close()

    print(f"\nDone: {n_inserted} rows inserted, {n_updated} updated for {start_dt} → {end_day}")


if __name__ == '__main__':
    main()
