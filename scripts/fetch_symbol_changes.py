#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
Fetch ASX code changes from asx.com.au and store in stockdb.db.

Usage:
    python3 fetch_symbol_changes.py [--db /path/to/stockdb.db]

Moved from asx-web; now writes to stockdb.db (was users.db).
"""
import argparse
import datetime
import os
import re
import sqlite3
import urllib.request

URL = 'https://www.asx.com.au/markets/market-resources/asx-codes-and-descriptors/asx-code-changes'

MONTH_MAP = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
}


def parse_date(date_str, year):
    """Parse '4-Mar' or '23 Dec' + year -> 'YYYY-MM-DD'."""
    s = date_str.strip()
    # Handle both 'day-Mon' and 'day Mon'
    parts = re.split(r'[-\s]+', s)
    if len(parts) != 2:
        return None
    day, mon = parts
    month = MONTH_MAP.get(mon[:3].capitalize())
    if not month:
        return None
    try:
        return f'{year}-{month}-{int(day):02d}'
    except ValueError:
        return None


def fetch_html():
    req = urllib.request.Request(
        URL,
        headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode('utf-8', errors='replace')


def parse_changes(html):
    """Return list of (old_symbol, new_symbol, effective_date, new_name) tuples.
    new_name is the company name under the new code, straight from the source
    table (cell 4) — a rename often changes the name too (e.g. HGO -> KAN is
    also Hillgrove Resources -> Kantra Copper), so the old symbol's name can't
    be assumed to carry over."""
    # Each year tab is preceded by dc:title&#34;:&#34;YEAR&#34;
    sections = re.split(r'dc:title&#34;:&#34;(\d{4})&#34;', html)

    records = []
    for i in range(1, len(sections), 2):
        year = sections[i]
        content = sections[i + 1] if i + 1 < len(sections) else ''

        table_m = re.search(r'<table\b.*?</table>', content, re.DOTALL)
        if not table_m:
            continue

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_m.group(0), re.DOTALL)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) < 4:
                continue
            cells = [re.sub(r'<[^>]+>', '', c).replace('&amp;', '&').strip()
                     for c in cells]
            date_s, old_code, _, new_code = cells[0], cells[1], cells[2], cells[3]
            new_name = cells[4] if len(cells) >= 5 else ''

            old_code = old_code.upper()
            new_code = new_code.upper()

            if not old_code or not new_code or old_code == new_code:
                continue  # skip name-only changes

            date = parse_date(date_s, year)
            if not date:
                continue

            records.append((old_code, new_code, date, new_name))

    return records


# Every table in stockdb.db keyed by a stock ticker whose history should follow
# a rename (market data, corporate actions, financials, predictions...). Column
# name is usually 'symbol'; asx_options uses 'share_symbol' since its own PK is
# the option's own code, not the underlying's.
#
# date_col/date_kind bound the migration to rows that predate the rename —
# ASX reuses vacated codes for unrelated later listings (confirmed: AR1 was
# EM1's code until 2018, then reissued to Austral Resources, a completely
# different company still trading under AR1 today). A blind, unconditional
# `WHERE symbol = old_symbol` would sweep that unrelated company's current
# data into the old entity's identity along with the real historical rows.
# date_kind 'epoch' means the column stores unix seconds; 'text' means
# 'YYYY-MM-DD' (or an ISO datetime whose first 10 chars sort the same way).
# None means the table has no natural per-row historical date (pure
# operational bookkeeping) — low-stakes enough to migrate unconditionally.
HISTORY_TABLES = [
    ('endofday',          'symbol',       'date',            'epoch'),
    ('endofmonth',        'symbol',       'date',            'epoch'),
    ('shorts',            'symbol',       'date',            'epoch'),
    ('corporate_events',  'symbol',       'date',            'epoch'),
    ('dividends',         'symbol',       'ex_date',         'epoch'),
    ('fundamentals',      'symbol',       'date',            'text'),
    ('shares_history',    'symbol',       'date',            'text'),
    ('events',            'symbol',       'event_date',      'epoch'),
    ('financials_annual', 'symbol',       'fiscal_year_end', 'text'),
    ('kronos_predictions','symbol',       'date',            'epoch'),
    ('eod_fetch_failures','symbol',       None,               None),
    ('asx_options',       'share_symbol', None,               None),
]


def migrate_history(conn, old_symbol, new_symbol, effective_date):
    """Move market/announcement-adjacent data from old_symbol to new_symbol so
    a rename doesn't orphan years of price/dividend/financials history under a
    code nothing queries anymore — but only rows that actually predate the
    rename (see HISTORY_TABLES docstring above for why that bound matters).

    Additionally guards against new_symbol having its OWN independent history
    that predates this very rename: that would mean new_symbol was a real,
    different, unrelated entity's code before being reissued — must not be
    silently merged with old_symbol's data. This is deliberately NOT triggered
    by new_symbol simply having rows at all — for any rename more than a few
    weeks old, new_symbol has been trading under its own name ever since and
    of course already has its own (perfectly legitimate, continuous) post-
    rename history; that's the ordinary case this whole migration exists to
    complete, not a red flag. Recorded in symbol_history_migrations either way
    so nothing is silently retried; a 'skipped_collision' row means a human
    needs to look at it.
    """
    conn.execute('''CREATE TABLE IF NOT EXISTS symbol_history_migrations (
        old_symbol  TEXT NOT NULL,
        new_symbol  TEXT NOT NULL,
        table_name  TEXT NOT NULL,
        status      TEXT NOT NULL,
        rows_moved  INTEGER NOT NULL DEFAULT 0,
        migrated_at TEXT NOT NULL,
        PRIMARY KEY (old_symbol, new_symbol, table_name)
    )''')
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    # Small grace window past the official effective_date: weekly/monthly fetch
    # cron jobs (fundamentals, financials, shares_history) can legitimately
    # write one more snapshot for the old code in the days right after a
    # rename, before this script has run to catch up — that's normal lag, not
    # code reuse. Real code-reuse gaps run to years, so 14 days costs nothing
    # in collision safety.
    grace = datetime.timedelta(days=14)
    effective_dt = datetime.datetime.strptime(effective_date, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
    # EOD-derived epoch columns store the trading day's midnight-AEST/AEDT
    # instant, which renders as ~14:00-15:00 UTC on the *previous* calendar
    # day (e.g. trading day 2018-04-16 is stored as 2018-04-15T14:00:00Z).
    # Comparing against plain UTC midnight of effective_date would therefore
    # misread the rename's own first trading day as "predates the rename".
    # Back the strict collision cutoff off by a day to absorb that — cheap,
    # and irrelevant to real collisions, which are always years apart.
    effective_epoch_strict = int((effective_dt - datetime.timedelta(days=1)).timestamp())
    effective_epoch_grace = int((effective_dt + grace).timestamp())
    effective_date_grace = (effective_dt + grace).strftime('%Y-%m-%d')

    for table, col, date_col, date_kind in HISTORY_TABLES:
        already = conn.execute(
            'SELECT 1 FROM symbol_history_migrations WHERE old_symbol=? AND new_symbol=? AND table_name=?',
            (old_symbol, new_symbol, table)
        ).fetchone()
        if already:
            continue  # already resolved (migrated or flagged) — never re-touch

        if date_col is not None:
            cutoff = effective_epoch_strict if date_kind == 'epoch' else effective_date
            collision = conn.execute(
                f'SELECT 1 FROM {table} WHERE {col} = ? AND {date_col} < ? LIMIT 1',
                (new_symbol, cutoff)
            ).fetchone()
            if collision:
                conn.execute(
                    'INSERT INTO symbol_history_migrations '
                    '(old_symbol, new_symbol, table_name, status, migrated_at) VALUES (?,?,?,?,?)',
                    (old_symbol, new_symbol, table, 'skipped_collision', now)
                )
                print(f"  WARNING: {table}.{col}={new_symbol} already has rows predating "
                      f"{effective_date} — skipping migration from {old_symbol} "
                      f"(possible code reuse, needs manual review)")
                continue

        if date_col is None:
            where_extra, params = '', (new_symbol, old_symbol)
        elif date_kind == 'epoch':
            where_extra, params = f' AND {date_col} <= ?', (new_symbol, old_symbol, effective_epoch_grace)
        else:  # 'text'
            where_extra, params = f' AND {date_col} <= ?', (new_symbol, old_symbol, effective_date_grace)

        # OR IGNORE: a handful of tables have a (symbol, date) PK/unique index, and
        # a same-day fetch can rarely land on an identical date for both codes
        # during the transition week (e.g. a weekly cron already ran once under
        # the new code before this migration caught up). Leaves that one row
        # behind under old_symbol rather than aborting the whole migration.
        cur = conn.execute(f'UPDATE OR IGNORE {table} SET {col} = ? WHERE {col} = ?{where_extra}', params)

        left_behind = 0
        if date_col is not None:
            left_behind = conn.execute(
                f'SELECT COUNT(*) FROM {table} WHERE {col} = ?', (old_symbol,)
            ).fetchone()[0]

        conn.execute(
            'INSERT INTO symbol_history_migrations '
            '(old_symbol, new_symbol, table_name, status, rows_moved, migrated_at) VALUES (?,?,?,?,?,?)',
            (old_symbol, new_symbol, table, 'migrated', cur.rowcount, now)
        )
        if cur.rowcount:
            print(f"  {table}: migrated {cur.rowcount} row(s) {old_symbol} -> {new_symbol}")
        if left_behind:
            print(f"  {table}: left {left_behind} row(s) under {old_symbol} dated after {effective_date} "
                  f"— likely the code has since been reissued to an unrelated listing")


def store_changes(records, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE IF NOT EXISTS symbol_changes (
        old_symbol     TEXT NOT NULL,
        new_symbol     TEXT NOT NULL,
        effective_date TEXT NOT NULL,
        exchange       TEXT NOT NULL DEFAULT 'ASX',
        PRIMARY KEY (old_symbol, new_symbol, effective_date)
    )''')
    try:
        conn.execute('ALTER TABLE symbol_changes ADD COLUMN new_name TEXT')
    except sqlite3.OperationalError:
        pass  # column already exists

    count_before = conn.execute('SELECT COUNT(*) FROM symbol_changes').fetchone()[0]
    today = datetime.date.today().isoformat()
    for old_code, new_code, effective_date, new_name in records:
        conn.execute(
            '''INSERT INTO symbol_changes (old_symbol, new_symbol, effective_date, new_name)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(old_symbol, new_symbol, effective_date) DO UPDATE SET
                   new_name = excluded.new_name''',
            (old_code, new_code, effective_date, new_name or None)
        )

        # Populate the new code into `symbols` immediately rather than waiting on
        # the next fetch_symbols run against ASX's official listed-companies CSV
        # (which can lag days behind a rename, or — as found investigating a
        # missing OBT — be silently broken for weeks with nothing surfacing the
        # failure). Only for renames already in effect: a code scheduled for a
        # future date (e.g. HGO -> KAN effective next month) doesn't trade yet
        # and shouldn't appear as a live symbol before it does.
        if effective_date <= today:
            old_row = conn.execute(
                'SELECT name, industry, shares FROM symbols WHERE symbol = ?', (old_code,)
            ).fetchone()
            old_name, industry, shares = old_row if old_row else (None, None, None)
            name = new_name or old_name or new_code
            conn.execute(
                '''INSERT INTO symbols (symbol, name, industry, shares, current)
                   VALUES (?, ?, ?, ?, 1)
                   ON CONFLICT(symbol) DO UPDATE SET
                       name     = excluded.name,
                       industry = COALESCE(symbols.industry, excluded.industry),
                       shares   = COALESCE(symbols.shares, excluded.shares),
                       current  = 1''',
                (new_code, name, industry, shares)
            )
            conn.execute('UPDATE symbols SET current = 0 WHERE symbol = ?', (old_code,))
            migrate_history(conn, old_code, new_code, effective_date)

    count_after = conn.execute('SELECT COUNT(*) FROM symbol_changes').fetchone()[0]
    conn.commit()
    conn.close()
    return count_after - count_before


def main():
    parser = argparse.ArgumentParser(description='Fetch ASX symbol changes')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, '..', 'stockdb', 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db),
                        help='Path to stockdb.db')
    args = parser.parse_args()

    print('Fetching ASX code changes...')
    html = fetch_html()
    records = parse_changes(html)
    print(f'Parsed {len(records)} symbol changes')

    inserted = store_changes(records, args.db)
    print(f'Inserted {inserted} new records into {args.db}')


if __name__ == '__main__':
    main()
