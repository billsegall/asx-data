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
