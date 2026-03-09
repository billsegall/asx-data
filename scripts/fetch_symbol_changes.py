#!/usr/bin/env python3
"""
Fetch ASX code changes from asx.com.au and store in stockdb.db.

Usage:
    python3 fetch_symbol_changes.py [--db /path/to/stockdb.db]

Moved from asx-web; now writes to stockdb.db (was users.db).
"""
import argparse
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
    """Return list of (old_symbol, new_symbol, effective_date) tuples."""
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

            old_code = old_code.upper()
            new_code = new_code.upper()

            if not old_code or not new_code or old_code == new_code:
                continue  # skip name-only changes

            date = parse_date(date_s, year)
            if not date:
                continue

            records.append((old_code, new_code, date))

    return records


def store_changes(records, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE IF NOT EXISTS symbol_changes (
        old_symbol     TEXT NOT NULL,
        new_symbol     TEXT NOT NULL,
        effective_date TEXT NOT NULL,
        PRIMARY KEY (old_symbol, new_symbol, effective_date)
    )''')
    inserted = 0
    for rec in records:
        cur = conn.execute(
            'INSERT OR IGNORE INTO symbol_changes (old_symbol, new_symbol, effective_date)'
            ' VALUES (?, ?, ?)', rec
        )
        inserted += cur.rowcount
    conn.commit()
    conn.close()
    return inserted


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
