#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_options.py — Fetch ASX options list and store in stockdb.db.

⚠️  CURRENTLY INACTIVE:
No public data source provides ASX options data with automated access.
The ASX does not provide a public API for options data.
This script is kept for future use if a data source becomes available.

See Database.md → Known Limitations → Options Data Limitation for details.

Usage:
  python3 fetch_options.py [--db /path/to/stockdb.db]

Env vars:
  STOCKDB  (default: ../stockdb/stockdb.db relative to this script)

Moved from asx-web; now writes to stockdb.db (was users.db).
"""

import argparse
import os
import sqlite3
from pathlib import Path
from html.parser import HTMLParser

URL = 'https://rosser.com.au/options.htm'


class OptionsParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_row = []
        self._current_cell = ''
        self._header_done = False

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self._in_table = True
        elif tag == 'tr' and self._in_table:
            self._in_row = True
            self._current_row = []
        elif tag in ('td', 'th') and self._in_row:
            self._in_cell = True
            self._current_cell = ''

    def handle_endtag(self, tag):
        if tag == 'table':
            self._in_table = False
        elif tag == 'tr' and self._in_row:
            self._in_row = False
            if self._current_row and self._header_done:
                self.rows.append(self._current_row)
        elif tag in ('td', 'th') and self._in_cell:
            self._in_cell = False
            self._current_row.append(self._current_cell.strip())
            if tag == 'th':
                self._header_done = True

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data


def fetch(db_path):
    print(f'Fetching {URL}...')

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed. Install with: pip install playwright")
        print("Then run: playwright install")
        return 1

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(URL, wait_until='networkidle', timeout=30000)
            html = page.content()
            browser.close()
    except Exception as e:
        print(f"ERROR fetching page: {e}")
        return 1

    parser = OptionsParser()
    parser.feed(html)

    rows = []
    for row in parser.rows:
        if len(row) < 5:
            continue
        option_symbol = row[0].strip()
        expiry        = row[1].strip()
        share_symbol  = row[3].strip()
        share_name    = row[4].strip()
        note          = row[5].strip() if len(row) > 5 else ''
        if not option_symbol or not expiry or not share_symbol:
            continue
        try:
            exercise = float(row[2].strip())
        except (ValueError, IndexError):
            continue
        rows.append((option_symbol, expiry, exercise, share_symbol, share_name, note or None))

    print(f'Parsed {len(rows)} options.')

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''CREATE TABLE IF NOT EXISTS asx_options (
        option_symbol  TEXT PRIMARY KEY,
        expiry         TEXT NOT NULL,
        exercise       REAL NOT NULL,
        share_symbol   TEXT NOT NULL,
        share_name     TEXT NOT NULL,
        note           TEXT,
        fetched_at     TEXT NOT NULL DEFAULT (datetime('now'))
    )''')

    conn.executemany(
        '''INSERT INTO asx_options
               (option_symbol, expiry, exercise, share_symbol, share_name, note, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(option_symbol) DO UPDATE SET
               expiry       = excluded.expiry,
               share_symbol = excluded.share_symbol,
               share_name   = excluded.share_name,
               fetched_at   = datetime('now'),
               -- Preserve manually corrected exercise/note (marked with "(was ...)");
               -- overwrite everything else with the freshly scraped values.
               exercise = CASE WHEN note LIKE '%(was %)' THEN exercise ELSE excluded.exercise END,
               note     = CASE WHEN note LIKE '%(was %)' THEN note     ELSE excluded.note     END''',
        rows
    )
    conn.commit()

    # Remove options no longer on the page (delisted/expired and removed by rosser)
    current_symbols = {r[0] for r in rows}
    existing = {r[0] for r in conn.execute('SELECT option_symbol FROM asx_options').fetchall()}
    removed = existing - current_symbols
    if removed:
        conn.executemany('DELETE FROM asx_options WHERE option_symbol = ?', [(s,) for s in removed])
        conn.commit()
        print(f'Removed {len(removed)} stale options.')

    conn.close()
    print('Done.')
    return 0


def main():
    parser = argparse.ArgumentParser(description='Fetch ASX options data')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, '..', 'stockdb', 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db),
                        help='Path to stockdb.db')
    args = parser.parse_args()
    exit(fetch(args.db))


if __name__ == '__main__':
    main()
