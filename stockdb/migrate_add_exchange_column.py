#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
migrate_add_exchange_column.py — Add the `exchange` column to stockdb.db tables.

Part of the multi-exchange prep groundwork: adds an additive
`exchange TEXT NOT NULL DEFAULT 'ASX'` column to every table that stores
per-symbol data, so a future second exchange can be distinguished without
a composite-key rewrite (deferred). Idempotent — checks PRAGMA table_info
before altering each table, so it is safe to re-run.

Usage:
  python3 migrate_add_exchange_column.py [--db /path/to/stockdb.db]
  python3 migrate_add_exchange_column.py --dry-run
"""

import argparse
import os
import sqlite3

TABLES = [
    'symbols',
    'endofday',
    'endofmonth',
    'shorts',
    'corporate_events',
    'dividends',
    'fundamentals',
    'financials_annual',
    'shares_history',
    'events',
    'eod_fetch_failures',
    'symbol_changes',
    'asx_options',
]


def has_column(conn, table, column):
    rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
    return any(r[1] == column for r in rows)


def table_exists(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def migrate(db_path, dry_run=False):
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')

    for table in TABLES:
        if not table_exists(conn, table):
            print(f'  SKIP {table}: table does not exist')
            continue
        if has_column(conn, table, 'exchange'):
            print(f'  SKIP {table}: exchange column already present')
            continue
        sql = f"ALTER TABLE {table} ADD COLUMN exchange TEXT NOT NULL DEFAULT 'ASX'"
        if dry_run:
            print(f'  WOULD RUN: {sql}')
        else:
            conn.execute(sql)
            conn.commit()
            print(f'  OK: {sql}')

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Add 'exchange' column to stockdb.db tables")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db))
    parser.add_argument('--dry-run', action='store_true', help='Print planned changes without applying them')
    args = parser.parse_args()

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Migrating {args.db}")
    migrate(args.db, dry_run=args.dry_run)
    print('Done.')


if __name__ == '__main__':
    main()
