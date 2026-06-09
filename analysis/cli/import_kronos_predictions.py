#!/usr/bin/env python3
"""Import predictions_kronos.json into the kronos_predictions history table.

Run after each sync to harri so historical snapshots accumulate.

Usage:
    python -m analysis.cli.import_kronos_predictions \
        --db stockdb/stockdb.db \
        --json analysis/results/predictions_kronos.json
"""

import argparse
import json
import sqlite3
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--json', default='analysis/results/predictions_kronos.json')
    args = parser.parse_args()

    with open(args.json) as f:
        data = json.load(f)

    generated_at = data.get('generated_at')
    predictions  = data.get('predictions', [])

    if not generated_at:
        print('[import_kronos] ERROR: no generated_at in JSON', file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(args.db)
    conn.execute('''CREATE TABLE IF NOT EXISTS kronos_predictions (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        generated_at TEXT NOT NULL,
        symbol       TEXT NOT NULL,
        score        REAL NOT NULL,
        date         INTEGER NOT NULL,
        name         TEXT,
        industry     TEXT
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_kronos_pred_run ON kronos_predictions (generated_at)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_kronos_pred_sym ON kronos_predictions (symbol)')

    existing = conn.execute(
        'SELECT COUNT(*) FROM kronos_predictions WHERE generated_at = ?', (generated_at,)
    ).fetchone()[0]
    if existing:
        print(f'[import_kronos] Run {generated_at} already in DB ({existing} rows). Skipping.')
        conn.close()
        return

    rows = [
        (generated_at, p['symbol'], p['score'], p['date'],
         p.get('name'), p.get('industry'))
        for p in predictions
    ]
    conn.executemany(
        'INSERT INTO kronos_predictions (generated_at, symbol, score, date, name, industry) '
        'VALUES (?, ?, ?, ?, ?, ?)',
        rows
    )
    conn.commit()
    conn.close()
    print(f'[import_kronos] Imported {len(rows)} predictions for run {generated_at}')


if __name__ == '__main__':
    main()
