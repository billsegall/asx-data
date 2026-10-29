# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""fetch_events.py: a fresh Yahoo date supersedes stale future rows of the same type."""
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
import fetch_events  # noqa: E402

DAY = 86400


def run(monkeypatch, db_path, info):
    class FakeTicker:
        def __init__(self, _t):
            self.info = info
    monkeypatch.setattr(fetch_events.yf, 'Ticker', FakeTicker)
    monkeypatch.setattr(sys, 'argv', ['fetch_events.py', '--db', db_path, '--delay', '0', '--symbols', 'WBC'])
    fetch_events.main()


def rows(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return sorted(conn.execute('SELECT event_type, event_date FROM events').fetchall())
    finally:
        conn.close()


def test_new_date_replaces_stale_future_rows(monkeypatch, tmp_path):
    db_path = str(tmp_path / 'stock.db')
    conn = sqlite3.connect(db_path)
    fetch_events.create_table(conn)
    now = int(time.time())
    future, shifted, recent_past = now + 4 * DAY, now + 4 * DAY + 3600, now - 2 * DAY
    for ts, typ in [(shifted, 'earnings'), (now + 10 * DAY, 'earnings'),
                    (recent_past, 'earnings'), (now + 30 * DAY, 'ex_dividend')]:
        conn.execute("INSERT INTO events (symbol, event_date, event_type, title, fetched_at)"
                     " VALUES ('WBC', ?, ?, 't', 'old')", (ts, typ))
    conn.commit()
    conn.close()

    run(monkeypatch, db_path, {'quoteType': 'EQUITY', 'earningsTimestampStart': future})

    assert rows(db_path) == sorted([
        ('earnings', future),            # fetched date kept
        ('earnings', recent_past),       # past event left alone
        ('ex_dividend', now + 30 * DAY), # type Yahoo didn't return left alone
    ])
