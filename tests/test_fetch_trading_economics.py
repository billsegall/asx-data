# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Sanity check on scraped Trading Economics prices."""
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'scripts'))
import fetch_trading_economics as fte  # noqa: E402


def test_plausible_accepts_normal_moves():
    assert fte.plausible(85900.0, 91800.0) is None
    assert fte.plausible(90.2, 89.9) is None
    assert fte.plausible(5.0, None) is None


def test_plausible_rejects_bad_scrapes():
    assert fte.plausible(0.0, 155550.0)            # lithium 0
    assert fte.plausible(62.9, 165750.0)           # lithium wrong number
    assert fte.plausible(278898.0, 16347.5)        # nickel 17x
    assert fte.plausible(7.8, 86.8)                # uranium decimal slip
    assert fte.plausible(-1.0, None)


def _run(tmp_path, price, date_str='November 25, 2026'):
    db = tmp_path / 'c.db'
    conn = sqlite3.connect(db)
    fte.init_tables(conn)
    conn.execute('INSERT INTO commodity_prices VALUES (?, ?, ?)', ('LITHIUM', 1763596800, 91800.0))  # 2025-11-20
    conn.commit()
    conn.close()
    argv = ['x', '--db', str(db), '--commodity', 'lithium']
    with patch.object(sys, 'argv', argv), patch.object(fte, 'fetch_te_price', return_value=(price, date_str)):
        try:
            fte.run()
        except SystemExit:
            pass
    return sqlite3.connect(db).execute(
        "SELECT price FROM commodity_prices WHERE id='LITHIUM' ORDER BY date").fetchall()


def test_run_rejects_implausible(tmp_path):
    assert _run(tmp_path, 0.0) == [(91800.0,)]


def test_run_inserts_plausible(tmp_path):
    assert _run(tmp_path, 85900.0) == [(91800.0,), (85900.0,)]


NEODYMIUM_PAGE = (
    '<html><body><div>Neodymium Rare Earth</div><div>Actual</div><div>927500</div>'
    '<div>Daily Change</div><div>0</div><div>0%</div>'
    '<p>Neodymium traded flat at 927,500 CNY/T on November 27, 2026.</p>'
    '<td>Molybdenum</td><td>607.50</td><td>Gallium 1,730.00 USD</td>'
    '<td>Soda Ash 500 CNY</td></body></html>'
)


def _fetch(html):
    resp = type('R', (), {'text': html, 'raise_for_status': lambda self: None})()
    with patch.object(fte.requests, 'get', return_value=resp):
        return fte.fetch_te_price('neodymium')


def test_fetch_reads_quote_header():
    # The loose "<number> CNY" fallback used to pick up 500 from the sidebar
    assert _fetch(NEODYMIUM_PAGE) == (927500.0, 'November 27, 2026')


def test_fetch_traded_flat_at_fallback():
    assert _fetch('<p>Neodymium traded flat at 927,500 CNY/T on November 27, 2026.</p>') == \
        (927500.0, 'November 27, 2026')


def test_neodymium_mapped():
    assert fte.COMMODITY_MAPPINGS['neodymium'] == ('NEODYMIUM', 'CNY/tonne')
