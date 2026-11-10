# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""fetch_asx_shares: updates symbols.shares from ASX, leaves gaps alone."""
import os
import sqlite3
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
import fetch_asx_shares  # noqa: E402


@pytest.fixture
def dbpath(tmp_path):
    p = tmp_path / 'stock.db'
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE symbols (symbol text primary key, name text, industry text, shares real, "
              "current INTEGER NOT NULL DEFAULT 1, exchange TEXT NOT NULL DEFAULT 'ASX')")
    c.executemany('INSERT INTO symbols (symbol, name, industry, shares, current) VALUES (?,?,?,?,?)', [
        ('FDR', 'Finder', 'Energy', 398197777.8, 1),   # stale snapshot
        ('BHP', 'BHP', 'Materials', 5000000000, 1),    # ASX has no figure this run
        ('NEW', 'New', 'Energy', 0, 1),                # never had a count
        ('OLD', 'Old', 'Delisted', 100, 0),            # not current: not asked
    ])
    c.commit()
    c.close()
    return p


def _resp(status, shares=None):
    r = MagicMock(status_code=status, ok=200 <= status < 300)
    r.json.return_value = {'data': {'numOfShares': shares}}
    return r


ASX = {'FDR': _resp(200, 554250497), 'BHP': _resp(404), 'NEW': _resp(200, 12345678)}


def _run(dbpath, *extra):
    session = MagicMock()
    session.headers = {}
    session.get.side_effect = lambda url, timeout: ASX[url.split('/')[-2]]
    with patch.object(fetch_asx_shares.requests, 'Session', return_value=session), \
         patch.object(sys, 'argv', ['x', '--db', str(dbpath), '--delay', '0', *extra]):
        fetch_asx_shares.main()
    return session


def _shares(dbpath):
    return dict(sqlite3.connect(dbpath).execute('SELECT symbol, shares FROM symbols'))


def test_updates_from_asx_and_keeps_missing(dbpath):
    session = _run(dbpath)
    s = _shares(dbpath)
    assert s['FDR'] == 554250497
    assert s['NEW'] == 12345678
    assert s['BHP'] == 5000000000          # 404 keeps the old value
    assert s['OLD'] == 100
    asked = {c.args[0].split('/')[-2] for c in session.get.call_args_list}
    assert asked == {'FDR', 'BHP', 'NEW'}  # delisted symbol not requested


def test_dry_run_writes_nothing(dbpath):
    before = _shares(dbpath)
    _run(dbpath, '--dry-run')
    assert _shares(dbpath) == before


def test_symbols_arg_limits_request(dbpath):
    session = _run(dbpath, '--symbols', 'fdr')
    assert session.get.call_count == 1
    assert _shares(dbpath)['FDR'] == 554250497
