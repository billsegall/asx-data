# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""symbols.current maintenance: rename rule (symbol_status) and the
re-enable check for listed, trading-again codes (fetch_eod_daily)."""
import datetime
import os
import sqlite3
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'stockdb'))
import symbol_status  # noqa: E402
import fetch_eod_daily  # noqa: E402

TODAY = '2026-10-28'


def ts(d):
    d = datetime.date.fromisoformat(d)
    return time.mktime(datetime.datetime(d.year, d.month, d.day).timetuple())


@pytest.fixture
def db():
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
        CREATE TABLE symbols (symbol TEXT PRIMARY KEY, name TEXT, industry TEXT, shares REAL,
                              current INTEGER NOT NULL DEFAULT 1, exchange TEXT NOT NULL DEFAULT 'ASX');
        CREATE TABLE endofday (symbol TEXT, date INTEGER, open REAL, high REAL, low REAL,
                               close REAL, volume INTEGER, PRIMARY KEY (symbol, date));
        CREATE TABLE endofmonth (symbol TEXT, date INTEGER, close REAL);
        CREATE TABLE symbol_changes (old_symbol TEXT NOT NULL, new_symbol TEXT NOT NULL,
                                     effective_date TEXT NOT NULL, exchange TEXT NOT NULL DEFAULT 'ASX',
                                     new_name TEXT, PRIMARY KEY (old_symbol, new_symbol, effective_date));
        CREATE TABLE eod_fetch_failures (symbol TEXT PRIMARY KEY, consecutive_misses INTEGER NOT NULL DEFAULT 0,
                                         first_miss_date TEXT NOT NULL, last_miss_date TEXT NOT NULL,
                                         exchange TEXT NOT NULL DEFAULT 'ASX');
    ''')
    yield conn
    conn.close()


def add_symbol(conn, sym, current, name=None):
    conn.execute('INSERT INTO symbols (symbol, name, current) VALUES (?, ?, ?)', (sym, name or sym + ' LTD', current))


def add_eod(conn, sym, date, close=1.0, volume=100):
    conn.execute('INSERT INTO endofday VALUES (?, ?, ?, ?, ?, ?, ?)',
                 (sym, ts(date), close, close, close, close, volume))


def add_change(conn, old, new, date):
    conn.execute('INSERT INTO symbol_changes (old_symbol, new_symbol, effective_date) VALUES (?, ?, ?)',
                 (old, new, date))


def current(conn, sym):
    return conn.execute('SELECT current FROM symbols WHERE symbol = ?', (sym,)).fetchone()[0]


class TestApplyRenameStatus:
    def test_reused_code_latest_rename_wins(self, db):
        # ASL renamed away in 2019, then MMC renamed to ASL in 2024: ASL is live
        add_symbol(db, 'ASL', 1)
        add_change(db, 'ASL', 'PRN', '2019-10-25')
        add_change(db, 'MMC', 'ASL', '2024-07-11')
        add_eod(db, 'ASL', '2026-10-15')
        symbol_status.apply_rename_status(db, TODAY)
        assert current(db, 'ASL') == 1

    def test_reissued_code_with_later_eod_kept(self, db):
        # Renamed away in 2020, reissued as a fresh listing (no rename record) that trades
        add_symbol(db, 'AR1', 1)
        add_change(db, 'AR1', 'XYZ', '2020-01-10')
        add_eod(db, 'AR1', '2026-08-21')
        assert symbol_status.apply_rename_status(db, TODAY) == (0, 0)
        assert current(db, 'AR1') == 1

    def test_renamed_away_code_switched_off(self, db):
        # Old code only has EOD up to the rename (+ grace): switched off
        add_symbol(db, 'OLD', 1)
        add_change(db, 'OLD', 'NEW', '2026-10-06')
        add_eod(db, 'OLD', '2026-10-15')  # within 14-day grace
        assert symbol_status.apply_rename_status(db, TODAY) == (0, 1)
        assert current(db, 'OLD') == 0

    def test_recent_new_code_switched_on(self, db):
        add_symbol(db, 'NEW', 0)
        add_change(db, 'OLD', 'NEW', '2026-10-20')
        symbol_status.apply_rename_status(db, TODAY)
        assert current(db, 'NEW') == 1

    def test_old_new_code_not_switched_on(self, db):
        # Renamed to years ago and since gone quiet: left to the EOD liveness checks
        add_symbol(db, 'NEW', 0)
        add_change(db, 'OLD', 'NEW', '2021-07-19')
        symbol_status.apply_rename_status(db, TODAY)
        assert current(db, 'NEW') == 0

    def test_future_rename_ignored(self, db):
        add_symbol(db, 'HGO', 1)
        add_change(db, 'HGO', 'KAN', '2026-11-15')
        assert symbol_status.apply_rename_status(db, TODAY) == (0, 0)
        assert current(db, 'HGO') == 1

    def test_chain_middle_code_switched_off(self, db):
        # A -> B (2020), B -> C (2025): B's latest event is "renamed away"
        add_symbol(db, 'B', 1)
        add_symbol(db, 'C', 1)
        add_change(db, 'A', 'B', '2020-01-01')
        add_change(db, 'B', 'C', '2025-01-01')
        symbol_status.apply_rename_status(db, TODAY)
        assert current(db, 'B') == 0
        assert current(db, 'C') == 1


class TestReenableListed:
    @pytest.fixture
    def setup(self, db, monkeypatch, tmp_path):
        csv_path = tmp_path / 'asx-official.csv'
        csv_path.write_text('ASX listed companies\n\nCompany name,ASX code,GICS industry group\n'
                            'WILUNA MINING,WMC,Materials\nSUSPENDED CO,SUS,Materials\n'
                            'NEW LISTING,NEW,Materials\nLIVE CO,LIV,Materials\n')
        monkeypatch.setattr(symbol_status, 'OFFICIAL_CSV', str(csv_path))
        monkeypatch.setattr(symbol_status.official_codes, '__defaults__', (str(csv_path),))
        add_symbol(db, 'WMC', 0)    # relisted after years suspended
        add_symbol(db, 'SUS', 0)    # still suspended: Yahoo has flat zero-volume rows
        add_symbol(db, 'NEW', 0)    # new listing, no EOD yet
        add_symbol(db, 'LIV', 1)    # already current: not a candidate
        add_symbol(db, 'GONE', 0)   # not on the official list: never probed
        add_symbol(db, 'WMCO', 0, name='WILUNA OPTION EXP')  # options excluded
        add_eod(db, 'WMC', '2022-12-30', close=0.205)
        db.execute("INSERT INTO eod_fetch_failures VALUES ('WMC', 6, '2026-08-31', '2026-10-13', 'ASX')")
        db.commit()

        calls = []
        yahoo = {
            'WMC': [('2026-10-22', 0.56, 3263260), ('2026-10-27', 0.48, 1234585), ('2026-10-28', 0.50, 1164110)],
            'SUS': [('2026-10-27', 0.10, 0)],
            'NEW': [('2026-10-27', 1.00, 5000)],
        }

        def fake_download(tickers, reverse_map, start_dt, end_dt):
            calls.append((sorted(reverse_map[t] for t in tickers), start_dt, end_dt))
            rows = []
            for t in tickers:
                sym = reverse_map[t]
                for d, cl, v in yahoo.get(sym, []):
                    if start_dt <= datetime.date.fromisoformat(d) < end_dt:
                        rows.append((sym, ts(d), cl, cl, cl, cl, v))
            return rows

        monkeypatch.setattr(fetch_eod_daily, 'download_rows', fake_download)
        return calls

    def test_switches_on_trading_codes_and_backfills(self, db, setup):
        months = fetch_eod_daily.reenable_listed(db, datetime.date(2026, 10, 28), delay=0)
        assert current(db, 'WMC') == 1
        assert current(db, 'NEW') == 1
        assert current(db, 'SUS') == 0     # zero volume only
        assert current(db, 'GONE') == 0
        assert months == {'2026-10'}
        wmc = [r[0] for r in db.execute("SELECT close FROM endofday WHERE symbol='WMC' ORDER BY date")]
        assert wmc == [0.205, 0.56, 0.48, 0.50]
        assert db.execute("SELECT COUNT(*) FROM eod_fetch_failures WHERE symbol='WMC'").fetchone()[0] == 0
        probed = setup[0][0]
        assert probed == ['NEW', 'SUS', 'WMC']   # only listed, switched-off, non-option codes

    def test_backfill_starts_after_earliest_last_eod(self, db, setup):
        fetch_eod_daily.reenable_listed(db, datetime.date(2026, 10, 28), delay=0)
        backfill = setup[1]
        assert backfill[0] == ['NEW', 'WMC']
        assert backfill[1] == datetime.date(2000, 1, 1)  # NEW has no EOD at all

    def test_standalone_run_excludes_today(self, db, setup):
        fetch_eod_daily.reenable_listed(db, datetime.date(2026, 10, 28), delay=0, include_today=False)
        assert all(end == datetime.date(2026, 10, 28) for _, _, end in setup)
        dates = [datetime.date.fromtimestamp(r[0]) for r in
                 db.execute("SELECT date FROM endofday WHERE symbol='WMC' ORDER BY date")]
        assert dates[-1] == datetime.date(2026, 10, 27)

    def test_no_official_list_skips(self, db, monkeypatch):
        monkeypatch.setattr(symbol_status, 'official_codes', lambda path=None: None)
        add_symbol(db, 'WMC', 0)
        assert fetch_eod_daily.reenable_listed(db, datetime.date(2026, 10, 28), delay=0) == set()
        assert current(db, 'WMC') == 0
