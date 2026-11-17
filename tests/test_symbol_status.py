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
                              current INTEGER NOT NULL DEFAULT 1, exchange TEXT NOT NULL DEFAULT 'ASX',
                              delisted_on TEXT);
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
                            'NEW LISTING,NEW,Materials\nLIVE CO,LIV,Materials\nDEAD CO,DEAD,Materials\n')
        monkeypatch.setattr(symbol_status, 'OFFICIAL_CSV', str(csv_path))
        monkeypatch.setattr(symbol_status.official_codes, '__defaults__', (str(csv_path),))
        add_symbol(db, 'WMC', 0)    # relisted after years suspended
        add_symbol(db, 'SUS', 0)    # still suspended: Yahoo has flat zero-volume rows
        add_symbol(db, 'NEW', 0)    # new listing, no EOD yet
        add_symbol(db, 'LIV', 1)    # already current: not a candidate
        add_symbol(db, 'GONE', 0)   # not on the official list, no EOD: never probed
        add_symbol(db, 'VAS', 0, name='VNGD AUS SHARES ETF UNITS')  # ETF: never on the list, traded recently
        add_symbol(db, 'OLDETF', 0)  # off the list, last traded 2024: not probed
        add_symbol(db, 'DEAD', 0)   # judged delisted: not probed even though on the list
        add_eod(db, 'VAS', '2026-10-01', close=100.0)
        add_eod(db, 'OLDETF', '2024-01-02')
        add_eod(db, 'DEAD', '2025-01-02')
        db.execute("UPDATE symbols SET delisted_on = '2025-01-02' WHERE symbol = 'DEAD'")
        add_symbol(db, 'WMCO', 0, name='WILUNA OPTION EXP')  # options excluded
        add_eod(db, 'WMC', '2022-12-30', close=0.205)
        db.execute("INSERT INTO eod_fetch_failures VALUES ('WMC', 6, '2026-08-31', '2026-10-13', 'ASX')")
        db.commit()

        calls = []
        yahoo = {
            'WMC': [('2026-10-22', 0.56, 3263260), ('2026-10-27', 0.48, 1234585), ('2026-10-28', 0.50, 1164110)],
            'SUS': [('2026-10-27', 0.10, 0)],
            'NEW': [('2026-10-27', 1.00, 5000)],
            'VAS': [('2026-10-27', 101.0, 20000)],
            'OLDETF': [('2026-10-27', 1.00, 100)],
            'DEAD': [('2026-10-27', 1.00, 100)],
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
        assert current(db, 'VAS') == 1     # ETF switched off by misses comes back
        assert current(db, 'OLDETF') == 0
        assert current(db, 'DEAD') == 0
        assert months == {'2026-10'}
        wmc = [r[0] for r in db.execute("SELECT close FROM endofday WHERE symbol='WMC' ORDER BY date")]
        assert wmc == [0.205, 0.56, 0.48, 0.50]
        assert db.execute("SELECT COUNT(*) FROM eod_fetch_failures WHERE symbol='WMC'").fetchone()[0] == 0
        probed = setup[0][0]
        # listed or recently traded, switched off, not delisted, not options
        assert probed == ['NEW', 'SUS', 'VAS', 'WMC']

    def test_backfill_starts_after_earliest_last_eod(self, db, setup):
        fetch_eod_daily.reenable_listed(db, datetime.date(2026, 10, 28), delay=0)
        backfill = setup[1]
        assert backfill[0] == ['NEW', 'VAS', 'WMC']
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


OFFICIAL = {'LIST', 'SUSP', 'BACK'}


class TestUpdateDelisted:
    def test_rules(self, db):
        add_symbol(db, 'GONE', 0)     # off the list, last traded 2026-03-02: delisted
        add_eod(db, 'GONE', '2026-03-02')
        add_symbol(db, 'ETF', 1)      # off the list (ETFs never on it), trading
        add_eod(db, 'ETF', '2026-10-27')
        add_symbol(db, 'SUSP', 0)     # on the list, suspended for months: not delisted
        add_eod(db, 'SUSP', '2025-06-02')
        add_symbol(db, 'NOEOD', 0)    # never traded: nothing to date it by
        add_symbol(db, 'RENAMED', 0)  # renamed away right after its last trade
        add_eod(db, 'RENAMED', '2026-01-05')
        add_change(db, 'RENAMED', 'NEWNAME', '2026-01-12')
        newly, cleared = symbol_status.update_delisted(db, OFFICIAL, TODAY)
        assert (newly, cleared) == (['GONE'], [])
        delisted = dict(db.execute('SELECT symbol, delisted_on FROM symbols WHERE delisted_on IS NOT NULL'))
        assert delisted == {'GONE': '2026-03-02'}

    def test_boundary_is_ninety_days(self, db):
        add_symbol(db, 'AT90', 0)
        add_eod(db, 'AT90', '2026-07-30')   # 90 days before 2026-10-28
        add_symbol(db, 'AT89', 0)
        add_eod(db, 'AT89', '2026-07-31')
        assert symbol_status.update_delisted(db, OFFICIAL, TODAY) == (['AT90'], [])

    def test_cleared_when_back_on_list_or_trading(self, db):
        add_symbol(db, 'BACK', 0)     # code reissued: on the list again
        add_eod(db, 'BACK', '2020-01-02')
        add_symbol(db, 'TRADES', 1)   # off-list code trading again
        add_eod(db, 'TRADES', '2026-10-27')
        add_symbol(db, 'STILL', 0)
        add_eod(db, 'STILL', '2020-01-02')
        db.execute("UPDATE symbols SET delisted_on = '2020-01-02'")
        assert symbol_status.update_delisted(db, OFFICIAL, TODAY) == ([], ['BACK', 'TRADES'])
        assert db.execute("SELECT delisted_on FROM symbols WHERE symbol = 'STILL'").fetchone()[0] == '2020-01-02'

    def test_switches_off(self, db):
        add_symbol(db, 'GONE', 1)
        add_eod(db, 'GONE', '2026-03-02')
        symbol_status.update_delisted(db, OFFICIAL, TODAY)
        assert current(db, 'GONE') == 0

    def test_column_added_to_old_schema(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE symbols (symbol TEXT PRIMARY KEY, current INTEGER)')
        symbol_status.ensure_delisted_column(conn)
        symbol_status.ensure_delisted_column(conn)   # idempotent
        assert 'delisted_on' in {r[1] for r in conn.execute('PRAGMA table_info(symbols)')}


class TestRetireSymbols:
    NOW = ts('2026-10-28') + 18 * 3600   # 18:00 local, after the 17:00 fetch

    def failures(self, db):
        return dict(db.execute('SELECT symbol, consecutive_misses FROM eod_fetch_failures'))

    def test_reports_only_codes_switched_off_this_run(self, db):
        add_symbol(db, 'LIVE', 1)
        add_eod(db, 'LIVE', '2026-10-28')
        add_symbol(db, 'DYING', 1)       # 9 misses so far, 10th today
        add_eod(db, 'DYING', '2026-10-14')
        add_symbol(db, 'OLDOFF', 0)      # switched off long ago: stale row from back then
        add_symbol(db, 'ZOMBIE', 1)      # switched back on by something, no trade for months
        add_eod(db, 'ZOMBIE', '2026-01-20')
        db.executemany('INSERT INTO eod_fetch_failures VALUES (?, ?, ?, ?, ?)', [
            ('DYING', 9, '2026-10-15', '2026-10-27', 'ASX'),
            ('OLDOFF', 74, '2026-03-30', '2026-06-10', 'ASX'),
            ('ZOMBIE', 70, '2026-03-30', '2026-10-27', 'ASX'),
        ])
        out = fetch_eod_daily.retire_symbols(db, {'DYING', 'ZOMBIE'}, {'LIVE'},
                                             datetime.date(2026, 10, 28), True, now_ts=self.NOW)
        # ZOMBIE goes quietly via the 90-day rule; only DYING is news
        assert out == {'no_trade': ['ZOMBIE'], 'misses': [('DYING', 10, '2026-10-15')]}
        assert current(db, 'DYING') == 0 and current(db, 'ZOMBIE') == 0 and current(db, 'LIVE') == 1
        assert self.failures(db) == {}   # rows for switched-off codes are dropped

    def test_illiquid_code_that_traded_this_week_kept(self, db):
        add_symbol(db, 'THIN', 1)
        add_eod(db, 'THIN', '2026-10-23')
        db.execute("INSERT INTO eod_fetch_failures VALUES ('THIN', 12, '2026-10-12', '2026-10-27', 'ASX')")
        out = fetch_eod_daily.retire_symbols(db, {'THIN'}, set(), datetime.date(2026, 10, 28), True,
                                             now_ts=self.NOW)
        assert out == {'no_trade': [], 'misses': []}
        assert current(db, 'THIN') == 1 and self.failures(db) == {'THIN': 13}

    def test_rerun_same_day_does_not_count_twice(self, db):
        add_symbol(db, 'DYING', 1)
        add_eod(db, 'DYING', '2026-10-14')
        db.execute("INSERT INTO eod_fetch_failures VALUES ('DYING', 5, '2026-10-21', '2026-10-27', 'ASX')")
        out = fetch_eod_daily.retire_symbols(db, {'DYING'}, set(), datetime.date(2026, 10, 27), False,
                                             now_ts=self.NOW)
        assert out == {'no_trade': [], 'misses': []} and self.failures(db) == {'DYING': 5}
