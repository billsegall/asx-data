# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""fetch_eod_daily: lookback upsert, session cutoff, miss tracking, endofmonth months."""
import datetime
import os
import sqlite3
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'stockdb'))
import fetch_eod_daily  # noqa: E402

SYD = fetch_eod_daily.SYDNEY


def ts(d):
    d = datetime.date.fromisoformat(d)
    return time.mktime(datetime.datetime(d.year, d.month, d.day).timetuple())


@pytest.fixture
def dbpath(tmp_path):
    p = tmp_path / 'stock.db'
    conn = sqlite3.connect(p)
    conn.executescript('''
        CREATE TABLE symbols (symbol TEXT PRIMARY KEY, name TEXT, industry TEXT, shares REAL,
                              current INTEGER NOT NULL DEFAULT 1, exchange TEXT NOT NULL DEFAULT 'ASX');
        CREATE TABLE endofday (symbol TEXT, date NUM, open REAL, high REAL, low REAL,
                               close REAL, volume INT, exchange TEXT NOT NULL DEFAULT 'ASX');
        CREATE UNIQUE INDEX idx_endofday_symbol_date ON endofday(symbol, date);
        CREATE TABLE endofmonth (symbol TEXT, date NUM, close REAL, exchange TEXT NOT NULL DEFAULT 'ASX');
        INSERT INTO symbols (symbol, name) VALUES ('AAA', 'AAA LTD'), ('BBB', 'BBB LTD'),
                                                  ('AAAO', 'AAA OPTION EXP');
    ''')
    conn.commit()
    conn.close()
    return str(p)


def add(db, sym, d, close, vol=1000):
    db.execute('INSERT INTO endofday (symbol, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)',
               (sym, ts(d), close, close, close, close, vol))


def rows(db, sym):
    return [(datetime.date.fromtimestamp(d).isoformat(), c, v) for d, c, v in
            db.execute('SELECT date, close, volume FROM endofday WHERE symbol=? ORDER BY date', (sym,))]


class TestLastCompleteDay:
    def test_before_and_after_close(self):
        f = fetch_eod_daily.last_complete_day
        assert f(datetime.datetime(2026, 11, 9, 16, 29, tzinfo=SYD)) == datetime.date(2026, 11, 8)
        assert f(datetime.datetime(2026, 11, 9, 16, 30, tzinfo=SYD)) == datetime.date(2026, 11, 9)
        assert f(datetime.datetime(2026, 11, 9, 0, 5, tzinfo=SYD)) == datetime.date(2026, 11, 8)


class TestMain:
    YAHOO = {
        # (sym, date) -> close, volume
        ('AAA', '2026-11-02'): (1.00, 500),
        ('AAA', '2026-11-03'): (1.10, 600),   # Melbourne Cup: ASX open, normal day
        ('AAA', '2026-11-05'): (1.20, 700),   # stored as a part-day bar
        ('AAA', '2026-11-06'): (1.30, 800),
        ('AAA', '2026-11-07'): (1.30, 0),     # Saturday bar: must be dropped
        ('BBB', '2026-11-04'): (2.00, 100),   # missed on its night
        ('BBB', '2026-11-06'): (2.10, 100),
        ('BBB', '2026-11-03'): (2.00, 0),     # flat filler for an old day: not inserted
        ('CCC', '2026-11-06'): (3.00, 0),     # zero volume on a new day: kept, as before
    }

    @pytest.fixture
    def run(self, dbpath, monkeypatch):
        calls = []

        def fake_download(tickers, reverse_map, start_dt, end_dt):
            calls.append((sorted(tickers), start_dt, end_dt))
            out = []
            for (sym, d), (cl, v) in self.YAHOO.items():
                day = datetime.date.fromisoformat(d)
                if fetch_eod_daily.exchange_yf_ticker(sym) in tickers and start_dt <= day < end_dt \
                        and fetch_eod_daily.is_trading_day(day):
                    out.append((sym, ts(d), cl, cl, cl, cl, v))
            return out

        monkeypatch.setattr(fetch_eod_daily, 'download_rows', fake_download)
        monkeypatch.setattr(fetch_eod_daily, 'last_complete_day', lambda now=None: datetime.date(2026, 11, 9))

        def go(*extra):
            monkeypatch.setattr(sys, 'argv', ['fetch_eod_daily.py', '--db', dbpath, '--delay', '0',
                                              '--no-reenable', *extra])
            fetch_eod_daily.main()
            return sqlite3.connect(dbpath)
        return go, calls

    def seed(self, dbpath):
        db = sqlite3.connect(dbpath)
        add(db, 'AAA', '2026-10-30', 0.90)
        add(db, 'AAA', '2026-11-02', 1.00, 500)
        add(db, 'AAA', '2026-11-03', 1.10, 600)
        add(db, 'AAA', '2026-11-05', 1.15, 300)   # part-day
        add(db, 'BBB', '2026-11-05', 2.05, 100)
        add(db, 'AAAO', '2026-11-04', 0.01, 10)   # option row: must survive
        db.execute("INSERT INTO symbols (symbol, name) VALUES ('CCC', 'CCC LTD')")
        add(db, 'CCC', '2026-11-05', 3.00)
        db.execute("CREATE TABLE eod_fetch_failures (symbol TEXT PRIMARY KEY, consecutive_misses INTEGER NOT NULL"
                   " DEFAULT 0, first_miss_date TEXT NOT NULL, last_miss_date TEXT NOT NULL,"
                   " exchange TEXT NOT NULL DEFAULT 'ASX')")
        db.commit()
        db.close()

    def test_lookback_inserts_missed_and_fixes_part_day(self, dbpath, run):
        self.seed(dbpath)
        go, calls = run
        db = go()
        assert rows(db, 'AAA') == [('2026-10-30', 0.90, 1000), ('2026-11-02', 1.00, 500),
                                   ('2026-11-03', 1.10, 600), ('2026-11-05', 1.20, 700),
                                   ('2026-11-06', 1.30, 800)]
        assert rows(db, 'BBB') == [('2026-11-04', 2.00, 100), ('2026-11-05', 2.05, 100),
                                   ('2026-11-06', 2.10, 100)]
        assert rows(db, 'CCC') == [('2026-11-05', 3.00, 1000), ('2026-11-06', 3.00, 0)]
        assert rows(db, 'AAAO') == [('2026-11-04', 0.01, 10)]
        # window: last 7 days up to the last complete day, options not fetched
        tickers, start, end = calls[0]
        assert start == datetime.date(2026, 11, 2) and end == datetime.date(2026, 11, 10)
        assert tickers == ['AAA.AX', 'BBB.AX', 'CCC.AX']

    def test_starts_at_last_stored_day_when_older(self, dbpath, run):
        db = sqlite3.connect(dbpath)
        add(db, 'AAA', '2026-10-20', 0.80)
        db.commit()
        go, calls = run
        go()
        assert calls[0][1] == datetime.date(2026, 10, 21)

    def test_rerun_changes_nothing_and_counts_miss_once(self, dbpath, run):
        self.seed(dbpath)
        db0 = sqlite3.connect(dbpath)
        db0.execute("INSERT INTO symbols (symbol, name) VALUES ('DDD', 'DDD LTD')")
        add(db0, 'DDD', '2026-11-02', 3.0)
        db0.commit()
        go, _ = run
        db = go()
        before = db.execute('SELECT * FROM endofday ORDER BY symbol, date').fetchall()
        miss = db.execute('SELECT symbol, consecutive_misses, last_miss_date FROM eod_fetch_failures').fetchall()
        assert miss == [('DDD', 1, '2026-11-06')]
        db = go()
        assert db.execute('SELECT * FROM endofday ORDER BY symbol, date').fetchall() == before
        assert db.execute('SELECT symbol, consecutive_misses FROM eod_fetch_failures').fetchall() == [('DDD', 1)]

    def test_only_latest_day_resets_miss_counter(self, dbpath, run):
        self.seed(dbpath)
        db0 = sqlite3.connect(dbpath)
        db0.execute("INSERT INTO eod_fetch_failures VALUES ('BBB', 3, '2026-10-01', '2026-11-03', 'ASX')")
        db0.execute("INSERT INTO eod_fetch_failures VALUES ('AAA', 2, '2026-10-01', '2026-11-03', 'ASX')")
        db0.commit()
        self.YAHOO = {k: v for k, v in TestMain.YAHOO.items() if k != ('BBB', '2026-11-06')}
        go, _ = run
        db = go()
        # AAA traded on the latest day (6th): reset. BBB only earlier in the window: still missing.
        assert db.execute('SELECT symbol, consecutive_misses FROM eod_fetch_failures').fetchall() == [('BBB', 4)]

    def test_endofmonth_uses_local_month(self, dbpath, run):
        db = sqlite3.connect(dbpath)
        add(db, 'AAA', '2026-10-30', 0.90)
        add(db, 'AAA', '2026-11-02', 1.00, 500)
        db.commit()
        go, _ = run
        db = go()
        eom = [(datetime.date.fromtimestamp(d).isoformat(), c) for d, c in
               db.execute("SELECT date, close FROM endofmonth WHERE symbol='AAA' ORDER BY date")]
        # Only November was touched; its month-end is the 6th, never a day in another month
        assert eom == [('2026-11-06', 1.30)]


def test_refresh_endofmonth_first_of_month_stays_in_its_month(dbpath):
    db = sqlite3.connect(dbpath)
    add(db, 'AAA', '2026-09-29', 1.0)
    add(db, 'AAA', '2026-09-30', 1.1)
    add(db, 'AAA', '2026-10-01', 1.2)   # local midnight is 30 Sep in UTC
    add(db, 'AAA', '2026-10-02', 1.3)
    fetch_eod_daily.refresh_endofmonth(db.cursor(), {'2026-09', '2026-10'})
    eom = [(datetime.date.fromtimestamp(d).isoformat(), c) for d, c in
           db.execute("SELECT date, close FROM endofmonth ORDER BY date")]
    assert eom == [('2026-09-30', 1.1), ('2026-10-02', 1.3)]


def test_download_rows_unadjusted_and_drops_non_trading_days(monkeypatch):
    import pandas as pd
    idx = pd.to_datetime(['2026-11-06', '2026-11-07', '2026-11-09'])
    cols = pd.MultiIndex.from_tuples([(f, 'AAA.AX') for f in ('Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume')])
    df = pd.DataFrame([[1, 2, 0.5, 1.5, 1.4, 100], [1.5, 1.5, 1.5, 1.5, 1.4, 0], [9, 9, 9, 9, 9, 9]], index=idx, columns=cols)
    seen = {}

    def fake(tickers, **kw):
        seen.update(kw)
        return df
    monkeypatch.setattr(fetch_eod_daily.yf, 'download', fake)
    out = fetch_eod_daily.download_rows(['AAA.AX'], {'AAA.AX': 'AAA'},
                                        datetime.date(2026, 11, 6), datetime.date(2026, 11, 8))
    assert seen['auto_adjust'] is False   # and the live bar for the 9th (past end) is dropped
    assert out == [('AAA', ts('2026-11-06'), 1.0, 2.0, 0.5, 1.5, 100)]
