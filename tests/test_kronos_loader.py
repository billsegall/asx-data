# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Kronos evaluation-date selection."""
import calendar
import datetime
import os
import sqlite3
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from analysis.kronos.loader import get_actual_5d_returns, get_evaluation_dates


def _aest_ts(day):
    """How endofday stores a trading day: midnight AEST, i.e. 14:00 UTC the day before."""
    return calendar.timegm(day.timetuple()) - 10 * 3600


def _db(tmp_path, n_days):
    """endofday with one row per weekday, n_days of them from Mon 2 Mar 2026."""
    path = str(tmp_path / 'stock.db')
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE endofday (symbol TEXT, date INTEGER, close REAL)')
    day, dates = datetime.date(2026, 3, 2), []
    while len(dates) < n_days:
        if day.weekday() < 5:
            dates.append(day)
        day += datetime.timedelta(days=1)
    conn.executemany('INSERT INTO endofday VALUES (?, ?, 1.0)',
                     [('BHP', _aest_ts(d)) for d in dates])
    conn.commit()
    conn.close()
    return path, [d.isoformat() for d in dates]


def test_sampling_without_forward_limit(tmp_path):
    path, dates = _db(tmp_path, 23)
    assert get_evaluation_dates(path, start='2026-03-01', end='2026-12-31') == dates[::5]


def test_min_forward_keeps_every_date_with_enough_days_after(tmp_path):
    # 23 trading days, indexes 0..22. Samples at 0,5,10,15,20. With 5 days
    # needed after, index 17 is the last usable day, so 15 is the last sample
    # (it has 7 days after). Only 20 drops — not the last 5 samples.
    path, dates = _db(tmp_path, 23)
    got = get_evaluation_dates(path, start='2026-03-01', end='2026-12-31', min_forward=5)
    assert got == [dates[i] for i in (0, 5, 10, 15)]


def test_min_forward_boundary_exactly_enough_days(tmp_path):
    # 21 days: index 15 has exactly 5 after it (16..20) — kept; 20 has none.
    path, dates = _db_fresh(tmp_path, 21)
    got = get_evaluation_dates(path, start='2026-03-01', end='2026-12-31', min_forward=5)
    assert got == [dates[i] for i in (0, 5, 10, 15)]
    # 20 days: index 15 has only 4 after it — dropped.
    path, dates = _db_fresh(tmp_path, 20)
    got = get_evaluation_dates(path, start='2026-03-01', end='2026-12-31', min_forward=5)
    assert got == [dates[i] for i in (0, 5, 10)]


def _db_fresh(tmp_path, n_days):
    sub = tmp_path / f'n{n_days}'
    sub.mkdir()
    return _db(sub, n_days)


def test_samples_stay_aligned_as_data_grows(tmp_path):
    # Resume caches key on dates: adding days must never shift earlier samples.
    short, _ = _db_fresh(tmp_path, 23)
    long, _ = _db_fresh(tmp_path, 40)
    a = get_evaluation_dates(short, start='2026-03-01', end='2026-12-31', min_forward=5)
    b = get_evaluation_dates(long, start='2026-03-01', end='2026-12-31', min_forward=5)
    assert b[:len(a)] == a and len(b) > len(a)


def test_actual_returns_over_horizon(tmp_path):
    path = str(tmp_path / 'stock.db')
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE endofday (symbol TEXT, date INTEGER, close REAL)')
    day, rows = datetime.date(2026, 3, 2), []
    while len(rows) < 12:
        if day.weekday() < 5:
            rows.append(('BHP', _aest_ts(day), 10.0 + len(rows)))
        day += datetime.timedelta(days=1)
    conn.executemany('INSERT INTO endofday VALUES (?, ?, ?)', rows)
    conn.commit()
    conn.close()
    # Closes 10, 11, 12, ... on consecutive trading days
    assert get_actual_5d_returns(path, '2026-03-02', ['BHP']) == {'BHP': 0.5}
    assert get_actual_5d_returns(path, '2026-03-02', ['BHP'], horizon=10) == {'BHP': 1.0}
    # 11 is the last horizon with a close after it (12 rows: t0 + 11)
    assert get_actual_5d_returns(path, '2026-03-02', ['BHP'], horizon=11) == {'BHP': 1.1}
    assert get_actual_5d_returns(path, '2026-03-02', ['BHP'], horizon=12) == {}


@pytest.fixture(params=['UTC', 'Australia/Brisbane', 'America/New_York'])
def machine_tz(request, monkeypatch):
    monkeypatch.setenv('TZ', request.param)
    time.tzset()
    yield request.param
    monkeypatch.undo()
    time.tzset()


def test_dates_are_trading_days_in_any_machine_timezone(tmp_path, machine_tz):
    # Rows are AEST midnight; read as UTC they used to come back as the day
    # before (Mon 2 Mar labelled Sun 1 Mar), so forecasts stopped at Friday's
    # close while returns were measured from Monday's.
    path, dates = _db(tmp_path, 23)
    got = get_evaluation_dates(path, start='2026-03-02', end='2026-12-31', min_forward=5)
    assert got[0] == '2026-03-02' == dates[0]
    assert all(datetime.date.fromisoformat(d).weekday() < 5 for d in got)
    # start/end bound by trading day, not by the machine's midnight
    assert get_evaluation_dates(path, start='2026-03-03', end='2026-03-03') == ['2026-03-03']


def test_actual_return_starts_at_eval_days_close(tmp_path, machine_tz):
    path = str(tmp_path / 'stock.db')
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE endofday (symbol TEXT, date INTEGER, close REAL)')
    # Fri 27 Feb close 8, then Mon 2 Mar .. closes 10, 11, ...
    rows = [('BHP', _aest_ts(datetime.date(2026, 2, 27)), 8.0)]
    day = datetime.date(2026, 3, 2)
    while len(rows) < 8:
        if day.weekday() < 5:
            rows.append(('BHP', _aest_ts(day), 10.0 + len(rows) - 1))
        day += datetime.timedelta(days=1)
    conn.executemany('INSERT INTO endofday VALUES (?, ?, ?)', rows)
    conn.commit()
    conn.close()
    # Monday close 10 → 5 trading days later close 15
    assert get_actual_5d_returns(path, '2026-03-02', ['BHP']) == {'BHP': 0.5}
