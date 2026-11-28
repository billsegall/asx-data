# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Commodity statistics used by commodity alerts and the digest."""
import datetime
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'stockdb'))
import commodity_stats as cs  # noqa: E402

UTC = datetime.timezone.utc


def ts(iso):
    return int(datetime.datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp())


def daily(start, end, price_fn):
    """Weekday rows from start to end (inclusive)."""
    d, out, i = datetime.date.fromisoformat(start), [], 0
    while d <= datetime.date.fromisoformat(end):
        if d.weekday() < 5:
            out.append((ts(d.isoformat()), price_fn(i)))
            i += 1
        d += datetime.timedelta(days=1)
    return out


def weekly(start, end, price_fn):
    d, out, i = datetime.date.fromisoformat(start), [], 0
    while d <= datetime.date.fromisoformat(end):
        out.append((ts(d.isoformat()), price_fn(i)))
        i += 1
        d += datetime.timedelta(days=7)
    return out


def test_short_weekly_history_reports_nothing_long_term():
    # Like lithium: weekly since April 2026
    s = cs.stats(weekly('2026-04-10', '2026-11-25', lambda i: 150000 - i * 2000))
    assert s['cadence'] == 'weekly'
    assert s['changes']['365'] is None
    assert s['changes']['1'] is None           # weekly: no 1-day change
    assert s['high_52w'] is None and s['low_52w'] is None
    assert s['ma200'] is None
    assert s['changes']['182'] is not None     # ~7.5 months of data covers 6m
    assert s['changes']['7']['pct'] < 0
    assert s['ma50'] is not None               # 10 weeks of data


def test_long_daily_history_reports_everything():
    s = cs.stats(daily('2024-01-01', '2026-11-27', lambda i: 100 + i * 0.1))
    assert s['cadence'] == 'daily'
    assert all(s['changes'][str(p)] is not None for p in cs.PERIODS)
    # rising series: latest above every earlier point of the year
    assert s['price'] > s['high_52w']
    assert s['ma50'] < s['price'] and s['ma200'] < s['ma50']


def test_twelve_month_change_value():
    rows = [(ts('2025-11-27'), 100.0), (ts('2026-05-28'), 80.0), (ts('2026-11-27'), 70.0)]
    s = cs.stats(rows)
    assert round(s['changes']['365']['pct'], 6) == -30.0
    assert s['changes']['365']['date'] == '2025-11-27'
    assert round(s['changes']['182']['pct'], 6) == -12.5


def test_gap_in_data_skips_period():
    # Nothing within 10 days before the 3-month target
    rows = [(ts('2026-01-01'), 100.0), (ts('2026-11-27'), 50.0)]
    s = cs.stats(rows)
    assert s['changes']['91'] is None


def test_zero_and_duplicate_points_ignored():
    rows = [(ts('2026-11-20'), 10.0), (ts('2026-11-25'), 0.0),
            (ts('2026-11-27'), 11.0), (ts('2026-11-27') + 3600, 12.0)]
    s = cs.stats(rows)
    assert s['price'] == 12.0 and s['points'] == 2 and s['date'] == '2026-11-27'


def test_empty():
    assert cs.stats([]) is None
    assert cs.stats([(ts('2026-11-27'), 0.0)]) is None
