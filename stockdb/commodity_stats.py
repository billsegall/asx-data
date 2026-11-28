# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Per-commodity statistics for alerts and the digest.

Pure functions over a commodity's price points, so the backend endpoint
(/api/commodities/stats) and its tests share one implementation.

Every statistic is None when the series can't support it. The scraped
weekly series (Trading Economics, manganese) only start in April 2026, so a
12-month change, a 52-week high/low or a 200-day average for them would
otherwise be computed from a few months of data and fire "all commodities"
alerts spuriously.
"""
import datetime
import statistics

# % change periods, in calendar days (1d, 1w, 1m, 3m, 6m, 12m)
PERIODS = (1, 7, 30, 91, 182, 365)
# The reference point for a period must lie within this many days of the
# target date (latest - period); otherwise there's a gap in the data and the
# change isn't reported.
MAX_REF_GAP = 10
# Moving averages over 50 and 200 trading days (70 and 280 calendar days),
# needing at least this many points in the window (a weekly series has 10)
MA_DAYS = (50, 200)
MIN_MA_POINTS = 8
# Fewer points than this in the last year and the 52-week range is skipped
MIN_52W_POINTS = 26
LOOKBACK_DAYS = 420   # history the endpoint needs: 365 + gap + slack


def _iso(d):
    return d.isoformat()


def series(rows):
    """[(date, price)] sorted, one point per date (the last wins), dropping
    non-positive prices. rows are (unix_ts, price)."""
    by_date = {}
    for ts, price in sorted(rows):
        if price is None or price <= 0:
            continue
        by_date[datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).date()] = price
    return sorted(by_date.items())


def cadence(points):
    """'daily' or 'weekly' from the median gap between the last 10 points."""
    tail = points[-11:]
    gaps = [(b[0] - a[0]).days for a, b in zip(tail, tail[1:])]
    if not gaps:
        return 'daily'
    return 'weekly' if statistics.median(gaps) > 3 else 'daily'


def reference(points, latest_date, days):
    """(date, price) of the last point at or before latest - days, or None if
    there's no such point or it's more than MAX_REF_GAP days early."""
    target = latest_date - datetime.timedelta(days=days)
    ref = None
    for d, p in points:
        if d > target:
            break
        ref = (d, p)
    if ref is None or (target - ref[0]).days > MAX_REF_GAP:
        return None
    return ref


def moving_average(points, days):
    """Mean of the points in the last `days` trading days (days * 7/5
    calendar days, so it works for daily, twice-weekly and weekly series).
    None unless the history covers the whole window with MIN_MA_POINTS."""
    latest = points[-1][0]
    start = latest - datetime.timedelta(days=round(days * 7 / 5))
    if points[0][0] > start + datetime.timedelta(days=MAX_REF_GAP):
        return None
    window = [p for d, p in points if d > start]
    if len(window) < MIN_MA_POINTS:
        return None
    return sum(window) / len(window)


def stats(rows):
    """Statistics for one commodity from its (unix_ts, price) rows, or None
    if there are no usable points."""
    points = series(rows)
    if not points:
        return None
    kind = cadence(points)
    latest_date, latest = points[-1]
    changes = {}
    for days in PERIODS:
        ref = reference(points, latest_date, days)
        # A weekly series has no 1-day change: its previous point is a week old
        if ref is None or (days == 1 and kind == 'weekly'):
            changes[str(days)] = None
            continue
        changes[str(days)] = {'date': _iso(ref[0]), 'price': ref[1],
                              'pct': (latest - ref[1]) / ref[1] * 100}
    year_ago = latest_date - datetime.timedelta(days=365)
    prior = [p for d, p in points[:-1] if d > year_ago]
    full_year = points[0][0] <= year_ago + datetime.timedelta(days=MAX_REF_GAP)
    have_range = full_year and len(prior) >= MIN_52W_POINTS
    return {
        'cadence': kind,
        'date': _iso(latest_date),
        'price': latest,
        'data_start': _iso(points[0][0]),
        'points': len(points),
        'changes': changes,
        # Range of the year before the latest point, excluding it, so "new
        # 52-week high" means the latest price is above every earlier one
        'high_52w': max(prior) if have_range else None,
        'low_52w': min(prior) if have_range else None,
        'ma50': moving_average(points, 50),
        'ma200': moving_average(points, 200),
    }
