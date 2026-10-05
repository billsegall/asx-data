# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Map an announcement's timestamp to a trading-day column index in a
FeatureMatrix, and find the first genuinely tradeable ("valid") day at or
after that nominal day — these are deliberately NOT the same thing.

FeatureMatrix.mask is `close non-NaN & volume > 0`. A halted stock FAILS
that mask on the halt day itself, so naively requiring mask[t0]==True at
the nominal announcement date would silently drop most trading_halt
events — exactly the cell this study cares about most.

Timezone note: endofday.date is unix seconds encoding a FIXED UTC+10
("Brisbane time, no DST") midnight for each trading day — confirmed
empirically (MAX(date)=1790863200 == 2026-10-02 00:00:00 in UTC+10,
regardless of the machine's own local timezone). announced_at is already
a naive Sydney-local date/time STRING (ASX publishes in local time) — we
only ever slice its date prefix, never convert it, so Sydney's daylight
saving is irrelevant here: we're not doing any arithmetic on it, just
comparing calendar-date labels. Hardcoding +10 (not relying on the
executing machine's local timezone) keeps this correct whether it runs
on harri or realiti.
"""

import datetime

import numpy as np

BRISBANE_OFFSET_SECONDS = 10 * 3600
MARKET_CLOSE_HOUR = 16  # ASX closes 4pm local


def date_strs(dates: np.ndarray) -> np.ndarray:
    """fm.dates (unix seconds, UTC+10-encoded) -> sorted array of 'YYYY-MM-DD' strings."""
    return np.array([
        datetime.datetime.utcfromtimestamp(int(t) + BRISBANE_OFFSET_SECONDS).strftime('%Y-%m-%d')
        for t in dates
    ])


def nearest_trading_day_index(date_strs: np.ndarray, announced_at: str) -> int | None:
    """Index of the first trading day at/after the announcement's effective date.

    `date_strs` must be the sorted output of `_date_strs(fm.dates)`. If the
    announcement's time-of-day is at/after market close (16:00), it's
    treated as effective the next trading day (news released after close
    can't have moved that day's own closing price). Weekends/holidays
    aren't in date_strs, so this naturally lands on the next real trading
    day for announcements made on non-trading days.

    Returns None if the target date falls outside the matrix's date range.
    """
    if not announced_at or len(announced_at) < 10:
        return None
    target_date = announced_at[:10]
    time_part = announced_at[11:16] if len(announced_at) >= 16 else ''
    if time_part and time_part >= f'{MARKET_CLOSE_HOUR:02d}:00':
        d = datetime.date.fromisoformat(target_date) + datetime.timedelta(days=1)
        target_date = d.isoformat()

    if len(date_strs) == 0 or target_date < date_strs[0]:
        return None
    idx = int(np.searchsorted(date_strs, target_date, side='left'))
    if idx >= len(date_strs):
        return None
    return idx


def find_valid_base(mask_row, nominal_idx: int, max_lag: int = 10):
    """First index in [nominal_idx, nominal_idx+max_lag] where mask_row is True.

    Returns (t0_idx, lag_days) or (None, None) if no valid day found within
    the bound — the caller should exclude the whole event in that case, not
    just one horizon.
    """
    T = mask_row.shape[0]
    upper = min(nominal_idx + max_lag, T - 1)
    for idx in range(nominal_idx, upper + 1):
        if bool(mask_row[idx]):
            return idx, idx - nominal_idx
    return None, None
