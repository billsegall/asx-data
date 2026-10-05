# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Map an announcement's timestamp to a trading-day column index in a
FeatureMatrix, and find the first genuinely tradeable ("valid") day at or
after that nominal day — these are deliberately NOT the same thing.

FeatureMatrix.mask is `close non-NaN & volume > 0`. A halted stock FAILS
that mask on the halt day itself, so naively requiring mask[t0]==True at
the nominal announcement date would silently drop most trading_halt
events — exactly the cell this study cares about most.

Timezone note: endofday.date is mostly unix seconds encoding a FIXED
UTC+10 midnight per trading day, but NOT uniformly -- a minority of rows
(confirmed live, ~3-4% in a spot-checked window) are stamped one hour off
this, apparently from a source feed that used Sydney-local (AEDT, UTC+11
during daylight saving) rather than a flat UTC+10 ("Brisbane") convention
at ingestion time. Rendered through a flat +10h offset, those rows land
on the WRONG calendar day (one day early), producing what looks like two
distinct FeatureMatrix columns for what is really one trading day (e.g.
a spurious "2026-03-01" (a Sunday) column sitting next to the real
"2026-03-02"). This is upstream in stockdb's endofday data, not something
to fix here -- `collapse_to_calendar_days()` below merges same-calendar-day
columns before this module does anything alignment-sensitive with them, so
every downstream trading-day count (t0_lag_days, forward-return horizons)
is exact regardless of the underlying timestamp inconsistency.

announced_at is already a naive Sydney-local date/time STRING (ASX
publishes in local time) — we only ever slice its date prefix, never
convert it, so Sydney's daylight saving is irrelevant to that side: we're
not doing arithmetic on it, just comparing calendar-date labels. Hardcoding
+10 (not relying on the executing machine's local timezone) keeps the
*endofday* side correct whether it runs on harri or realiti.
"""

import datetime

import numpy as np
import torch

BRISBANE_OFFSET_SECONDS = 10 * 3600
MARKET_CLOSE_HOUR = 16  # ASX closes 4pm local


def date_strs(dates: np.ndarray) -> np.ndarray:
    """fm.dates (unix seconds, UTC+10-encoded) -> array of 'YYYY-MM-DD' strings.

    ASX never trades weekends, so any column that renders to a Saturday or
    Sunday is necessarily a stamping artifact (confirmed: the AEDT-vs-flat-
    UTC+10 mismatch described in this module's docstring always shifts a
    timestamp EARLIER by up to an hour, never later, so a weekend render
    always means the intended day was the next trading day, never the
    previous one). Rolled forward here so collapse_to_calendar_days() merges
    it into the real Monday/next-trading-day column instead of leaving a
    phantom weekend column that inflates lag/horizon counts by one.
    """
    out = []
    for t in dates:
        d = datetime.datetime.utcfromtimestamp(int(t) + BRISBANE_OFFSET_SECONDS).date()
        if d.weekday() == 5:    # Saturday -> Monday
            d += datetime.timedelta(days=2)
        elif d.weekday() == 6:  # Sunday -> Monday
            d += datetime.timedelta(days=1)
        out.append(d.isoformat())
    return np.array(out)


def collapse_to_calendar_days(close: torch.Tensor, mask: torch.Tensor, dates: np.ndarray):
    """Merge FeatureMatrix columns that render to the same calendar day.

    Returns (close2, mask2, dates2, date_strs2) with exactly one column per
    distinct calendar day, so every downstream trading-day count (lag,
    horizon shift) this module computes is exact. For symbols with a valid
    value on more than one source column sharing a day (rare), the last one
    wins -- matching FeatureMatrix._build_pivots' own "keep last" dedup
    convention for duplicate (date, symbol) rows.
    """
    ds = date_strs(dates)
    uniq, inverse = np.unique(ds, return_inverse=True)
    N, T = close.shape
    T2 = len(uniq)
    inv_t = torch.as_tensor(inverse, dtype=torch.long)

    close2 = torch.full((N, T2), float('nan'), dtype=close.dtype, device=close.device)
    mask2 = torch.zeros((N, T2), dtype=torch.bool, device=close.device)
    dates2 = np.empty(T2, dtype=dates.dtype)
    seen = np.zeros(T2, dtype=bool)

    for t in range(T):
        u = int(inverse[t])
        col_mask = mask[:, t]
        if col_mask.any():
            close2[col_mask, u] = close[col_mask, t]
            mask2[:, u] |= col_mask
        if not seen[u]:
            dates2[u] = dates[t]
            seen[u] = True

    return close2, mask2, dates2, uniq


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


def find_last_valid(mask_row, nominal_idx: int, max_lag: int = 10):
    """Last valid index strictly before nominal_idx, scanning back up to
    max_lag days — the last known-good price before a halt/event started.

    A multi-day halt means `nominal_idx - 1` is itself invalid (still
    halted), so a naive "check one column back" undershoots exactly the
    events this diagnostic is meant to cover. Returns (idx, lag) or
    (None, None).
    """
    lower = max(nominal_idx - max_lag, 0)
    for idx in range(nominal_idx - 1, lower - 1, -1):
        if bool(mask_row[idx]):
            return idx, nominal_idx - idx
    return None, None
