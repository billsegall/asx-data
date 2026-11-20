# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Load per-symbol OHLCV DataFrames from SQLite for Kronos inference."""

import calendar
import sqlite3
import datetime
import pandas as pd

# endofday.date is midnight AEST (14:00 UTC the day before). Brisbane has no
# daylight saving, so this converts every row to its own trading day; reading
# the timestamps as UTC labels each row with the previous calendar day.
ASX_TZ = 'Australia/Brisbane'
_AEST_OFFSET = 10 * 3600


def _local_dates(ts: pd.Series) -> pd.Series:
    """endofday unix timestamps → naive trading-day dates."""
    return (pd.to_datetime(ts, unit='s', utc=True).dt.tz_convert(ASX_TZ)
            .dt.normalize().dt.tz_localize(None))


def _local_midnight_ts(date_str: str) -> int:
    """'YYYY-MM-DD' → that trading day's endofday timestamp, whatever the machine's timezone."""
    day = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    return calendar.timegm(day.timetuple()) - _AEST_OFFSET


def _conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-131072")
    return conn


def load_all_ohlcv(
    db_path: str,
    min_days: int = 520,
    max_single_day_move: float = 0.50,
) -> dict[str, pd.DataFrame]:
    """Load OHLCV for all qualifying current symbols.

    Qualifying: current=1, ≥min_days rows, last trade ≤60 days ago.
    Each DataFrame has columns [open, high, low, close, volume] indexed by date (datetime).
    Forward-fills gaps of ≤5 consecutive missing trading days.
    Excludes symbols with any single-day close move > max_single_day_move (split noise).
    """
    cutoff_stale = datetime.date.today() - datetime.timedelta(days=60)
    cutoff_ts = int(datetime.datetime(
        cutoff_stale.year, cutoff_stale.month, cutoff_stale.day
    ).timestamp())

    with _conn(db_path) as conn:
        # Active symbols — handle both old schema (no current column) and new
        try:
            syms = pd.read_sql_query(
                "SELECT symbol FROM symbols WHERE current = 1"
                " AND name NOT LIKE '%OPTION%' AND name NOT LIKE '%WARRANT%'",
                conn,
            )['symbol'].tolist()
        except Exception:
            syms = pd.read_sql_query(
                "SELECT symbol FROM symbols"
                " WHERE name NOT LIKE '%OPTION%' AND name NOT LIKE '%WARRANT%'",
                conn,
            )['symbol'].tolist()

        if not syms:
            return {}

        ph = ','.join('?' * len(syms))
        df = pd.read_sql_query(
            f"""SELECT symbol, date, open, high, low, close, volume
                FROM endofday
                WHERE symbol IN ({ph})
                ORDER BY symbol, date""",
            conn,
            params=syms,
        )

    df['date'] = _local_dates(df['date'])

    result = {}
    for sym, grp in df.groupby('symbol', sort=False):
        grp = grp.sort_values('date').set_index('date')
        grp = grp[['open', 'high', 'low', 'close', 'volume']].copy()

        # Must be active recently
        if grp.index[-1].timestamp() < cutoff_ts:
            continue

        # Must have enough history
        if len(grp) < min_days:
            continue

        # Exclude noisy symbols (large single-day moves from splits etc.)
        rets = grp['close'].pct_change().abs()
        if (rets > max_single_day_move).any():
            continue

        result[sym] = grp

    return result


def get_evaluation_dates(
    db_path: str,
    start: str = '2025-03-01',
    end: str | None = None,
    step_days: int = 5,
    min_forward: int = 0,
) -> list[str]:
    """Return actual ASX trading dates sampled every step_days from start to end.

    min_forward drops dates with fewer than that many trading days after them,
    so a forward return can be measured. Sampling stays aligned to start, so the
    same dates come back on every run (resume caches key on them).
    """
    end_str = end or datetime.date.today().isoformat()

    start_ts = _local_midnight_ts(start)
    end_ts   = _local_midnight_ts(end_str) + 86399

    with _conn(db_path) as conn:
        dates = pd.read_sql_query(
            "SELECT DISTINCT date FROM endofday WHERE date >= ? AND date <= ? ORDER BY date",
            conn,
            params=[start_ts, end_ts],
        )

    dates['date'] = _local_dates(dates['date']).dt.strftime('%Y-%m-%d')
    all_dates = dates['date'].tolist()

    # Sample every step_days
    usable = all_dates[:len(all_dates) - min_forward] if min_forward else all_dates
    return usable[::step_days]


def get_actual_5d_returns(
    db_path: str,
    eval_date: str,
    symbols: list[str],
    horizon: int = 5,
) -> dict[str, float]:
    """Return actual forward returns over `horizon` trading days from eval_date.

    Uses the horizon-th next trading date available in endofday (not calendar
    days). Symbols missing data are omitted.
    """
    eval_ts = _local_midnight_ts(eval_date)

    with _conn(db_path) as conn:
        ph = ','.join('?' * len(symbols))
        # Get eval_date close and the horizon-th subsequent close per symbol
        df = pd.read_sql_query(
            f"""SELECT symbol, date, close
                FROM endofday
                WHERE symbol IN ({ph}) AND date >= ?
                ORDER BY symbol, date""",
            conn,
            params=symbols + [eval_ts],
        )

    result = {}
    for sym, grp in df.groupby('symbol', sort=False):
        grp = grp.sort_values('date').reset_index(drop=True)
        if len(grp) <= horizon:
            continue
        close_t0 = grp.loc[0, 'close']
        close_tn = grp.loc[horizon, 'close']
        if close_t0 and close_t0 > 0:
            result[sym] = (close_tn - close_t0) / close_t0
    return result
