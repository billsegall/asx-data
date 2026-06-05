# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Load per-symbol OHLCV DataFrames from SQLite for Kronos inference."""

import sqlite3
import datetime
import pandas as pd


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
        # Active symbols only
        syms = pd.read_sql_query(
            "SELECT symbol FROM symbols WHERE current = 1"
            " AND name NOT LIKE '%OPTION%' AND name NOT LIKE '%WARRANT%'",
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

    df['date'] = pd.to_datetime(df['date'], unit='s')

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

        # Forward-fill short gaps (≤5 days) using business-day reindex
        full_idx = pd.bdate_range(grp.index[0], grp.index[-1])
        grp = grp.reindex(full_idx)
        # Only fill if gap ≤5 consecutive NaNs
        grp = grp.fillna(method='ffill', limit=5)
        grp = grp.dropna()

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
) -> list[str]:
    """Return actual ASX trading dates sampled every step_days from start to end."""
    end_str = end or datetime.date.today().isoformat()

    start_ts = int(datetime.datetime.strptime(start, '%Y-%m-%d').timestamp())
    end_ts   = int(datetime.datetime.strptime(end_str, '%Y-%m-%d').timestamp()) + 86399

    with _conn(db_path) as conn:
        dates = pd.read_sql_query(
            "SELECT DISTINCT date FROM endofday WHERE date >= ? AND date <= ? ORDER BY date",
            conn,
            params=[start_ts, end_ts],
        )

    dates['date'] = pd.to_datetime(dates['date'], unit='s').dt.strftime('%Y-%m-%d')
    all_dates = dates['date'].tolist()

    # Sample every step_days
    return all_dates[::step_days]


def get_actual_5d_returns(
    db_path: str,
    eval_date: str,
    symbols: list[str],
) -> dict[str, float]:
    """Return actual 5-trading-day forward returns from eval_date for each symbol.

    Uses the 5th next trading date available in endofday (not calendar days).
    Returns {} entry omitted for symbols missing data.
    """
    eval_ts = int(datetime.datetime.strptime(eval_date, '%Y-%m-%d').timestamp())

    with _conn(db_path) as conn:
        ph = ','.join('?' * len(symbols))
        # Get eval_date close and the 5th subsequent close per symbol
        df = pd.read_sql_query(
            f"""SELECT symbol, date, close
                FROM endofday
                WHERE symbol IN ({ph}) AND date >= ?
                ORDER BY symbol, date""",
            conn,
            params=symbols + [eval_ts],
        )

    df['date'] = pd.to_datetime(df['date'], unit='s')

    result = {}
    for sym, grp in df.groupby('symbol', sort=False):
        grp = grp.sort_values('date').reset_index(drop=True)
        if len(grp) < 6:
            continue
        close_t0 = grp.loc[0, 'close']
        close_t5 = grp.loc[5, 'close']
        if close_t0 and close_t0 > 0:
            result[sym] = (close_t5 - close_t0) / close_t0
    return result
