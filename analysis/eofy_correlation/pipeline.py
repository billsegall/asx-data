# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""EOFY tax-loss/gain correlation pipeline.

For each current ASX symbol, tests whether its own Q1-3 (Jul-Mar) return
predicts its own Q4 (Apr-Jun) return across every financial year in its
trading history — the theory being that Q4 sees tax-loss/gain-driven
trading (Australian FY ends June 30).

Plain CPU pipeline (pandas/numpy/scipy) — no GPU/tensor ops needed since this
is a per-symbol scalar time-series correlation, not cross-sectional ranking.
Deliberately does not import analysis.core (DataLoader et al.) since that
package pulls in torch via feature_matrix/gpu_ops, which isn't installed on
every machine this pipeline might run on; this module only needs a plain
SQLite read.
"""

import datetime
import json
import logging
import os
import sqlite3
import time

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


def _fdr_correct(p_values: np.ndarray, alpha: float = 0.05):
    """Benjamini-Hochberg FDR correction (mirrors analysis.discovery.fdr_correction.fdr_correct).

    Duplicated here rather than imported — analysis.discovery.__init__ pulls in
    torch transitively via ic_sweep.py, which this plain-CPU pipeline must not
    require.
    """
    n = len(p_values)
    if n == 0:
        return np.array([], dtype=bool), np.array([])
    order = np.argsort(p_values)
    ranks = np.empty(n, dtype=int)
    ranks[order] = np.arange(1, n + 1)
    corrected = np.minimum(1.0, p_values * n / ranks)
    for i in range(n - 2, -1, -1):
        corrected[order[i]] = min(corrected[order[i]], corrected[order[i + 1]])
    reject = corrected <= alpha
    return reject, corrected

MIN_YEARS_FLOOR = 5
OUTLIER_ABS_RETURN = 3.0       # exclude single-quarter |return| > 300% (likely un-adjusted split)
BOUNDARY_TOLERANCE_DAYS = 10   # max staleness allowed for a boundary close
FRESHNESS_BUFFER_DAYS = 5      # calendar-day buffer before trusting a just-closed FY
MIN_FY_YEAR = 1989             # floor — endofday data starts 1988-01


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_current_symbols(conn) -> dict:
    """Return {symbol: industry} for current, non-index symbols."""
    rows = conn.execute(
        "SELECT symbol, industry FROM symbols "
        "WHERE current = 1 AND symbol NOT IN ('XAO', 'XJO')"
    ).fetchall()
    return {sym: (industry or 'Unknown') for sym, industry in rows}


def _load_eod(conn, symbols: list) -> pd.DataFrame:
    placeholders = ','.join('?' * len(symbols))
    df = pd.read_sql_query(
        f"SELECT symbol, date, close FROM endofday WHERE symbol IN ({placeholders}) "
        f"ORDER BY symbol, date",
        conn, params=symbols,
    )
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df = df.drop_duplicates(subset=['symbol', 'date'], keep='first')
    return df


def _load_shares(conn, symbols: list) -> dict:
    placeholders = ','.join('?' * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, shares FROM symbols WHERE symbol IN ({placeholders})",
        symbols,
    ).fetchall()
    return {sym: shares for sym, shares in rows}


def _load_corporate_event_dates(conn, symbols: list) -> dict:
    """Return {symbol: [pd.Timestamp, ...]} of corporate action dates."""
    placeholders = ','.join('?' * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, date FROM corporate_events WHERE symbol IN ({placeholders})",
        symbols,
    ).fetchall()
    out: dict = {}
    for sym, ts in rows:
        out.setdefault(sym, []).append(pd.Timestamp(ts, unit='s'))
    return out


# ---------------------------------------------------------------------------
# FY boundary math
# ---------------------------------------------------------------------------

def _fy_label(fy_year: int) -> str:
    """e.g. 1999 -> '1998-99', 2026 -> '2025-26'."""
    return f'{fy_year - 1}-{str(fy_year)[-2:]}'


def _fy_boundaries(fy_year: int):
    """(q13_start, q13_end, q4_start, q4_end) as pd.Timestamp for FY `fy_year`
    (Jul 1 fy_year-1 -> Jun 30 fy_year)."""
    return (
        pd.Timestamp(fy_year - 1, 7, 1),
        pd.Timestamp(fy_year, 3, 31),
        pd.Timestamp(fy_year, 4, 1),
        pd.Timestamp(fy_year, 6, 30),
    )


def _max_completed_fy_year(today: datetime.date, buffer_days: int = FRESHNESS_BUFFER_DAYS) -> int:
    """Latest FY whose Jun-30 close date has already occurred, per the calendar."""
    candidate = today.year if today.month >= 7 else today.year - 1
    fye = datetime.date(candidate, 6, 30)
    if today < fye + datetime.timedelta(days=buffer_days):
        candidate -= 1
    return candidate


def _asof_close(dates_arr: np.ndarray, closes_arr: np.ndarray, boundary: pd.Timestamp):
    """Last close at/before `boundary`, or (None, None) if missing/stale.

    dates_arr must be sorted ascending datetime64[ns]; closes_arr aligned.
    """
    idx = np.searchsorted(dates_arr, np.datetime64(boundary), side='right') - 1
    if idx < 0:
        return None, None
    actual_date = dates_arr[idx]
    staleness_days = (np.datetime64(boundary) - actual_date) / np.timedelta64(1, 'D')
    if staleness_days > BOUNDARY_TOLERANCE_DAYS:
        return None, None
    return float(closes_arr[idx]), pd.Timestamp(actual_date)


# ---------------------------------------------------------------------------
# Per-symbol FY return computation
# ---------------------------------------------------------------------------

def compute_fy_returns(dates_arr: np.ndarray, closes_arr: np.ndarray,
                        event_dates: list, max_fy_year: int,
                        min_fy_year: int = MIN_FY_YEAR) -> list:
    """Return one record per FY the symbol has full boundary data for.

    Each record: {fy, q13_return, q4_return, excluded, reason}.
    """
    records = []
    for fy_year in range(min_fy_year, max_fy_year + 1):
        q13_start, q13_end, q4_start, q4_end = _fy_boundaries(fy_year)

        entry13, _ = _asof_close(dates_arr, closes_arr, q13_start)
        exit13, _ = _asof_close(dates_arr, closes_arr, q13_end)
        entry4, _ = _asof_close(dates_arr, closes_arr, q4_start)
        exit4, _ = _asof_close(dates_arr, closes_arr, q4_end)

        if entry13 is None or exit13 is None or entry4 is None or exit4 is None:
            continue
        if entry13 <= 0 or entry4 <= 0:
            continue

        q13_return = exit13 / entry13 - 1
        q4_return = exit4 / entry4 - 1

        reason = None
        if abs(q13_return) > OUTLIER_ABS_RETURN or abs(q4_return) > OUTLIER_ABS_RETURN:
            reason = 'outlier'
        elif any(q13_start <= ev <= q4_end for ev in event_dates):
            reason = 'corporate_event'

        records.append({
            'fy': _fy_label(fy_year),
            'fy_year': fy_year,
            'q13_return': q13_return,
            'q4_return': q4_return,
            'excluded': reason is not None,
            'reason': reason,
        })
    return records


def compute_symbol_correlation(records: list, min_years: int = MIN_YEARS_FLOOR):
    """Pearson r between q13_return and q4_return across included FYs.

    Returns None if fewer than min_years included FYs.
    """
    included = [r for r in records if not r['excluded']]
    n_years = len(included)
    if n_years < min_years:
        return None

    q13 = np.array([r['q13_return'] for r in included])
    q4 = np.array([r['q4_return'] for r in included])

    r_value, p_value = stats.pearsonr(q13, q4)
    if np.isnan(r_value):
        return None

    fy_years = [r['fy_year'] for r in included]

    return {
        'n_years': n_years,
        'r': float(r_value),
        'p_value': float(p_value),
        'direction': 'positive' if r_value >= 0 else 'negative',
        'mean_q13_return': float(np.mean(q13)),
        'mean_q4_return': float(np.mean(q4)),
        'std_q13_return': float(np.std(q13, ddof=1)) if n_years > 1 else 0.0,
        'std_q4_return': float(np.std(q4, ddof=1)) if n_years > 1 else 0.0,
        'first_fy': _fy_label(min(fy_years)),
        'last_fy': _fy_label(max(fy_years)),
        'n_outliers_excluded': sum(1 for r in records if r['excluded']),
    }


# ---------------------------------------------------------------------------
# SQLite DB helpers
# ---------------------------------------------------------------------------

_EOFY_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS eofy_correlation (
    symbol          TEXT    PRIMARY KEY,
    industry        TEXT    NOT NULL,
    n_years         INTEGER NOT NULL,
    r               REAL    NOT NULL,
    p_value         REAL    NOT NULL,
    fdr_p           REAL    NOT NULL,
    direction       TEXT    NOT NULL,
    mean_q13_return REAL,
    mean_q4_return  REAL,
    std_q13_return  REAL,
    std_q4_return   REAL,
    first_fy        TEXT    NOT NULL,
    last_fy         TEXT    NOT NULL,
    market_cap      REAL,
    n_outliers_excluded INTEGER NOT NULL DEFAULT 0,
    fy_detail_json  TEXT    NOT NULL,
    run_at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eofy_industry ON eofy_correlation (industry);
CREATE INDEX IF NOT EXISTS idx_eofy_r        ON eofy_correlation (r);
CREATE INDEX IF NOT EXISTS idx_eofy_n_years  ON eofy_correlation (n_years);
CREATE INDEX IF NOT EXISTS idx_eofy_fdr_p    ON eofy_correlation (fdr_p);

CREATE TABLE IF NOT EXISTS eofy_correlation_runs (
    run_at          INTEGER PRIMARY KEY,
    n_symbols_tested INTEGER NOT NULL,
    n_significant   INTEGER NOT NULL,
    min_years       INTEGER NOT NULL,
    fdr_alpha       REAL    NOT NULL,
    elapsed_seconds REAL    NOT NULL
);
"""


def init_eofy_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    for stmt in _EOFY_DB_SCHEMA.split(';'):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()
    conn.close()


def write_to_db(df: pd.DataFrame, meta: dict, db_path: str) -> None:
    """Replace all rows in eofy_correlation.db (atomic transaction)."""
    run_at = int(meta.get('generated_at', time.time()))

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    try:
        conn.execute('BEGIN')
        conn.execute('DELETE FROM eofy_correlation')
        for _, r in df.iterrows():
            conn.execute(
                '''INSERT INTO eofy_correlation
                   (symbol, industry, n_years, r, p_value, fdr_p, direction,
                    mean_q13_return, mean_q4_return, std_q13_return, std_q4_return,
                    first_fy, last_fy, market_cap, n_outliers_excluded, fy_detail_json, run_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    r['symbol'], r['industry'], int(r['n_years']), float(r['r']),
                    float(r['p_value']), float(r['fdr_p']), r['direction'],
                    float(r['mean_q13_return']), float(r['mean_q4_return']),
                    float(r['std_q13_return']), float(r['std_q4_return']),
                    r['first_fy'], r['last_fy'],
                    float(r['market_cap']) if pd.notna(r['market_cap']) else None,
                    int(r['n_outliers_excluded']), r['fy_detail_json'], run_at,
                )
            )
        conn.execute(
            '''INSERT INTO eofy_correlation_runs
               (run_at, n_symbols_tested, n_significant, min_years, fdr_alpha, elapsed_seconds)
               VALUES (?,?,?,?,?,?)''',
            (
                run_at, meta['n_symbols_tested'], meta['n_significant'],
                meta['min_years'], meta['fdr_alpha'], meta['elapsed_seconds'],
            )
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(db_path: str, min_years: int = MIN_YEARS_FLOOR,
                  fdr_alpha: float = 0.05,
                  freshness_buffer_days: int = FRESHNESS_BUFFER_DAYS):
    """Run the full EOFY correlation computation for all current symbols.

    Returns (df, meta) — df has one row per symbol that passed min_years,
    meta is a dict of run-level stats.
    """
    t0 = time.time()
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA cache_size=-131072')

    industries = _load_current_symbols(conn)
    symbols = sorted(industries.keys())
    logger.info('EOFY correlation: %d current symbols', len(symbols))

    eod = _load_eod(conn, symbols)
    shares = _load_shares(conn, symbols)
    corp_events = _load_corporate_event_dates(conn, symbols)
    conn.close()

    max_fy_year = _max_completed_fy_year(datetime.date.today(), freshness_buffer_days)
    logger.info('Latest completed FY: %s', _fy_label(max_fy_year))

    results = []
    n_tested = 0
    for symbol, sub in eod.groupby('symbol', sort=False):
        dates_arr = sub['date'].values
        closes_arr = sub['close'].values
        records = compute_fy_returns(
            dates_arr, closes_arr, corp_events.get(symbol, []), max_fy_year
        )
        corr = compute_symbol_correlation(records, min_years=min_years)
        if corr is None:
            continue
        n_tested += 1

        latest_close = float(closes_arr[-1])
        sh = shares.get(symbol)
        market_cap = (sh * latest_close) if sh else None

        row = {
            'symbol': symbol,
            'industry': industries.get(symbol, 'Unknown'),
            'market_cap': market_cap,
            'fy_detail_json': json.dumps(records),
            **corr,
        }
        results.append(row)

    df = pd.DataFrame(results)
    if len(df) == 0:
        meta = {
            'generated_at': int(time.time()),
            'n_symbols_tested': 0,
            'n_significant': 0,
            'min_years': min_years,
            'fdr_alpha': fdr_alpha,
            'elapsed_seconds': time.time() - t0,
        }
        return df, meta

    reject, fdr_p = _fdr_correct(df['p_value'].values, alpha=fdr_alpha)
    df['fdr_p'] = fdr_p

    meta = {
        'generated_at': int(time.time()),
        'n_symbols_tested': n_tested,
        'n_significant': int(reject.sum()),
        'min_years': min_years,
        'fdr_alpha': fdr_alpha,
        'elapsed_seconds': time.time() - t0,
    }
    logger.info(
        'EOFY correlation: %d symbols tested, %d significant (fdr<%.2f), %.1fs',
        n_tested, meta['n_significant'], fdr_alpha, meta['elapsed_seconds'],
    )
    return df, meta
