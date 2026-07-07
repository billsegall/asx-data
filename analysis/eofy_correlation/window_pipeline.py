# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Sub-window EOFY correlation: per-stock Q1-3 vs a specific Q4 sub-window.

Follow-up to pipeline.py's whole-Q4 test. The weekly breakdown
(run_eofy_weekly_breakdown.py) found the top-50 |r| stocks' Q4 effect
concentrated in day57-70 of Q4 (~late May). This module tests that window
("A") and its complement, day71-91 (~Jun30, "B"), against the full current-
symbol universe, using the same per-FY guards as pipeline.py.

Stores results in the same eofy_correlation.db, in sibling tables so the
web page can offer "Full Quarter" / "Late May" / "Rest of Quarter" tabs.
"""

import datetime
import json
import sqlite3
import time

import numpy as np
import pandas as pd
from scipy import stats

from analysis.eofy_correlation.pipeline import (
    MIN_YEARS_FLOOR,
    _asof_close,
    _fdr_correct,
    _fy_boundaries,
    _load_corporate_event_dates,
    _load_current_symbols,
    _load_eod,
    _load_shares,
    _max_completed_fy_year,
    compute_fy_returns,
)

# day57-70 of Q4 (Apr1=day1): the strongest window found by the weekly breakdown.
# day71-91: complement, ending at the true Jun-30 boundary (not q4_start+91d,
# which overshoots to Jul 1 -- the weekly script's week-13 boundary did this).
WINDOWS = {
    'A': {'start_day': 57, 'end_day': 70, 'label': 'Late May (day 57-70)'},
    'B': {'start_day': 71, 'end_day': 91, 'label': 'Rest of Q4 (day 71-91, to Jun 30)'},
}

_WINDOW_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS eofy_window_correlation (
    symbol              TEXT    NOT NULL,
    window              TEXT    NOT NULL,
    industry            TEXT    NOT NULL,
    n_years             INTEGER NOT NULL,
    r                    REAL    NOT NULL,
    p_value              REAL    NOT NULL,
    fdr_p                REAL    NOT NULL,
    direction            TEXT    NOT NULL,
    mean_q13_return       REAL,
    mean_window_return    REAL,
    std_q13_return        REAL,
    std_window_return     REAL,
    first_fy              TEXT    NOT NULL,
    last_fy               TEXT    NOT NULL,
    market_cap            REAL,
    n_outliers_excluded   INTEGER NOT NULL DEFAULT 0,
    fy_detail_json        TEXT    NOT NULL,
    run_at                INTEGER NOT NULL,
    PRIMARY KEY (symbol, window)
);
CREATE INDEX IF NOT EXISTS idx_eofy_window_industry ON eofy_window_correlation (window, industry);
CREATE INDEX IF NOT EXISTS idx_eofy_window_r        ON eofy_window_correlation (window, r);
CREATE INDEX IF NOT EXISTS idx_eofy_window_fdr       ON eofy_window_correlation (window, fdr_p);

CREATE TABLE IF NOT EXISTS eofy_window_definitions (
    window      TEXT PRIMARY KEY,
    label       TEXT    NOT NULL,
    start_day   INTEGER NOT NULL,
    end_day     INTEGER NOT NULL,
    n_tested    INTEGER NOT NULL,
    n_significant INTEGER NOT NULL,
    run_at      INTEGER NOT NULL
);
"""


def init_eofy_window_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    for stmt in _WINDOW_DB_SCHEMA.split(';'):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()
    conn.close()


def _window_return(dates_arr, closes_arr, boundary_a, boundary_b):
    a, _ = _asof_close(dates_arr, closes_arr, boundary_a)
    b, _ = _asof_close(dates_arr, closes_arr, boundary_b)
    if a is None or b is None or a <= 0:
        return None
    return b / a - 1


def run_window_pipeline(db_path: str, min_years: int = MIN_YEARS_FLOOR, fdr_alpha: float = 0.05):
    """Run the day57-70 / day71-91 sub-window tests for all current symbols.

    Returns {'A': (df, meta), 'B': (df, meta)}.
    """
    t0 = time.time()
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA cache_size=-131072')
    industries = _load_current_symbols(conn)
    symbols = sorted(industries.keys())
    eod = _load_eod(conn, symbols)
    shares = _load_shares(conn, symbols)
    corp_events = _load_corporate_event_dates(conn, symbols)
    conn.close()

    max_fy_year = _max_completed_fy_year(datetime.date.today())

    per_symbol_records = {}  # symbol -> (rows, n_outliers, latest_close)
    for symbol, sub in eod.groupby('symbol', sort=False):
        dates_arr = sub['date'].values
        closes_arr = sub['close'].values
        fy_records = compute_fy_returns(dates_arr, closes_arr, corp_events.get(symbol, []), max_fy_year)
        n_outliers = sum(1 for r in fy_records if r['excluded'])
        rows = []
        for rec in fy_records:
            if rec['excluded']:
                continue
            q13_start, q13_end, q4_start, q4_end = _fy_boundaries(rec['fy_year'])
            a1 = q4_start + pd.Timedelta(days=WINDOWS['A']['start_day'] - 1)
            a2 = q4_start + pd.Timedelta(days=WINDOWS['A']['end_day'])
            b1 = q4_start + pd.Timedelta(days=WINDOWS['B']['start_day'] - 1)
            b2 = q4_end
            a_ret = _window_return(dates_arr, closes_arr, a1, a2)
            b_ret = _window_return(dates_arr, closes_arr, b1, b2)
            if a_ret is None or b_ret is None:
                continue
            rows.append({
                'fy': rec['fy'], 'fy_year': rec['fy_year'],
                'q13_return': rec['q13_return'], 'a_return': a_ret, 'b_return': b_ret,
            })
        if len(rows) >= min_years:
            per_symbol_records[symbol] = (rows, n_outliers, float(closes_arr[-1]))

    results = {'A': [], 'B': []}
    for symbol, (rows, n_outliers, latest_close) in per_symbol_records.items():
        q13 = np.array([r['q13_return'] for r in rows])
        n_years = len(rows)
        fy_years = [r['fy_year'] for r in rows]
        first_fy = min(rows, key=lambda r: r['fy_year'])['fy']
        last_fy = max(rows, key=lambda r: r['fy_year'])['fy']
        sh = shares.get(symbol)
        market_cap = (sh * latest_close) if sh else None

        for label, key in (('A', 'a_return'), ('B', 'b_return')):
            wret = np.array([r[key] for r in rows])
            r_value, p_value = stats.pearsonr(q13, wret)
            if np.isnan(r_value):
                continue
            fy_detail = [
                {'fy': r['fy'], 'q13_return': r['q13_return'], 'window_return': r[key]}
                for r in rows
            ]
            results[label].append({
                'symbol': symbol,
                'industry': industries[symbol],
                'n_years': n_years,
                'r': float(r_value),
                'p_value': float(p_value),
                'direction': 'positive' if r_value >= 0 else 'negative',
                'mean_q13_return': float(np.mean(q13)),
                'mean_window_return': float(np.mean(wret)),
                'std_q13_return': float(np.std(q13, ddof=1)) if n_years > 1 else 0.0,
                'std_window_return': float(np.std(wret, ddof=1)) if n_years > 1 else 0.0,
                'first_fy': first_fy,
                'last_fy': last_fy,
                'market_cap': market_cap,
                'n_outliers_excluded': n_outliers,
                'fy_detail_json': json.dumps(fy_detail),
            })

    out = {}
    for label in ('A', 'B'):
        df = pd.DataFrame(results[label])
        if len(df) > 0:
            reject, fdr_p = _fdr_correct(df['p_value'].values, alpha=fdr_alpha)
            df['fdr_p'] = fdr_p
        meta = {
            'window': label,
            'label': WINDOWS[label]['label'],
            'start_day': WINDOWS[label]['start_day'],
            'end_day': WINDOWS[label]['end_day'],
            'n_tested': len(df),
            'n_significant': int((df['fdr_p'] < fdr_alpha).sum()) if len(df) > 0 else 0,
            'run_at': int(time.time()),
        }
        out[label] = (df, meta)

    elapsed = time.time() - t0
    for label in out:
        out[label][1]['elapsed_seconds'] = elapsed
    return out


def write_window_to_db(results: dict, db_path: str) -> None:
    """Replace all rows in eofy_window_correlation + eofy_window_definitions."""
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    try:
        conn.execute('BEGIN')
        conn.execute('DELETE FROM eofy_window_correlation')
        conn.execute('DELETE FROM eofy_window_definitions')
        for label, (df, meta) in results.items():
            for _, r in df.iterrows():
                conn.execute(
                    '''INSERT INTO eofy_window_correlation
                       (symbol, window, industry, n_years, r, p_value, fdr_p, direction,
                        mean_q13_return, mean_window_return, std_q13_return, std_window_return,
                        first_fy, last_fy, market_cap, n_outliers_excluded, fy_detail_json, run_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (
                        r['symbol'], label, r['industry'], int(r['n_years']), float(r['r']),
                        float(r['p_value']), float(r['fdr_p']), r['direction'],
                        float(r['mean_q13_return']), float(r['mean_window_return']),
                        float(r['std_q13_return']), float(r['std_window_return']),
                        r['first_fy'], r['last_fy'],
                        float(r['market_cap']) if pd.notna(r['market_cap']) else None,
                        int(r['n_outliers_excluded']), r['fy_detail_json'], meta['run_at'],
                    )
                )
            conn.execute(
                '''INSERT INTO eofy_window_definitions
                   (window, label, start_day, end_day, n_tested, n_significant, run_at)
                   VALUES (?,?,?,?,?,?,?)''',
                (label, meta['label'], meta['start_day'], meta['end_day'],
                 meta['n_tested'], meta['n_significant'], meta['run_at']),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
