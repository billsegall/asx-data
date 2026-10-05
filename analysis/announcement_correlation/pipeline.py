# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Announcement-type -> forward-return correlation pipeline.

For every ASX announcement, resolves a `type` (structured extracted_* table,
or a headline-regex fallback for trading halts/placements/etc — see
types.py/classify.py), aligns it to the first tradeable day at/after its
announcement date (alignment.py — careful to not just use the nominal day,
since a halted stock fails FeatureMatrix's own validity mask on the halt day
itself), and computes forward returns at several horizons, excess of the
XAO index over the same window.

Unlike eofy_correlation (deliberately CPU-only, avoids importing
analysis.core to dodge torch), this module WANTS GPU vectorization: forward
returns for every symbol/date/horizon are computed once as tensor ops, then
gathered at each event's specific (symbol, date) cell, rather than looping
per-event SQL queries ~80k times.

endofday.close is already split/consolidation-adjusted at the source
(fetch_splits.py re-downloads full history via yfinance auto_adjust=True on
every new corporate event) -- so corporate_events is used purely as an
EXCLUSION guard here (drop any event whose [t0, t0+max_horizon] window
overlaps a logged split/consolidation), same pattern as eofy_correlation,
not a ratio-adjustment.
"""

import logging
import sqlite3
import time

import numpy as np
import pandas as pd
import torch
from scipy import stats as scipy_stats

from ..backtest.forward_returns import forward_returns
from ..core.data_loader import DataLoader
from ..core.feature_matrix import FeatureMatrix
from . import alignment
from . import types as type_resolver

logger = logging.getLogger(__name__)

HORIZONS = [1, 5, 20, 60]
EVENT_DAY_MAX_LAG = 10
OUTLIER_ABS_RETURN = 3.0        # same guard as eofy_correlation
MIN_N_DISPLAY = 20
MARKET_INDEX_SYMBOL = 'XAO'     # All Ordinaries, present as a regular endofday symbol


def _fdr_correct(p_values: np.ndarray, alpha: float = 0.05):
    """Benjamini-Hochberg FDR correction.

    Duplicated from eofy_correlation.pipeline._fdr_correct rather than
    imported -- keeps each experimental module independently runnable (and
    removable) without one depending on the other's internals.
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


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_announcements(ann_db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(ann_db_path)
    try:
        return pd.read_sql_query(
            'SELECT ids_id, ticker, headline, announced_at, price_sensitive FROM announcements',
            conn,
        )
    finally:
        conn.close()


def _load_corporate_event_dates(stock_db_path: str, symbols: list) -> dict:
    """{symbol: [unix_seconds, ...]} — same encoding as endofday/fm.dates, no
    date-string conversion needed since both come from the same table."""
    conn = sqlite3.connect(stock_db_path)
    try:
        placeholders = ','.join('?' * len(symbols))
        rows = conn.execute(
            f'SELECT symbol, date FROM corporate_events WHERE symbol IN ({placeholders})',
            symbols,
        ).fetchall()
    finally:
        conn.close()
    out: dict = {}
    for sym, ts in rows:
        out.setdefault(sym, []).append(int(ts))
    return out


def _overlaps_corporate_event(event_dates, dates: np.ndarray, t0_idx: int, max_horizon: int) -> bool:
    if not event_dates:
        return False
    end_idx = min(t0_idx + max_horizon, len(dates) - 1)
    lo, hi = int(dates[t0_idx]), int(dates[end_idx])
    return any(lo <= ev <= hi for ev in event_dates)


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(stock_db_path: str, ann_db_path: str, cache_dir: str,
                  horizons: list = None, min_n_display: int = MIN_N_DISPLAY,
                  fdr_alpha: float = 0.05):
    """Returns (agg_df, detail_df, meta)."""
    t0 = time.time()
    horizons = horizons or HORIZONS
    max_horizon = max(horizons)

    loader = DataLoader(stock_db_path, split='all')
    # min_history_days=0: load_eod's filter applies whenever split != 'backtest',
    # so split='all' does NOT skip it by default -- the usual 252-day default
    # would silently drop every recently-listed small cap, exactly the
    # population doing placements/rights issues/capital raisings.
    eod = loader.load_eod(min_history_days=0)
    fm = FeatureMatrix(eod, pd.DataFrame(columns=['symbol', 'date', 'short']),
                        split='all', cache_dir=cache_dir)
    features = fm.build()
    # Collapse same-calendar-day duplicate columns (see alignment.py's
    # timezone note) BEFORE any trading-day counting, so t0_lag_days and
    # every horizon shift below are exact calendar-trading-day counts.
    close, mask, dates, date_str_arr = alignment.collapse_to_calendar_days(
        features['close'], fm.mask, fm.dates)
    sym_to_idx = {s: i for i, s in enumerate(fm.symbols)}

    logger.info('Price matrix: %d symbols x %d dates', len(fm.symbols), len(dates))

    fwd = {h: forward_returns(close, mask, h) for h in horizons}

    xao_idx = sym_to_idx.get(MARKET_INDEX_SYMBOL)
    market_fwd = {}
    if xao_idx is not None:
        xao_close = close[xao_idx:xao_idx + 1, :]
        # XAO is an index -- its endofday.volume is 0 for ~99% of rows (no
        # traded volume on an index, by design of the source feed). fm.mask
        # requires volume>0, so XAO fails FeatureMatrix's own validity mask
        # almost everywhere, which made market_fwd NaN for ~98-100% of
        # events and silently collapsed "excess return" to raw return
        # (confirmed live: excess_return == raw_return for 98-100% of
        # events across every horizon). The index only needs a valid close
        # to be usable as a baseline -- drop the volume gate for this row.
        xao_mask = ~torch.isnan(xao_close)
        for h in horizons:
            market_fwd[h] = forward_returns(xao_close, xao_mask, h)
    else:
        logger.warning('%s not found in price matrix -- excess returns will equal raw returns',
                        MARKET_INDEX_SYMBOL)

    corp_events = _load_corporate_event_dates(stock_db_path, list(fm.symbols))

    ann = load_announcements(ann_db_path)
    extracted_index = type_resolver.build_type_index(ann_db_path)
    ann['type'] = [
        type_resolver.resolve_type(ids_id, headline, extracted_index)
        for ids_id, headline in zip(ann['ids_id'], ann['headline'])
    ]

    n_total = len(ann)
    n_excluded_outside_range = 0
    n_excluded_no_valid_base = 0
    n_excluded_corporate_event = 0
    n_excluded_outlier = 0
    n_type_other = int((ann['type'] == type_resolver.OTHER_TYPE).sum())

    detail_rows = []
    for row in ann.itertuples(index=False):
        sym_idx = sym_to_idx.get(row.ticker)
        if sym_idx is None:
            n_excluded_outside_range += 1
            continue
        nominal_idx = alignment.nearest_trading_day_index(date_str_arr, row.announced_at)
        if nominal_idx is None:
            n_excluded_outside_range += 1
            continue
        t0_idx, t0_lag = alignment.find_valid_base(mask[sym_idx], nominal_idx, EVENT_DAY_MAX_LAG)
        if t0_idx is None:
            n_excluded_no_valid_base += 1
            continue
        if _overlaps_corporate_event(corp_events.get(row.ticker), dates, t0_idx, max_horizon):
            n_excluded_corporate_event += 1
            continue

        event_day_reaction = None
        prev_idx, _ = alignment.find_last_valid(mask[sym_idx], nominal_idx, EVENT_DAY_MAX_LAG)
        if prev_idx is not None:
            prev_close = close[sym_idx, prev_idx].item()
            cur_close = close[sym_idx, t0_idx].item()
            if prev_close:
                event_day_reaction = (cur_close - prev_close) / prev_close

        detail = {
            'ids_id': row.ids_id, 'ticker': row.ticker, 'type': row.type,
            'price_sensitive': int(row.price_sensitive or 0),
            'announced_at': row.announced_at, 't0_lag_days': t0_lag,
            'event_day_reaction': event_day_reaction,
        }
        any_outlier = False
        for h in horizons:
            r = fwd[h][sym_idx, t0_idx].item()
            if np.isnan(r) or abs(r) > OUTLIER_ABS_RETURN:
                if not np.isnan(r):
                    any_outlier = True
                detail[f'raw_return_{h}'] = None
                detail[f'excess_return_{h}'] = None
                continue
            m = market_fwd[h][0, t0_idx].item() if xao_idx is not None else 0.0
            excess = r if np.isnan(m) else (r - m)
            detail[f'raw_return_{h}'] = r
            detail[f'excess_return_{h}'] = excess
        if any_outlier:
            n_excluded_outlier += 1
        detail_rows.append(detail)

    detail_df = pd.DataFrame(detail_rows)
    agg_df = _aggregate(detail_df, horizons, min_n_display, fdr_alpha)

    meta = {
        'generated_at': int(time.time()),
        'n_events_total': n_total,
        'n_excluded_outside_range': n_excluded_outside_range,
        'n_excluded_no_valid_base': n_excluded_no_valid_base,
        'n_excluded_corporate_event': n_excluded_corporate_event,
        'n_excluded_outlier': n_excluded_outlier,
        'n_type_other': n_type_other,
        'elapsed_seconds': time.time() - t0,
    }
    logger.info(
        'Announcement correlation: %d events, excluded outside_range=%d no_valid_base=%d '
        'corp_event=%d outlier_events=%d, other=%d, %.1fs',
        n_total, n_excluded_outside_range, n_excluded_no_valid_base,
        n_excluded_corporate_event, n_excluded_outlier, n_type_other,
        meta['elapsed_seconds'],
    )
    return agg_df, detail_df, meta


def _aggregate(detail_df: pd.DataFrame, horizons: list, min_n_display: int,
                fdr_alpha: float) -> pd.DataFrame:
    if len(detail_df) == 0:
        return pd.DataFrame()

    agg_rows = []
    for h in horizons:
        rcol, ecol = f'raw_return_{h}', f'excess_return_{h}'
        sub_all = detail_df[detail_df[ecol].notna()]
        groups = [('both', sub_all)] + [
            (str(ps), sub_all[sub_all['price_sensitive'] == ps]) for ps in (0, 1)
        ]
        for ps_label, sub_ps in groups:
            for type_label, g in sub_ps.groupby('type'):
                if type_label == type_resolver.OTHER_TYPE:
                    continue
                n = len(g)
                if n == 0:
                    continue
                excess = g[ecol].to_numpy(dtype=float)
                raw = g[rcol].to_numpy(dtype=float)
                hit_rate = float((excess > 0).mean())
                n_pos = int((excess > 0).sum())
                if n >= 2:
                    t_stat, p_value = scipy_stats.ttest_1samp(excess, 0.0)
                    t_stat, p_value = float(t_stat), float(p_value)
                else:
                    t_stat, p_value = float('nan'), float('nan')
                sign_p = float(scipy_stats.binomtest(n_pos, n, 0.5).pvalue)
                agg_rows.append({
                    'type': type_label, 'horizon': h, 'price_sensitive': ps_label,
                    'n': n, 'mean_raw': float(np.mean(raw)), 'mean_excess': float(np.mean(excess)),
                    'median_excess': float(np.median(excess)),
                    'std_excess': float(np.std(excess, ddof=1)) if n > 1 else 0.0,
                    'hit_rate': hit_rate, 't_stat': t_stat, 'p_value': p_value,
                    'sign_test_p': sign_p, 'display_cutoff': n < min_n_display,
                })

    agg_df = pd.DataFrame(agg_rows)
    if len(agg_df) == 0:
        return agg_df

    # BH-FDR only within the primary family (pooled price_sensitive='both' cells) --
    # pooling the PS-split cells into the same correction would both underpower
    # the already-thin rare-type cells and over-correct them.
    agg_df['fdr_p'] = np.nan
    pooled_mask = agg_df['price_sensitive'] == 'both'
    pooled = agg_df[pooled_mask]
    if len(pooled) > 0:
        _, fdr_p = _fdr_correct(pooled['p_value'].fillna(1.0).to_numpy(), alpha=fdr_alpha)
        agg_df.loc[pooled_mask, 'fdr_p'] = fdr_p
    return agg_df


# ---------------------------------------------------------------------------
# SQLite DB helpers
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS announcement_correlation (
    type              TEXT    NOT NULL,
    horizon           INTEGER NOT NULL,
    price_sensitive   TEXT    NOT NULL,
    n                 INTEGER NOT NULL,
    mean_raw          REAL,
    mean_excess       REAL,
    median_excess     REAL,
    std_excess        REAL,
    hit_rate          REAL,
    t_stat            REAL,
    p_value           REAL,
    sign_test_p       REAL,
    fdr_p             REAL,
    display_cutoff    INTEGER NOT NULL,
    run_at            INTEGER NOT NULL,
    PRIMARY KEY (type, horizon, price_sensitive, run_at)
);
CREATE INDEX IF NOT EXISTS idx_annc_type    ON announcement_correlation (type);
CREATE INDEX IF NOT EXISTS idx_annc_horizon ON announcement_correlation (horizon);
CREATE INDEX IF NOT EXISTS idx_annc_fdr_p   ON announcement_correlation (fdr_p);

CREATE TABLE IF NOT EXISTS announcement_correlation_events (
    ids_id              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    type                TEXT    NOT NULL,
    price_sensitive     INTEGER NOT NULL,
    announced_at        TEXT    NOT NULL,
    t0_lag_days         INTEGER NOT NULL,
    event_day_reaction  REAL,
    raw_return_1        REAL,  excess_return_1  REAL,
    raw_return_5        REAL,  excess_return_5  REAL,
    raw_return_20       REAL,  excess_return_20 REAL,
    raw_return_60       REAL,  excess_return_60 REAL,
    run_at              INTEGER NOT NULL,
    PRIMARY KEY (ids_id, ticker, run_at)
);
CREATE INDEX IF NOT EXISTS idx_annce_type ON announcement_correlation_events (type);

CREATE TABLE IF NOT EXISTS announcement_correlation_runs (
    run_at                      INTEGER PRIMARY KEY,
    n_events_total               INTEGER NOT NULL,
    n_excluded_outside_range     INTEGER NOT NULL,
    n_excluded_no_valid_base     INTEGER NOT NULL,
    n_excluded_corporate_event   INTEGER NOT NULL,
    n_excluded_outlier           INTEGER NOT NULL,
    n_type_other                 INTEGER NOT NULL,
    elapsed_seconds               REAL NOT NULL
);
"""


def init_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    for stmt in _SCHEMA.split(';'):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()
    conn.close()


def write_to_db(agg_df: pd.DataFrame, detail_df: pd.DataFrame, meta: dict, db_path: str) -> None:
    """Replace all rows (atomic transaction) -- v1 keeps only the latest run."""
    run_at = int(meta.get('generated_at', time.time()))
    horizons = HORIZONS

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    try:
        conn.execute('BEGIN')
        conn.execute('DELETE FROM announcement_correlation')
        conn.execute('DELETE FROM announcement_correlation_events')
        conn.execute('DELETE FROM announcement_correlation_runs')

        for _, r in agg_df.iterrows():
            conn.execute(
                '''INSERT INTO announcement_correlation
                   (type, horizon, price_sensitive, n, mean_raw, mean_excess, median_excess,
                    std_excess, hit_rate, t_stat, p_value, sign_test_p, fdr_p, display_cutoff, run_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    r['type'], int(r['horizon']), r['price_sensitive'], int(r['n']),
                    float(r['mean_raw']), float(r['mean_excess']), float(r['median_excess']),
                    float(r['std_excess']), float(r['hit_rate']),
                    float(r['t_stat']) if pd.notna(r['t_stat']) else None,
                    float(r['p_value']) if pd.notna(r['p_value']) else None,
                    float(r['sign_test_p']) if pd.notna(r['sign_test_p']) else None,
                    float(r['fdr_p']) if pd.notna(r['fdr_p']) else None,
                    int(r['display_cutoff']), run_at,
                )
            )

        cols = ['ids_id', 'ticker', 'type', 'price_sensitive', 'announced_at',
                't0_lag_days', 'event_day_reaction']
        for h in horizons:
            cols += [f'raw_return_{h}', f'excess_return_{h}']
        placeholders = ','.join('?' * (len(cols) + 1))
        for _, r in detail_df.iterrows():
            values = [r.get(c) for c in cols]
            values = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else v for v in values]
            conn.execute(
                f'INSERT INTO announcement_correlation_events ({",".join(cols)}, run_at) '
                f'VALUES ({placeholders})',
                values + [run_at],
            )

        conn.execute(
            '''INSERT INTO announcement_correlation_runs
               (run_at, n_events_total, n_excluded_outside_range, n_excluded_no_valid_base,
                n_excluded_corporate_event, n_excluded_outlier, n_type_other, elapsed_seconds)
               VALUES (?,?,?,?,?,?,?,?)''',
            (
                run_at, meta['n_events_total'], meta['n_excluded_outside_range'],
                meta['n_excluded_no_valid_base'], meta['n_excluded_corporate_event'],
                meta['n_excluded_outlier'], meta['n_type_other'], meta['elapsed_seconds'],
            )
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
