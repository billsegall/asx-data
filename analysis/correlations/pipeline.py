# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Full lead-lag correlation discovery pipeline.

Steps:
  1. Load training EOD data → log returns, align calendar
  2. Liquidity filter → ~400-600 symbols (or a subset via symbols_hint)
  3. Subtract XAO returns (market-adjust, optional)
  4. GPU CCF for all (pair, lag) combinations at lags 1..max_lag
  5. p-values via t-distribution
  6. FDR correction (Benjamini-Hochberg) across all tests
  7. Keep pairs: fdr_p < alpha AND |r| >= min_r
  8. Stability: split training into 3 sub-periods, recompute significance
  9. Backtest validation: compute CCF on held-out period
 10. Optionally save correlations.csv + correlations_meta.json
"""

import json
import logging
import os
import sqlite3
import time

import numpy as np
import pandas as pd
import torch

from ..core.data_loader import DataLoader
from ..discovery.fdr_correction import fdr_correct
from .lead_lag import ccf_pvalues, compute_ccf_gpu

logger = logging.getLogger(__name__)

MIN_LIQUIDITY_VALUE  = 500_000   # $500k median daily traded value
MIN_COVERAGE_FRAC    = 0.90      # ≥90% of training days must have data
N_STABILITY_PERIODS  = 5         # sub-periods for stability check


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _load_pivot(db_path: str, split: str, symbols=None) -> pd.DataFrame:
    """Load EOD → compute log returns → pivot to (symbol × date) DataFrame.

    Returns a DataFrame with symbols as index, pd.Timestamp as columns,
    and market log-returns as values (NaN for missing dates).
    """
    loader = DataLoader(db_path, split=split)
    eod = loader.load_eod(symbols=symbols, min_history_days=0)
    eod = eod.sort_values(['symbol', 'date'])
    eod['log_ret'] = eod.groupby('symbol')['close'].transform(
        lambda x: np.log(x / x.shift(1))
    )
    # Drop duplicate (symbol, date) rows (documented quirk in stockdb)
    eod = eod.drop_duplicates(subset=['symbol', 'date'], keep='first')
    pivot = eod.pivot(index='symbol', columns='date', values='log_ret')
    if symbols is not None:
        pivot = pivot.reindex(symbols)
    return pivot


def _liquidity_filter(db_path: str,
                      min_value: float = MIN_LIQUIDITY_VALUE,
                      min_coverage: float = MIN_COVERAGE_FRAC,
                      symbols_hint: list[str] | None = None) -> list[str]:
    """Return symbols passing liquidity filter on training data.

    If symbols_hint is given, only those symbols are considered —
    this avoids loading the full market universe when running per-industry.
    """
    loader = DataLoader(db_path, split='train')
    eod = loader.load_eod(symbols=symbols_hint, min_history_days=0)
    eod = eod.drop_duplicates(subset=['symbol', 'date'], keep='first')
    eod['traded_value'] = eod['close'] * eod['volume']

    n_total_days = eod['date'].nunique()
    min_days = int(min_coverage * n_total_days)

    stats_df = eod.groupby('symbol').agg(
        median_value=('traded_value', 'median'),
        n_days=('date', 'count'),
    )
    valid = stats_df[
        (stats_df['median_value'] >= min_value) &
        (stats_df['n_days'] >= min_days)
    ].index.tolist()

    logger.info(
        'Liquidity filter: %d / %d symbols pass '
        '(min_value=$%s, min_days=%d)',
        len(valid), len(stats_df), f'{min_value:,.0f}', min_days,
    )
    return sorted(valid)


def _market_adjust(pivot: pd.DataFrame, db_path: str, split: str) -> pd.DataFrame:
    """Subtract XAO log-return from each symbol row."""
    loader = DataLoader(db_path, split=split)
    xao_eod = loader.load_eod(symbols=['XAO'], min_history_days=0)
    xao_eod = xao_eod.sort_values('date')
    xao_eod['log_ret'] = np.log(xao_eod['close'] / xao_eod['close'].shift(1))
    xao_series = xao_eod.set_index('date')['log_ret']
    xao_aligned = xao_series.reindex(pivot.columns)
    return pivot.subtract(xao_aligned, axis=1)


def _to_tensor(pivot: pd.DataFrame, device: str) -> torch.Tensor:
    """Convert pivot DataFrame to float32 CUDA tensor, NaN → 0."""
    arr = pivot.values.astype(np.float32)
    t = torch.tensor(arr, device=device)
    return torch.nan_to_num(t, nan=0.0)


# ---------------------------------------------------------------------------
# Stability check
# ---------------------------------------------------------------------------

def _stability_check(pivot: pd.DataFrame,
                     max_lag: int,
                     min_r: float,
                     fdr_alpha: float,
                     device: str) -> np.ndarray:
    """Split training pivot into N_STABILITY_PERIODS sub-periods; track per-period significance.

    Args:
        pivot: (N_sym × N_dates) market-adjusted log-return DataFrame

    Returns:
        (N_sym, N_sym, max_lag, N_STABILITY_PERIODS) uint8 array — 1 if significant, 0 if not,
        ordered from oldest (index 0) to newest (index N_STABILITY_PERIODS-1).
    """
    N = len(pivot)
    sig_periods = np.zeros((N, N, max_lag, N_STABILITY_PERIODS), dtype=np.uint8)

    dates = list(pivot.columns)
    n = len(dates)
    breaks = [n * i // N_STABILITY_PERIODS for i in range(N_STABILITY_PERIODS + 1)]

    for p_idx in range(N_STABILITY_PERIODS):
        period_dates = dates[breaks[p_idx]:breaks[p_idx + 1]]
        sub_pivot = pivot[period_dates]
        ret_t = _to_tensor(sub_pivot, device)
        T_sub = ret_t.shape[1]

        r = compute_ccf_gpu(ret_t, max_lag).cpu().numpy()
        p = ccf_pvalues(r, max_lag, T_sub)

        # FDR per sub-period (exclude self-pairs)
        diag = np.eye(N, dtype=bool)
        valid = np.repeat(~diag[:, :, np.newaxis], max_lag, axis=2)  # (N, N, max_lag)
        p_flat = p[valid]
        _, p_adj_flat = fdr_correct(p_flat, alpha=fdr_alpha)
        p_adj = np.ones_like(p)
        p_adj[valid] = p_adj_flat

        sig = (np.abs(r) >= min_r) & (p_adj <= fdr_alpha)
        sig_periods[:, :, :, p_idx] = sig.astype(np.uint8)

    return sig_periods


# ---------------------------------------------------------------------------
# SQLite DB helpers
# ---------------------------------------------------------------------------

_CORR_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS correlations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    leader          TEXT    NOT NULL,
    follower        TEXT    NOT NULL,
    industry        TEXT    NOT NULL,
    lag_days        INTEGER NOT NULL,
    direction       TEXT    NOT NULL,
    train_r         REAL    NOT NULL,
    backtest_r      REAL,
    fdr_p           REAL    NOT NULL,
    stability       TEXT    NOT NULL,
    n_stable        INTEGER NOT NULL,
    recency_score   INTEGER NOT NULL,
    market_adjusted INTEGER NOT NULL,
    run_at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corr_industry ON correlations (industry);
CREATE INDEX IF NOT EXISTS idx_corr_leader   ON correlations (leader);
CREATE INDEX IF NOT EXISTS idx_corr_follower ON correlations (follower);
CREATE INDEX IF NOT EXISTS idx_corr_n_stable ON correlations (n_stable, recency_score);
CREATE INDEX IF NOT EXISTS idx_corr_train_r  ON correlations (train_r);
CREATE INDEX IF NOT EXISTS idx_corr_lag      ON correlations (lag_days);
CREATE INDEX IF NOT EXISTS idx_corr_ind_r    ON correlations (industry, train_r);

CREATE TABLE IF NOT EXISTS correlation_runs (
    industry          TEXT    PRIMARY KEY,
    run_at            INTEGER NOT NULL,
    n_symbols         INTEGER NOT NULL,
    n_pairs_tested    INTEGER NOT NULL,
    n_significant     INTEGER NOT NULL,
    n_stable          INTEGER NOT NULL,
    train_start       TEXT    NOT NULL,
    train_end         TEXT    NOT NULL,
    backtest_start    TEXT,
    backtest_end      TEXT,
    max_lag           INTEGER NOT NULL,
    min_r             REAL    NOT NULL,
    elapsed_seconds   REAL    NOT NULL
);
"""


def init_correlations_db(corr_db_path: str) -> None:
    """Create correlations.db schema if it does not exist.

    Migrates automatically if the DB has the old schema (stable/n_stable columns
    instead of stability/n_stable/recency_score).
    """
    conn = sqlite3.connect(corr_db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    # Detect old schema: lacks 'stability' column
    try:
        conn.execute('SELECT stability FROM correlations LIMIT 0')
    except sqlite3.OperationalError:
        logger.info('Migrating correlations.db to new stability schema...')
        conn.execute('DROP TABLE IF EXISTS correlations')
        conn.execute('DROP TABLE IF EXISTS correlation_runs')
        conn.commit()
    for stmt in _CORR_DB_SCHEMA.split(';'):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()
    conn.close()


def write_to_db(df: pd.DataFrame, meta: dict,
                corr_db_path: str, industry: str) -> None:
    """Replace all data for one industry in correlations.db (atomic transaction)."""
    run_at          = int(meta.get('generated_at', time.time()))
    train_period    = meta.get('train_period', ['', ''])
    backtest_period = meta.get('backtest_period', ['', ''])

    rows = []
    for _, r in df.iterrows():
        bk = r.get('backtest_r')
        rows.append((
            r['leader'],
            r['follower'],
            industry,
            int(r['lag_days']),
            r['direction'],
            float(r['train_r']),
            float(bk) if bk is not None and not pd.isna(bk) else None,
            float(r['fdr_p']),
            str(r['stability']),
            int(r['n_stable']),
            int(r['recency_score']),
            int(bool(r.get('market_adjusted', True))),
            run_at,
        ))

    run_row = (
        industry,
        run_at,
        int(meta.get('n_symbols_tested', 0)),
        int(meta.get('n_pairs_tested', 0)),
        int(meta.get('n_significant', 0)),
        int(meta.get('n_stable', 0)),
        train_period[0] if train_period else '',
        train_period[1] if len(train_period) > 1 else '',
        backtest_period[0] if backtest_period else None,
        backtest_period[1] if len(backtest_period) > 1 else None,
        int(meta.get('max_lag', 0)),
        float(meta.get('min_r', 0.0)),
        float(meta.get('elapsed_seconds', 0.0)),
    )

    conn = sqlite3.connect(corr_db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    try:
        with conn:
            conn.execute('DELETE FROM correlations WHERE industry = ?', (industry,))
            if rows:
                conn.executemany(
                    """INSERT INTO correlations
                       (leader, follower, industry, lag_days, direction,
                        train_r, backtest_r, fdr_p, stability, n_stable, recency_score,
                        market_adjusted, run_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    rows,
                )
            conn.execute(
                """INSERT OR REPLACE INTO correlation_runs
                   (industry, run_at, n_symbols, n_pairs_tested, n_significant,
                    n_stable, train_start, train_end, backtest_start, backtest_end,
                    max_lag, min_r, elapsed_seconds)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                run_row,
            )
    finally:
        conn.close()

    logger.info('DB: wrote %d rows for industry %r', len(rows), industry)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    db_path: str,
    output_dir: str | None,
    max_lag: int = 20,
    min_r: float = 0.15,
    fdr_alpha: float = 0.05,
    market_adjust: bool = True,
    device: str = None,
    symbols_hint: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Full correlation discovery pipeline. Returns (results_df, meta_dict).

    Args:
        output_dir:   Directory for CSV + JSON output. None = skip file writes
                      (used by run_industry_correlations which writes to SQLite).
        symbols_hint: Restrict the liquidity filter to this symbol list.
                      Used for per-industry runs to avoid loading the full universe.
    """
    if device is None:
        device = 'cuda' if torch.cuda.is_available() else 'cpu'

    logger.info('Starting correlation pipeline on %s', device)
    t0 = time.time()

    # ── 1. Liquidity filter ──────────────────────────────────────────────────
    symbols = _liquidity_filter(db_path, symbols_hint=symbols_hint)
    N = len(symbols)
    if N < 2:
        logger.warning('Fewer than 2 symbols after liquidity filter — skipping')
        empty = pd.DataFrame(columns=[
            'leader', 'follower', 'lag_days', 'direction',
            'train_r', 'backtest_r', 'fdr_p', 'stability', 'n_stable', 'recency_score',
            'market_adjusted',
        ])
        return empty, {'n_symbols_tested': N, 'n_significant': 0, 'n_stable': 0,
                       'generated_at': int(time.time()), 'elapsed_seconds': 0.0,
                       'train_period': ['', ''], 'backtest_period': ['', ''],
                       'max_lag': max_lag, 'min_r': min_r, 'n_pairs_tested': 0,
                       'market_adjusted': market_adjust, 'device': device}
    logger.info('N symbols after filter: %d', N)

    # ── 2. Load training pivot ───────────────────────────────────────────────
    logger.info('Loading training data...')
    pivot_train = _load_pivot(db_path, 'train', symbols)
    pivot_train = pivot_train.sort_index(axis=1)
    dates_train = list(pivot_train.columns)
    T_train = len(dates_train)
    logger.info('Train matrix: %d symbols × %d dates', N, T_train)

    # ── 3. Market adjustment ─────────────────────────────────────────────────
    if market_adjust:
        logger.info('Applying XAO market adjustment...')
        pivot_train = _market_adjust(pivot_train, db_path, 'train')

    # ── 4. GPU CCF ───────────────────────────────────────────────────────────
    logger.info('Computing CCF on %s...', device)
    ret_t = _to_tensor(pivot_train, device)
    r_all = compute_ccf_gpu(ret_t, max_lag).cpu().numpy()   # (N, N, max_lag)
    logger.info('CCF done in %.1fs', time.time() - t0)

    # ── 5. P-values ──────────────────────────────────────────────────────────
    logger.info('Computing p-values...')
    p_all = ccf_pvalues(r_all, max_lag, T_train)             # (N, N, max_lag)

    # ── 6. FDR correction (all pairs × lags, exclude self-pairs) ────────────
    n_tests = N * (N - 1) * max_lag
    logger.info('Applying FDR correction across %d tests...', n_tests)
    diag_mask = np.eye(N, dtype=bool)
    valid_mask = np.repeat(~diag_mask[:, :, np.newaxis], max_lag, axis=2)  # (N, N, max_lag)
    p_flat = p_all[valid_mask]
    _, p_adj_flat = fdr_correct(p_flat, alpha=fdr_alpha)
    p_adj_all = np.ones((N, N, max_lag), dtype=np.float64)
    p_adj_all[valid_mask] = p_adj_flat

    # ── 7. Significant triplets ──────────────────────────────────────────────
    sig_mask = (np.abs(r_all) >= min_r) & (p_adj_all <= fdr_alpha)
    n_sig = int(sig_mask.sum())
    logger.info('Significant (leader, follower, lag) triplets: %d', n_sig)

    # ── 8. Stability check ───────────────────────────────────────────────────
    logger.info('Running stability check (%d sub-periods)...', N_STABILITY_PERIODS)
    sig_periods = _stability_check(pivot_train, max_lag, min_r, fdr_alpha, device)
    # sig_periods: (N, N, max_lag, N_STABILITY_PERIODS)
    # Recency weights: index 0 = oldest (weight 1), index 4 = newest (weight 16)
    _weights = np.array([2**p for p in range(N_STABILITY_PERIODS)], dtype=np.int32)
    recency_scores = (sig_periods * _weights).sum(axis=-1).astype(np.int32)  # (N, N, max_lag)
    n_stable_arr   = sig_periods.sum(axis=-1).astype(np.int8)                # (N, N, max_lag)

    # ── 9. Backtest validation ───────────────────────────────────────────────
    logger.info('Running backtest validation...')
    r_bt = np.zeros_like(r_all)
    bt_start = bt_end = ''
    try:
        pivot_bt = _load_pivot(db_path, 'backtest', symbols)
        pivot_bt = pivot_bt.sort_index(axis=1)
        if market_adjust:
            pivot_bt = _market_adjust(pivot_bt, db_path, 'backtest')
        dates_bt = list(pivot_bt.columns)
        ret_bt = _to_tensor(pivot_bt, device)
        r_bt = compute_ccf_gpu(ret_bt, max_lag).cpu().numpy()
        if dates_bt:
            bt_start = dates_bt[0].strftime('%Y-%m-%d')
            bt_end   = dates_bt[-1].strftime('%Y-%m-%d')
        logger.info('Backtest: %d dates', len(dates_bt))
    except Exception as exc:
        logger.warning('Backtest validation failed: %s', exc)

    # ── 10. Build output rows ────────────────────────────────────────────────
    logger.info('Building output...')
    i_idx, j_idx, k_idx = np.where(sig_mask)
    rows = []
    for i, j, k in zip(i_idx, j_idx, k_idx):
        lag = int(k) + 1
        r_tr = float(r_all[i, j, k])
        r_bk = float(r_bt[i, j, k])
        rows.append({
            'leader':          symbols[i],
            'follower':        symbols[j],
            'lag_days':        lag,
            'direction':       'positive' if r_tr >= 0 else 'negative',
            'train_r':         round(r_tr, 6),
            'backtest_r':      round(r_bk, 6),
            'fdr_p':           round(float(p_adj_all[i, j, k]), 8),
            'stability':       ''.join(map(str, sig_periods[i, j, k])),
            'n_stable':        int(n_stable_arr[i, j, k]),
            'recency_score':   int(recency_scores[i, j, k]),
            'market_adjusted': market_adjust,
        })

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        df_out = pd.DataFrame(columns=[
            'leader', 'follower', 'lag_days', 'direction',
            'train_r', 'backtest_r', 'fdr_p', 'stability', 'n_stable', 'recency_score',
            'market_adjusted',
        ])

    # ── 11. Optionally save CSV + metadata ───────────────────────────────────
    train_start = dates_train[0].strftime('%Y-%m-%d') if dates_train else ''
    train_end   = dates_train[-1].strftime('%Y-%m-%d') if dates_train else ''
    meta = {
        'generated_at':     int(time.time()),
        'n_symbols_tested': N,
        'n_pairs_tested':   n_tests,
        'n_significant':    n_sig,
        'n_stable':         int((n_stable_arr[sig_mask] == N_STABILITY_PERIODS).sum()),
        'train_period':     [train_start, train_end],
        'backtest_period':  [bt_start, bt_end],
        'max_lag':          max_lag,
        'min_r':            min_r,
        'fdr_alpha':        fdr_alpha,
        'market_adjusted':  market_adjust,
        'device':           device,
        'elapsed_seconds':  round(time.time() - t0, 1),
    }

    if output_dir is not None:
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, 'correlations.csv')
        df_out.to_csv(csv_path, index=False)
        logger.info('Saved %d rows to %s', len(df_out), csv_path)

        meta_path = os.path.join(output_dir, 'correlations_meta.json')
        with open(meta_path, 'w') as f:
            json.dump(meta, f, indent=2)
        logger.info('Saved metadata to %s', meta_path)

    logger.info('Pipeline complete in %.1fs', time.time() - t0)
    return df_out, meta
