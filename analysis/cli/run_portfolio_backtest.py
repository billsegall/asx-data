#!/usr/bin/env python3
"""Portfolio backtest: pick top-10 per signal at training cutoff, track $1000/stock over backtest period.

Entry date  = last trading day in training set (~2025-02-28)
Backtest    = 2025-03-01 onwards (~1 year of real out-of-sample data)
Investment  = $1000 per stock, 10 stocks per signal = $10,000 per signal

Usage:
    python -m analysis.cli.run_portfolio_backtest --db stockdb/stockdb.db
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import numpy as np
import pandas as pd
import torch

from analysis.core.data_loader import DataLoader
from analysis.core.feature_matrix import FeatureMatrix
from analysis.core.gpu_ops import gpu_monitor
from analysis.signals import ShortTrendSignal, ShortSqueezeSignal, VolumeAnomalySignal

INVESTMENT_PER_STOCK = 1000.0
TOP_N = 10

SIGNALS = [
    ShortTrendSignal(),
    ShortSqueezeSignal(),
    VolumeAnomalySignal(),
]


def compute_top10(signal, fm_train):
    """Return top-N symbols by signal score on the last training date."""
    features = fm_train.build()
    mask = fm_train.mask
    sig = signal.compute(features, mask)   # (N_sym, N_dates)

    # Use the last date that has signal data
    N, T = sig.shape
    scores_last = sig[:, -1].cpu().numpy()
    symbols = fm_train.symbols

    ranked = sorted(
        [(symbols[i], float(scores_last[i])) for i in range(N) if not np.isnan(scores_last[i])],
        key=lambda x: x[1], reverse=True
    )
    return ranked[:TOP_N]


def build_portfolio_series(top10_symbols, bt_eod_df, sym_meta):
    """For each symbol, compute daily $1000 holding value over backtest period.

    Returns dict ready for JSON serialisation.
    """
    # Build a close price pivot for just the relevant symbols
    syms = [s for s, _ in top10_symbols]
    df = bt_eod_df[bt_eod_df['symbol'].isin(syms)].copy()
    df = df.drop_duplicates(subset=['date', 'symbol'], keep='last')

    pivot = df.pivot(index='date', columns='symbol', values='close').sort_index()
    pivot.index = pivot.index  # already DatetimeIndex from DataLoader

    # Dates as ms timestamps for JS/Plotly
    dates_ms = [int(d.timestamp() * 1000) for d in pivot.index]

    holdings = []
    total_by_date = np.zeros(len(pivot))

    for sym, score in top10_symbols:
        if sym not in pivot.columns:
            continue
        prices = pivot[sym].values.astype(float)

        # Entry price = first valid price in backtest period
        valid_idx = np.where(~np.isnan(prices))[0]
        if len(valid_idx) == 0:
            continue
        entry_idx = valid_idx[0]
        entry_price = prices[entry_idx]
        if entry_price <= 0:
            continue

        # Value of $1000 holding at each date
        values = np.where(
            ~np.isnan(prices),
            INVESTMENT_PER_STOCK * prices / entry_price,
            np.nan
        )
        # Forward-fill value when no price (e.g. no trading on that day)
        for i in range(1, len(values)):
            if np.isnan(values[i]):
                values[i] = values[i - 1]
        # Before entry date: no position
        values[:entry_idx] = np.nan

        total_by_date += np.where(np.isnan(values), 0, values)

        meta = sym_meta.get(sym, {})
        holdings.append({
            'symbol': sym,
            'name': meta.get('name', ''),
            'industry': meta.get('industry', ''),
            'score': round(score, 4),
            'entry_price': round(float(entry_price), 4),
            'entry_date_ms': dates_ms[entry_idx] if entry_idx < len(dates_ms) else None,
            'current_price': round(float(prices[valid_idx[-1]]), 4) if len(valid_idx) else None,
            'current_value': round(float(values[valid_idx[-1]]), 2) if len(valid_idx) else None,
            'pnl': round(float(values[valid_idx[-1]]) - INVESTMENT_PER_STOCK, 2) if len(valid_idx) else None,
            'pnl_pct': round((float(values[valid_idx[-1]]) / INVESTMENT_PER_STOCK - 1) * 100, 2) if len(valid_idx) else None,
            'values': [round(float(v), 2) if not np.isnan(v) else None for v in values],
        })

    total_list = [round(float(v), 2) if v > 0 else None for v in total_by_date]

    return {
        'holdings': holdings,
        'dates_ms': dates_ms,
        'total': total_list,
        'invested': INVESTMENT_PER_STOCK * len(holdings),
        'current_total': round(float(total_by_date[-1]), 2) if len(total_by_date) else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--output-dir', default='analysis/results')
    args = parser.parse_args()

    print(f"[portfolio_backtest] GPU: {gpu_monitor()}")

    # --- Load training data (for signal scoring at cutoff) ---
    print("[portfolio_backtest] Loading training EOD + shorts...")
    t0 = time.time()
    train_loader = DataLoader(args.db, split='train')
    train_eod = train_loader.load_eod(min_history_days=252)
    train_shorts = train_loader.load_shorts()
    print(f"[portfolio_backtest] Training data loaded in {time.time()-t0:.1f}s")

    fm_train = FeatureMatrix(train_eod, train_shorts, split='train', cache_dir='analysis/cache')
    fm_train.build()  # warm cache

    entry_ts = int(fm_train.dates[-1])
    entry_date_str = pd.Timestamp(entry_ts, unit='s').strftime('%Y-%m-%d')
    print(f"[portfolio_backtest] Entry date (last training day): {entry_date_str}")

    # Load symbol metadata
    symbols_df = train_loader.load_symbols()
    sym_meta = {row['symbol']: {'name': row['name'], 'industry': row['industry']}
                for _, row in symbols_df.iterrows()}

    # --- Load backtest EOD prices ---
    print("[portfolio_backtest] Loading backtest EOD prices...")
    bt_loader = DataLoader(args.db, split='backtest')
    bt_eod = bt_loader.load_eod(min_history_days=0)
    print(f"[portfolio_backtest] Backtest rows: {len(bt_eod)}")

    # --- Compute signal scores at cutoff, pick top 10, track portfolio ---
    os.makedirs(args.output_dir, exist_ok=True)
    result = {
        'generated_at': int(time.time()),
        'entry_date': entry_date_str,
        'investment_per_stock': INVESTMENT_PER_STOCK,
        'top_n': TOP_N,
        'signals': {},
    }

    for signal in SIGNALS:
        print(f"\n[portfolio_backtest] Signal: {signal.name}")
        top10 = compute_top10(signal, fm_train)
        print(f"  Top {TOP_N}: {[s for s, _ in top10]}")

        series = build_portfolio_series(top10, bt_eod, sym_meta)
        n_held = len(series['holdings'])
        invested = series['invested']
        current = series['current_total']
        pnl_pct = (current / invested - 1) * 100 if invested > 0 else 0
        print(f"  Invested: ${invested:,.0f}  Current: ${current:,.0f}  P&L: {pnl_pct:+.1f}%")

        result['signals'][signal.name] = series
        torch.cuda.empty_cache()

    # --- Combined portfolio across all signals (deduplicated by symbol) ---
    all_syms_seen = {}
    all_dates_ms = None
    for sig_name, sig_data in result['signals'].items():
        if all_dates_ms is None:
            all_dates_ms = sig_data['dates_ms']
        for h in sig_data['holdings']:
            if h['symbol'] not in all_syms_seen:
                all_syms_seen[h['symbol']] = h

    combined_total = np.zeros(len(all_dates_ms) if all_dates_ms else 0)
    combined_holdings = list(all_syms_seen.values())
    for h in combined_holdings:
        vals = np.array([v if v is not None else np.nan for v in h['values']])
        combined_total += np.where(np.isnan(vals), 0, vals)

    result['combined'] = {
        'holdings': combined_holdings,
        'dates_ms': all_dates_ms or [],
        'total': [round(float(v), 2) if v > 0 else None for v in combined_total],
        'invested': INVESTMENT_PER_STOCK * len(combined_holdings),
        'current_total': round(float(combined_total[-1]), 2) if len(combined_total) else None,
    }
    combined_pnl = (result['combined']['current_total'] / result['combined']['invested'] - 1) * 100
    print(f"\n[portfolio_backtest] Combined: {len(combined_holdings)} stocks  "
          f"Invested: ${result['combined']['invested']:,.0f}  "
          f"Current: ${result['combined']['current_total']:,.0f}  "
          f"P&L: {combined_pnl:+.1f}%")

    out_path = os.path.join(args.output_dir, 'portfolio_backtest.json')
    with open(out_path, 'w') as f:
        json.dump(result, f)
    print(f"\n[portfolio_backtest] Saved → {out_path}")


if __name__ == '__main__':
    main()
