# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Simple warrant backtest and IC sweep.

Uses the same TRAIN_CUTOFF_TS as the equity analysis framework.
All entry/exit logic is strictly on the backtest period (>= TRAIN_CUTOFF_TS).
"""

import numpy as np
from scipy import stats

from ..core.train_test_split import TRAIN_CUTOFF_TS
from .signals.core import compute_all_signals

SIGNAL_NAMES = ['premium_compression', 'underlying_momentum', 'delta_drift', 'implied_vol']


def backtest_signal(
    feat: dict,
    signal_scores: np.ndarray,
    *,
    entry_z: float = 0.5,
    hold_days: int = 10,
    round_trip_cost: float = 0.03,
) -> dict:
    """Backtest a single signal on one (warrant, underlying) pair.

    Only enters on dates in the backtest period (>= TRAIN_CUTOFF_TS).
    Positions do not overlap.

    Returns dict: n_trades, hit_rate, avg_gross, avg_net, sharpe.
    """
    dates  = feat['dates']
    w_ret  = feat['w_ret']
    dte    = feat['dte']
    n      = len(dates)

    in_bt = dates >= TRAIN_CUTOFF_TS
    trades_gross = []
    last_exit = -1

    for t in range(n - hold_days):
        if not in_bt[t]:
            continue
        if t <= last_exit:
            continue
        if np.isnan(signal_scores[t]) or signal_scores[t] < entry_z:
            continue
        if dte[t] < 15:
            continue

        fwd = w_ret[t + 1:t + 1 + hold_days]
        valid = ~np.isnan(fwd)
        if valid.sum() < hold_days // 2:
            continue

        gross = float(np.nansum(fwd))
        trades_gross.append(gross)
        last_exit = t + hold_days

    if not trades_gross:
        return {'n_trades': 0, 'hit_rate': 0.0, 'avg_gross': 0.0, 'avg_net': 0.0, 'sharpe': 0.0}

    gross = np.array(trades_gross)
    net   = gross - round_trip_cost

    return {
        'n_trades': len(gross),
        'hit_rate': float((gross > 0).mean()),
        'avg_gross': float(gross.mean()),
        'avg_net':   float(net.mean()),
        'sharpe':    float(net.mean() / (net.std(ddof=1) + 1e-10)) if len(net) > 1 else 0.0,
    }


def run_ic_sweep(pairs_features: list, fwd_days: tuple = (3, 5, 10)) -> dict:
    """Compute Spearman IC for each signal × forward horizon across all pairs.

    Only uses training data (< TRAIN_CUTOFF_TS) to avoid lookahead.

    Returns {signal_name: {fwd_N: {ic, n, p_value}}}.
    """
    ic_data = {s: {f'fwd_{d}': ([], []) for d in fwd_days} for s in SIGNAL_NAMES}

    for feat in pairs_features:
        in_train = feat['dates'] < TRAIN_CUTOFF_TS
        sigs = compute_all_signals(feat)
        w_ret = feat['w_ret']
        n = len(feat['dates'])

        for sig_name, scores in sigs.items():
            for d in fwd_days:
                key = f'fwd_{d}'
                for t in range(n - d):
                    if not in_train[t]:
                        continue
                    if np.isnan(scores[t]):
                        continue
                    fwd = np.nansum(w_ret[t + 1:t + 1 + d])
                    if np.isnan(fwd):
                        continue
                    ic_data[sig_name][key][0].append(scores[t])
                    ic_data[sig_name][key][1].append(fwd)

    summary = {}
    for sig_name in SIGNAL_NAMES:
        summary[sig_name] = {}
        for fwd_key in [f'fwd_{d}' for d in fwd_days]:
            score_list, fwd_list = ic_data[sig_name][fwd_key]
            if len(score_list) < 20:
                summary[sig_name][fwd_key] = {'ic': 0.0, 'ic_ir': 0.0, 'n': 0, 'p_value': 1.0}
                continue
            ic, p = stats.spearmanr(score_list, fwd_list)
            # IC-IR: ic / std across 30-day buckets approximation
            arr = np.array(score_list)
            ic_ir = float(ic) / (arr.std(ddof=1) / np.sqrt(len(arr)) + 1e-10) if arr.std() > 0 else 0.0
            summary[sig_name][fwd_key] = {
                'ic':      round(float(ic), 4),
                'ic_ir':   round(ic_ir, 3),
                'n':       len(score_list),
                'p_value': round(float(p), 4),
            }

    return summary
