# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Compute per-pair features from aligned warrant + underlying price series."""

import numpy as np
from datetime import date, datetime


def compute_features(pair: dict) -> dict | None:
    """Compute derived features for a (warrant, underlying) pair.

    Returns dict of aligned 1-D NumPy arrays (length n), or None if insufficient data.
    """
    wp = pair['warrant_df']
    up = pair['underlying_df']
    expiry: date = pair['expiry']
    strike: float = pair['strike']
    call_put: str = pair['call_put']

    dates_unix = wp['date'].values.astype(np.int64)
    w_close = wp['close'].values.astype(np.float64)
    u_close = up['close'].values.astype(np.float64)
    u_vol   = up['volume'].values.astype(np.float64)
    shorts  = up['shorts_pct'].values.astype(np.float64)
    n = len(dates_unix)

    if n < 20 or np.all(w_close <= 0.001):
        return None

    # Days to expiry at each date
    dte = np.array([
        max(0, (expiry - datetime.fromtimestamp(int(d)).date()).days)
        for d in dates_unix
    ], dtype=np.float64)

    # Moneyness and intrinsic value
    if call_put == 'C':
        moneyness = u_close / np.maximum(strike, 1e-6)
        intrinsic = np.maximum(0.0, u_close - strike)
    else:
        moneyness = np.where(u_close > 0, strike / u_close, np.nan)
        intrinsic = np.maximum(0.0, strike - u_close)

    # Premium = time value (warrant price minus intrinsic)
    premium = np.maximum(0.0, w_close - intrinsic)

    # Premium ratio = premium / underlying price
    premium_ratio = np.where(u_close > 0.001, premium / u_close, np.nan)

    # Log returns (clip to handle illiquid gaps)
    w_ret = np.full(n, np.nan)
    u_ret = np.full(n, np.nan)
    with np.errstate(divide='ignore', invalid='ignore'):
        w_ret[1:] = np.clip(np.log(w_close[1:] / np.maximum(w_close[:-1], 1e-6)), -1.0,  1.0)
        u_ret[1:] = np.clip(np.log(u_close[1:] / np.maximum(u_close[:-1], 1e-6)), -0.5,  0.5)

    # Actual delta: rolling 10-day ratio of returns (warrant / underlying)
    actual_delta = np.full(n, np.nan)
    for t in range(10, n):
        wr = w_ret[t - 9:t + 1]
        ur = u_ret[t - 9:t + 1]
        valid = ~np.isnan(wr) & ~np.isnan(ur) & (np.abs(ur) > 5e-4)
        if valid.sum() < 5:
            continue
        actual_delta[t] = np.nanmean(wr[valid] / ur[valid])

    # Staleness: price unchanged for 5 consecutive days
    stale = np.zeros(n, dtype=bool)
    for t in range(5, n):
        if np.all(w_close[t - 4:t + 1] == w_close[t]):
            stale[t] = True

    return {
        'dates':         dates_unix,
        'dte':           dte,
        'moneyness':     moneyness,
        'w_close':       w_close,
        'u_close':       u_close,
        'u_vol':         u_vol,
        'intrinsic':     intrinsic,
        'premium':       premium,
        'premium_ratio': premium_ratio,
        'w_ret':         w_ret,
        'u_ret':         u_ret,
        'actual_delta':  actual_delta,
        'shorts_pct':    shorts,
        'stale':         stale,
    }
