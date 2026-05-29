# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Four warrant signals + composite scorer.

All signals return a 1-D float64 array (same length as features['dates']).
Higher value = stronger buy signal. NaN = no signal at that date.
"""

import numpy as np


# ---------------------------------------------------------------------------
# Rolling helpers
# ---------------------------------------------------------------------------

def _rolling_zscore(x: np.ndarray, window: int) -> np.ndarray:
    out = np.full_like(x, np.nan)
    for t in range(window - 1, len(x)):
        chunk = x[t - window + 1:t + 1]
        valid = ~np.isnan(chunk)
        if valid.sum() < max(window // 2, 5):
            continue
        mu = np.nanmean(chunk)
        std = np.nanstd(chunk, ddof=1)
        if std < 1e-8:
            continue
        out[t] = (x[t] - mu) / std
    return out


def _rolling_sum(x: np.ndarray, window: int) -> np.ndarray:
    out = np.full_like(x, np.nan)
    for t in range(window - 1, len(x)):
        chunk = x[t - window + 1:t + 1]
        valid_count = np.sum(~np.isnan(chunk))
        if valid_count < window // 2:
            continue
        out[t] = np.nansum(chunk)
    return out


# ---------------------------------------------------------------------------
# Signal 1: Premium Compression
# ---------------------------------------------------------------------------

def score_premium_compression(feat: dict) -> np.ndarray:
    """Unusually compressed premium ratio vs 60-day history.

    Hypothesis: cheap time value + reasonable moneyness → warrant likely to
    expand when underlying moves. Higher score = more compressed (better).

    Active when moneyness in [0.6, 1.4] and DTE >= 20.
    """
    pr = feat['premium_ratio'].copy()
    pr[feat['moneyness'] < 0.6] = np.nan
    pr[feat['moneyness'] > 1.4] = np.nan
    pr[feat['dte'] < 20]        = np.nan

    z = _rolling_zscore(pr, 60)
    return -z   # inverted: low premium = high z-inversion = positive score


# ---------------------------------------------------------------------------
# Signal 2: Underlying Momentum
# ---------------------------------------------------------------------------

def score_underlying_momentum(feat: dict) -> np.ndarray:
    """Positive underlying momentum not yet reflected in warrant price.

    Combines 5-day and 20-day return z-scores on the underlying.
    For calls: bullish momentum → higher score.
    Muted for DTE < 15.
    """
    u_ret = feat['u_ret']

    mom5  = _rolling_sum(u_ret, 5)
    mom20 = _rolling_sum(u_ret, 20)

    z5  = _rolling_zscore(mom5,  60)
    z20 = _rolling_zscore(mom20, 60)

    score = 0.6 * np.where(np.isnan(z5),  0.0, z5) \
          + 0.4 * np.where(np.isnan(z20), 0.0, z20)
    score[np.isnan(z5) & np.isnan(z20)] = np.nan
    score[feat['dte'] < 15] = np.nan
    return score


# ---------------------------------------------------------------------------
# Signal 3: Delta Drift
# ---------------------------------------------------------------------------

def score_delta_drift(feat: dict) -> np.ndarray:
    """Actual delta persistently below theoretical → warrant lagging.

    Theoretical delta for calls: ~0.5 at ATM, scaling linearly with moneyness.
    Only for calls with moneyness [0.8, 1.2] and DTE >= 15.
    """
    actual_delta    = feat['actual_delta'].copy()
    moneyness       = feat['moneyness']

    # Linear approximation: 0.5 at ATM, 0 at m=0.5, 1 at m=1.5
    theoretical     = np.clip(0.5 + 0.5 * (moneyness - 1.0), 0.05, 0.95)
    delta_gap       = theoretical - actual_delta

    delta_gap[moneyness < 0.8]  = np.nan
    delta_gap[moneyness > 1.2]  = np.nan
    delta_gap[feat['dte'] < 15] = np.nan

    return _rolling_zscore(delta_gap, 30)


# ---------------------------------------------------------------------------
# Signal 4: Implied Volatility vs Historical Volatility
# ---------------------------------------------------------------------------

def score_implied_vol(feat: dict) -> np.ndarray:
    """Warrant IV cheap relative to underlying historical vol.

    IV approximation (Brenner-Subrahmanyam): IV ≈ premium_ratio / sqrt(T) * sqrt(2π)
    Signal: hist_vol / IV → high ratio = cheap option.

    Active for moneyness [0.85, 1.15], DTE [30, 270].
    """
    pr        = feat['premium_ratio'].copy()
    dte       = feat['dte']
    moneyness = feat['moneyness']
    u_ret     = feat['u_ret']
    n         = len(pr)

    # Implied vol
    T_years = np.maximum(dte / 365.0, 1.0 / 365.0)
    iv = pr / np.sqrt(T_years) * np.sqrt(2 * np.pi)
    iv = np.clip(iv, 0.01, 5.0)

    # 30-day historical vol of underlying
    hist_vol = np.full(n, np.nan)
    for t in range(30, n):
        chunk = u_ret[t - 29:t + 1]
        valid = ~np.isnan(chunk)
        if valid.sum() < 20:
            continue
        hist_vol[t] = np.nanstd(chunk, ddof=1) * np.sqrt(252)

    ratio = hist_vol / np.where(iv > 0, iv, np.nan)

    ratio[moneyness < 0.85]  = np.nan
    ratio[moneyness > 1.15]  = np.nan
    ratio[dte < 30]          = np.nan
    ratio[dte > 270]         = np.nan

    return _rolling_zscore(ratio, 60)


# ---------------------------------------------------------------------------
# Composite
# ---------------------------------------------------------------------------

def compute_all_signals(feat: dict) -> dict:
    """Compute all four signals. NaN on stale-price dates."""
    stale = feat['stale']

    signals = {
        'premium_compression': score_premium_compression(feat),
        'underlying_momentum': score_underlying_momentum(feat),
        'delta_drift':         score_delta_drift(feat),
        'implied_vol':         score_implied_vol(feat),
    }
    for arr in signals.values():
        arr[stale] = np.nan

    return signals
