# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Multi-lag IC sweep with FDR correction.

Sweeps all (feature, lag) pairs, computes Spearman IC series + stats,
applies Benjamini-Hochberg FDR correction across all tests.
"""

import time
import numpy as np
import pandas as pd
import torch

from ..core.gpu_ops import compute_ic_series, compute_ic_stats, rolling_zscore_fast, rolling_slope
from .fdr_correction import fdr_correct


# Features derived from base tensors for IC sweep
def _derived_features(features: dict[str, torch.Tensor], mask: torch.Tensor) -> dict[str, torch.Tensor]:
    """Compute all candidate features for the IC sweep."""
    derived = {}
    ret = features.get('returns')
    vol = features.get('volume')
    short = features.get('short_pct')
    close = features.get('close')
    hl = features.get('hl_spread')
    gap = features.get('gap')

    if ret is not None:
        derived['returns_1d'] = ret
        derived['returns_z20'] = rolling_zscore_fast(ret, 20)
        derived['returns_z5'] = rolling_zscore_fast(ret, 5)
        derived['returns_slope20'] = rolling_slope(ret, 20)

    if vol is not None:
        log_vol = torch.log1p(vol.float())
        log_vol[~mask] = float('nan')
        derived['log_volume'] = log_vol
        derived['volume_z20'] = rolling_zscore_fast(log_vol, 20)
        derived['volume_z5'] = rolling_zscore_fast(log_vol, 5)

    if short is not None:
        short_m = short.clone()
        short_m[~mask] = float('nan')
        derived['short_pct'] = short_m
        derived['short_z20'] = rolling_zscore_fast(short_m, 20)
        derived['short_slope20'] = rolling_slope(short_m, 20)

    if hl is not None:
        derived['hl_spread'] = hl
        derived['hl_z20'] = rolling_zscore_fast(hl, 20)

    if gap is not None:
        derived['gap'] = gap

    return derived


class ICSweep:
    """Sweep all (feature × lag) pairs and rank by IC_IR after FDR correction.

    Args:
        features: base feature dict from FeatureMatrix.build()
        forward_returns: (N_sym, N_dates) forward return tensor
        mask: (N_sym, N_dates) validity mask
        max_lag: maximum forward lag to test (days)
        fdr_alpha: FDR threshold for significance
    """

    def __init__(
        self,
        features: dict[str, torch.Tensor],
        forward_returns: torch.Tensor,
        mask: torch.Tensor,
        max_lag: int = 20,
        fdr_alpha: float = 0.05,
    ):
        self.features = features
        self.forward_returns = forward_returns
        self.mask = mask
        self.max_lag = max_lag
        self.fdr_alpha = fdr_alpha
        self._results: pd.DataFrame | None = None

    def run(self) -> pd.DataFrame:
        """Run IC sweep. Returns DataFrame ranked by abs(IC_IR)."""
        if self._results is not None:
            return self._results

        print("[ICSweep] Computing derived features...")
        derived = _derived_features(self.features, self.mask)
        feature_names = sorted(derived.keys())
        lags = list(range(1, self.max_lag + 1))

        rows = []
        total = len(feature_names) * len(lags)
        print(f"[ICSweep] Sweeping {len(feature_names)} features × {len(lags)} lags = {total} tests")
        t0 = time.time()

        for fi, fname in enumerate(feature_names):
            feat = derived[fname]
            for lag in lags:
                ic_series = compute_ic_series(feat, self.forward_returns, lag, self.mask)
                stats = compute_ic_stats(ic_series)
                rows.append(dict(feature=fname, lag=lag, **stats))

            elapsed = time.time() - t0
            eta = elapsed / (fi + 1) * (len(feature_names) - fi - 1)
            print(f"  [{fi+1}/{len(feature_names)}] {fname} — {elapsed:.1f}s elapsed, ~{eta:.0f}s remaining")

        df = pd.DataFrame(rows)

        # FDR correction across all tests
        p_values = df['p_value'].fillna(1.0).values
        reject, corrected_p = fdr_correct(p_values, alpha=self.fdr_alpha)
        df['fdr_corrected_p'] = corrected_p
        df['fdr_significant'] = reject

        # Sort by abs IC_IR descending
        df['abs_ic_ir'] = df['ic_ir'].abs()
        df = df.sort_values('abs_ic_ir', ascending=False).reset_index(drop=True)

        self._results = df
        print(f"[ICSweep] Done in {time.time()-t0:.1f}s. {reject.sum()} significant pairs (FDR α={self.fdr_alpha})")
        return df

    def top_signals(self, n: int = 20) -> list[tuple[str, int]]:
        """Return top-n (feature, lag) pairs that are FDR-significant."""
        df = self.run()
        sig = df[df['fdr_significant']].head(n)
        return list(zip(sig['feature'], sig['lag']))
