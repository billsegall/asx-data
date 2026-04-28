# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""ShortSqueezeSignal: high short% + recent positive returns → squeeze candidate."""

import torch
from .base import Signal
from ..core.gpu_ops import rolling_zscore_fast, cross_sectional_rank


class ShortSqueezeSignal(Signal):
    name = "short_squeeze"
    description = "High short interest + upward price momentum → potential squeeze"
    required_features = ['short_pct', 'returns']

    def __init__(self, short_window: int = 60, return_window: int = 5, top_decile: float = 0.8):
        self.short_window = short_window
        self.return_window = return_window
        self.top_decile = top_decile

    def compute(self, features: dict[str, torch.Tensor], mask: torch.Tensor) -> torch.Tensor:
        short_pct = features['short_pct']
        returns = features['returns']
        device = short_pct.device

        short_mask = mask & ~torch.isnan(short_pct)
        ret_mask = mask & ~torch.isnan(returns)

        # Rolling sum of returns over return_window (recent momentum)
        N, T = returns.shape
        ret_filled = returns.clone()
        ret_filled[~ret_mask] = 0.0
        # Cumulative rolling return (approx, not compounded)
        recent_return = torch.zeros_like(returns)
        for lag in range(self.return_window):
            if lag < T:
                recent_return[:, lag:] += ret_filled[:, :T - lag]
        recent_return[:, :self.return_window] = float('nan')

        # Cross-sectional rank of short_pct (high = top decile candidate)
        short_rank = cross_sectional_rank(short_pct, short_mask)
        # Cross-sectional rank of recent momentum
        momentum_rank = cross_sectional_rank(recent_return, mask)

        # Squeeze signal: product of (high short rank) × (positive momentum rank)
        # Both must be in top_decile for signal to fire
        valid = short_mask & ~torch.isnan(momentum_rank)
        squeeze = torch.where(
            valid & (short_rank >= self.top_decile),
            momentum_rank,
            torch.tensor(float('nan'), device=device)
        )
        return squeeze
