# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""ShortTrendSignal: negated rolling slope of short% → bearish when shorts rising."""

import torch
from .base import Signal
from ..core.gpu_ops import rolling_slope, cross_sectional_rank


class ShortTrendSignal(Signal):
    name = "short_trend"
    description = "Negated 20-day slope of short interest; bullish when shorts declining"
    required_features = ['short_pct']

    def __init__(self, window: int = 20):
        self.window = window

    def compute(self, features: dict[str, torch.Tensor], mask: torch.Tensor) -> torch.Tensor:
        short_pct = features['short_pct']
        # Mask: only where we have short data
        short_mask = mask & ~torch.isnan(short_pct)

        slope = rolling_slope(short_pct, self.window)
        # Negate: rising shorts = bearish = negative signal
        signal = -slope
        # Replace invalid with NaN
        signal[~short_mask] = float('nan')
        # Cross-sectional rank [0,1]
        return cross_sectional_rank(signal, short_mask)
