# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""AnnouncementSignal: placeholder — future earnings/guidance signal."""

import torch
from .base import Signal


class AnnouncementSignal(Signal):
    name = "announcement"
    description = "Earnings/guidance announcement signal (placeholder)"
    required_features = ['returns']

    def compute(self, features: dict[str, torch.Tensor], mask: torch.Tensor) -> torch.Tensor:
        return torch.full_like(features['returns'], float('nan'))
