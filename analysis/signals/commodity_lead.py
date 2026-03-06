"""CommodityLeadSignal: placeholder — commodity price trend → sector leads.

Fetches GC=F (gold), CL=F (crude oil), HG=F (copper) from yfinance.
Maps to ASX symbols via GICS industry in symbols table.
"""

import torch
from .base import Signal


class CommodityLeadSignal(Signal):
    name = "commodity_lead"
    description = "Commodity price momentum → matched GICS sector signal (placeholder)"
    required_features = ['returns']

    COMMODITY_SECTOR_MAP = {
        'GC=F': ['Gold', 'Metals & Mining'],
        'CL=F': ['Energy', 'Oil, Gas & Consumable Fuels'],
        'HG=F': ['Copper', 'Metals & Mining'],
    }

    def compute(self, features: dict[str, torch.Tensor], mask: torch.Tensor) -> torch.Tensor:
        # Placeholder: returns zeros (no signal)
        return torch.full_like(features['returns'], float('nan'))
