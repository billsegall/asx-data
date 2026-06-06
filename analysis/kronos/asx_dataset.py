# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Multi-symbol ASX Dataset for Kronos fine-tuning.

Loads pre-TRAIN_CUTOFF OHLCV from SQLite, one window per sample.
Windows are entirely within a single symbol's history (no cross-symbol contamination).
Returns (x_tensor, x_stamp_tensor) matching Kronos training format.
"""

import random
import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

from analysis.kronos.loader import load_all_ohlcv

TRAIN_CUTOFF = '2025-03-01'
_FEATURES = ['open', 'high', 'low', 'close', 'volume', 'amount']
_STAMP_COLS = ['minute', 'hour', 'weekday', 'day', 'month']


def _build_windows(ohlcv: dict[str, pd.DataFrame], window: int, val_frac: float, split: str) -> list[tuple]:
    """Return list of (array_x, array_stamp) tuples, each shape (window, 6) and (window, 5)."""
    cutoff = pd.Timestamp(TRAIN_CUTOFF)
    samples = []

    for sym, df in ohlcv.items():
        df = df[df.index < cutoff].copy()
        if len(df) < window + 1:
            continue

        df['amount'] = df['close'] * df['volume']

        df['minute'] = 0
        df['hour'] = 0
        df['weekday'] = df.index.weekday
        df['day'] = df.index.day
        df['month'] = df.index.month

        n = len(df)
        split_idx = int(n * (1 - val_frac))

        if split == 'train':
            sub = df.iloc[:split_idx]
        else:
            sub = df.iloc[split_idx:]

        if len(sub) < window:
            continue

        x_arr = sub[_FEATURES].values.astype(np.float32)
        s_arr = sub[_STAMP_COLS].values.astype(np.float32)

        for start in range(len(sub) - window + 1):
            samples.append((x_arr[start:start + window], s_arr[start:start + window]))

    return samples


class ASXKronosDataset(Dataset):
    """Sliding-window dataset over all qualifying ASX symbols, pre-TRAIN_CUTOFF."""

    def __init__(
        self,
        db_path: str,
        split: str = 'train',
        lookback: int = 128,
        predict_len: int = 10,
        val_frac: float = 0.15,
        clip: float = 5.0,
        seed: int = 42,
        ohlcv: dict | None = None,
    ):
        assert split in ('train', 'val')
        self.window = lookback + predict_len + 1
        self.clip = clip

        if ohlcv is None:
            ohlcv = load_all_ohlcv(db_path)

        self.samples = _build_windows(ohlcv, self.window, val_frac, split)

        rng = random.Random(seed)
        rng.shuffle(self.samples)

        print(f"[{split.upper()}] {len(self.samples):,} windows from {len(ohlcv)} symbols")

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int):
        x, stamp = self.samples[idx]

        mean = x.mean(axis=0)
        std = x.std(axis=0)
        x = (x - mean) / (std + 1e-5)
        x = np.clip(x, -self.clip, self.clip)

        return torch.from_numpy(x), torch.from_numpy(stamp)
