"""Build (N_sym × N_dates) GPU tensors from EOD + shorts DataFrames.

Parquet cache is keyed by (split, max_eod_date_hash) to auto-invalidate when DB updates.
"""

import hashlib
import os
import time
import numpy as np
import pandas as pd
import torch

DEVICE = 'cuda' if torch.cuda.is_available() else 'cpu'


class FeatureMatrix:
    """Pivot EOD + shorts into dense (N_sym, N_dates) GPU tensors.

    Args:
        eod_df:    long-format DataFrame from DataLoader.load_eod()
        shorts_df: long-format DataFrame from DataLoader.load_shorts()
        split:     'train' | 'backtest' | 'all' — used for cache keying
        cache_dir: directory for parquet cache files
    """

    def __init__(self, eod_df: pd.DataFrame, shorts_df: pd.DataFrame,
                 split: str = 'train', cache_dir: str = 'analysis/cache'):
        self.eod_df = eod_df
        self.shorts_df = shorts_df
        self.split = split
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        self._symbols: list[str] | None = None
        self._dates: np.ndarray | None = None
        self._mask: torch.Tensor | None = None
        self._features: dict[str, torch.Tensor] | None = None

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_key(self) -> str:
        max_date = self.eod_df['date'].max()
        h = hashlib.md5(f"{self.split}:{max_date}".encode()).hexdigest()[:12]
        return h

    def _cache_path(self, name: str) -> str:
        key = self._cache_key()
        return os.path.join(self.cache_dir, f"{name}_{key}.parquet")

    def _cache_exists(self, names: list[str]) -> bool:
        return all(os.path.exists(self._cache_path(n)) for n in names)

    def _save_cache(self, pivots: dict[str, pd.DataFrame]):
        for name, df in pivots.items():
            df.to_parquet(self._cache_path(name))

    def _load_cache(self, names: list[str]) -> dict[str, pd.DataFrame]:
        return {n: pd.read_parquet(self._cache_path(n)) for n in names}

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self) -> dict[str, torch.Tensor]:
        """Build all feature tensors. Returns dict of (N_sym, N_dates) tensors."""
        if self._features is not None:
            return self._features

        cache_names = ['close', 'open', 'high', 'low', 'volume', 'short_pct']
        if self._cache_exists(cache_names):
            print(f"[FeatureMatrix] Loading from parquet cache ({self.split})")
            pivots = self._load_cache(cache_names)
        else:
            print(f"[FeatureMatrix] Building pivot tables ({self.split})...")
            t0 = time.time()
            pivots = self._build_pivots()
            self._save_cache(pivots)
            print(f"[FeatureMatrix] Pivots built and cached in {time.time()-t0:.1f}s")

        self._assemble(pivots)
        return self._features

    def _build_pivots(self) -> dict[str, pd.DataFrame]:
        eod = self.eod_df.copy()

        # Deduplicate: keep last row per (date, symbol) — duplicates exist in EOD data
        before = len(eod)
        eod = eod.drop_duplicates(subset=['date', 'symbol'], keep='last')
        if len(eod) < before:
            print(f"[FeatureMatrix] Dropped {before - len(eod)} duplicate (date, symbol) rows")

        # Pivot each OHLCV field
        pivots = {}
        for col in ('open', 'high', 'low', 'close', 'volume'):
            pivots[col] = eod.pivot(index='date', columns='symbol', values=col)

        # Shorts: pivot and reindex to EOD dates
        eod_dates = pivots['close'].index
        if len(self.shorts_df) > 0:
            spiv = self.shorts_df.pivot(index='date', columns='symbol', values='short')
            spiv = spiv.reindex(eod_dates, method='ffill')
        else:
            spiv = pd.DataFrame(index=eod_dates, dtype=float)

        pivots['short_pct'] = spiv
        return pivots

    def _assemble(self, pivots: dict[str, pd.DataFrame]):
        """Align columns, compute derived features, move to GPU."""
        close_piv = pivots['close']
        symbols = close_piv.columns.tolist()
        dates_idx = close_piv.index  # DatetimeIndex

        self._symbols = symbols
        self._dates = (dates_idx.astype(np.int64) // 10**9).values  # unix timestamps

        N = len(symbols)
        T = len(dates_idx)

        def _to_tensor(df: pd.DataFrame, sym_list: list[str]) -> torch.Tensor:
            """Reindex to sym_list (columns) then convert to (N, T) float32 tensor."""
            aligned = df.reindex(columns=sym_list)
            arr = aligned.values.T.astype(np.float32)  # (N, T)
            return torch.from_numpy(arr).to(DEVICE)

        close_t = _to_tensor(close_piv, symbols)
        open_t  = _to_tensor(pivots['open'],   symbols)
        high_t  = _to_tensor(pivots['high'],   symbols)
        low_t   = _to_tensor(pivots['low'],    symbols)
        vol_t   = _to_tensor(pivots['volume'], symbols)

        # Short pct: only symbols present in shorts pivot
        short_piv = pivots['short_pct']
        short_t = _to_tensor(short_piv, symbols)  # NaN where no shorts data

        # Validity mask: close is non-NaN and volume > 0
        mask = ~torch.isnan(close_t) & (vol_t > 0)

        # Derived features
        returns = torch.full_like(close_t, float('nan'))
        valid_shift = mask[:, :-1] & mask[:, 1:]
        returns[:, 1:] = torch.where(
            valid_shift,
            (close_t[:, 1:] - close_t[:, :-1]) / close_t[:, :-1].clamp(min=1e-8),
            torch.tensor(float('nan'), device=DEVICE)
        )

        log_returns = torch.full_like(close_t, float('nan'))
        log_returns[:, 1:] = torch.where(
            valid_shift,
            torch.log((close_t[:, 1:] / close_t[:, :-1].clamp(min=1e-8)).clamp(min=1e-10)),
            torch.tensor(float('nan'), device=DEVICE)
        )

        hl_spread = torch.where(
            mask & ~torch.isnan(high_t) & ~torch.isnan(low_t),
            (high_t - low_t) / close_t.clamp(min=1e-8),
            torch.tensor(float('nan'), device=DEVICE)
        )

        gap = torch.full_like(close_t, float('nan'))
        gap[:, 1:] = torch.where(
            valid_shift & ~torch.isnan(open_t[:, 1:]),
            (open_t[:, 1:] - close_t[:, :-1]) / close_t[:, :-1].clamp(min=1e-8),
            torch.tensor(float('nan'), device=DEVICE)
        )

        self._mask = mask
        self._features = {
            'close':       close_t,
            'open':        open_t,
            'high':        high_t,
            'low':         low_t,
            'returns':     returns,
            'log_returns': log_returns,
            'volume':      vol_t,
            'short_pct':   short_t,
            'hl_spread':   hl_spread,
            'gap':         gap,
        }

    @property
    def mask(self) -> torch.Tensor:
        if self._mask is None:
            self.build()
        return self._mask

    @property
    def symbols(self) -> list[str]:
        if self._symbols is None:
            self.build()
        return self._symbols

    @property
    def dates(self) -> np.ndarray:
        if self._dates is None:
            self.build()
        return self._dates

    def symbol_index(self, sym: str) -> int:
        return self.symbols.index(sym)
