# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""BacktestEngine: train/backtest isolation enforced at runtime.

fit_threshold() only uses fm_train.
run() only uses fm_backtest — raises ValueError if passed training data.
"""

import math
import numpy as np
import pandas as pd
import torch
from scipy import stats as scipy_stats

from ..core.feature_matrix import FeatureMatrix
from ..core.gpu_ops import compute_ic_series, compute_ic_stats
from ..core.train_test_split import TRAIN_CUTOFF_TS
from ..signals.base import Signal
from .metrics import BacktestResult


class BacktestEngine:
    """Evaluate a signal on held-out backtest data.

    Args:
        signal: Signal instance to evaluate
        fm_train: FeatureMatrix built from training data
        fm_backtest: FeatureMatrix built from backtest data
        horizons: list of forward-looking horizons (days) to evaluate
    """

    def __init__(
        self,
        signal: Signal,
        fm_train: FeatureMatrix,
        fm_backtest: FeatureMatrix,
        horizons: list[int] = None,
    ):
        self.signal = signal
        self.fm_train = fm_train
        self.fm_backtest = fm_backtest
        self.horizons = horizons or [1, 5, 20]
        self._threshold: float | None = None
        self._train_ic_ir: float = float('nan')

    def _assert_backtest(self, fm: FeatureMatrix):
        """Raise if fm contains any training-period dates."""
        if fm.dates is None or len(fm.dates) == 0:
            return
        if fm.dates.min() < TRAIN_CUTOFF_TS:
            raise ValueError(
                "BacktestEngine.run() received a FeatureMatrix containing training data. "
                "Pass fm_backtest (split='backtest') only."
            )

    def _forward_returns(self, fm: FeatureMatrix, horizon: int) -> torch.Tensor:
        """Compute horizon-day forward returns from close prices."""
        close = fm.build()['close']
        mask = fm.mask
        N, T = close.shape
        fwd = torch.full_like(close, float('nan'))
        if T > horizon:
            fwd_close = close[:, horizon:]
            base_close = close[:, :T - horizon]
            valid = mask[:, :T - horizon] & mask[:, horizon:]
            fwd[:, :T - horizon] = torch.where(
                valid,
                (fwd_close - base_close) / base_close.clamp(min=1e-8),
                torch.tensor(float('nan'), device=close.device)
            )
        return fwd

    def fit_threshold(self) -> float:
        """Compute signal activation threshold from training IC_IR.

        Threshold = z-score at which signal is worth acting on.
        Higher IC_IR → lower threshold (signal fires more often).
        """
        print(f"[BacktestEngine] Fitting threshold on training data for '{self.signal.name}'...")
        features = self.fm_train.build()
        mask = self.fm_train.mask
        sig = self.signal.compute(features, mask)

        fwd = self._forward_returns(self.fm_train, horizon=self.horizons[0])
        ic_series = compute_ic_series(sig, fwd, lag=1, mask=mask)
        stats = compute_ic_stats(ic_series)
        self._train_ic_ir = stats.get('ic_ir', float('nan'))

        ic_ir = abs(self._train_ic_ir) if not math.isnan(self._train_ic_ir) else 0.0
        # Threshold: signal percentile above which we act
        # Strong signal (IC_IR > 0.1): act on top 20%; weak: top 30%
        if ic_ir > 0.10:
            self._threshold = 0.80
        elif ic_ir > 0.05:
            self._threshold = 0.75
        else:
            self._threshold = 0.70

        print(f"[BacktestEngine] Train IC_IR={self._train_ic_ir:.4f}, threshold percentile={self._threshold:.2f}")
        return self._threshold

    def run(self, symbols_df: pd.DataFrame | None = None) -> BacktestResult:
        """Evaluate signal strictly on backtest data.

        Args:
            symbols_df: optional DataFrame with [symbol, industry] for by-industry breakdown
        """
        self._assert_backtest(self.fm_backtest)

        if self._threshold is None:
            self.fit_threshold()

        print(f"[BacktestEngine] Running backtest for '{self.signal.name}'...")
        features = self.fm_backtest.build()
        mask = self.fm_backtest.mask
        sig = self.signal.compute(features, mask)

        # Compute backtest IC_IR
        fwd_1 = self._forward_returns(self.fm_backtest, horizon=1)
        ic_series = compute_ic_series(sig, fwd_1, lag=1, mask=mask)
        bt_stats = compute_ic_stats(ic_series)
        backtest_ic_ir = bt_stats.get('ic_ir', float('nan'))

        # Trigger mask: signal >= threshold percentile
        trigger_mask = (sig >= self._threshold) & mask & ~torch.isnan(sig)

        hit_rate = {}
        mean_return = {}
        all_triggered_returns = []

        for h in self.horizons:
            fwd = self._forward_returns(self.fm_backtest, horizon=h)
            # Only evaluate positions triggered at t (exclude last h days — no fwd data)
            N, T = sig.shape
            eval_mask = trigger_mask.clone()
            if T > h:
                eval_mask[:, T - h:] = False

            triggered_fwd = fwd[eval_mask & ~torch.isnan(fwd)]
            n = triggered_fwd.numel()
            if n > 0:
                returns_cpu = triggered_fwd.cpu().numpy()
                hit_rate[h] = float((returns_cpu > 0).mean())
                mean_return[h] = float(returns_cpu.mean())
                if h == self.horizons[0]:
                    all_triggered_returns = returns_cpu
            else:
                hit_rate[h] = float('nan')
                mean_return[h] = float('nan')

        n_triggers = int(trigger_mask.sum().item())

        # Sharpe proxy (annualized) from 1-day triggered positions
        sharpe_proxy = float('nan')
        if len(all_triggered_returns) > 1:
            r = all_triggered_returns
            sharpe_proxy = float((r.mean() / (r.std() + 1e-10)) * (252 ** 0.5))

        # Max drawdown from cumulative PnL of triggered positions
        max_drawdown = float('nan')
        if len(all_triggered_returns) > 1:
            cum = np.cumsum(all_triggered_returns)
            running_max = np.maximum.accumulate(cum)
            drawdowns = cum - running_max
            max_drawdown = float(drawdowns.min())

        # p-value vs random baseline (binomial test on 1-day hit rate)
        p_value = 1.0
        h1 = self.horizons[0]
        if h1 in hit_rate and not math.isnan(hit_rate[h1]) and n_triggers > 0:
            n_correct = int(hit_rate[h1] * n_triggers)
            p_value = float(scipy_stats.binomtest(n_correct, n_triggers, p=0.5, alternative='greater').pvalue)

        # By-industry breakdown
        by_industry = pd.DataFrame()
        if symbols_df is not None and 'industry' in symbols_df.columns:
            syms = self.fm_backtest.symbols
            sym_to_industry = dict(zip(symbols_df['symbol'], symbols_df['industry']))
            industries = [sym_to_industry.get(s, 'Unknown') for s in syms]

            h1 = self.horizons[0]
            fwd = self._forward_returns(self.fm_backtest, horizon=h1)
            N, T = sig.shape
            eval_mask = trigger_mask.clone()
            if T > h1:
                eval_mask[:, T - h1:] = False

            rows = []
            for i, ind in enumerate(industries):
                sym_mask = eval_mask[i] & ~torch.isnan(fwd[i])
                sym_fwd = fwd[i][sym_mask].cpu().numpy()
                if len(sym_fwd) > 0:
                    rows.append(dict(
                        industry=ind,
                        symbol=syms[i],
                        n=len(sym_fwd),
                        hit_rate=float((sym_fwd > 0).mean()),
                        mean_return=float(sym_fwd.mean()),
                    ))
            if rows:
                by_industry = pd.DataFrame(rows).groupby('industry').agg(
                    n=('n', 'sum'),
                    hit_rate=('hit_rate', 'mean'),
                    mean_return=('mean_return', 'mean'),
                ).reset_index()

        overfit = (
            not math.isnan(self._train_ic_ir) and abs(self._train_ic_ir) > 0.1
            and h1 in hit_rate and not math.isnan(hit_rate[h1]) and hit_rate[h1] < 0.51
        )

        return BacktestResult(
            signal_name=self.signal.name,
            horizon_days=self.horizons,
            hit_rate=hit_rate,
            mean_return=mean_return,
            sharpe_proxy=sharpe_proxy,
            n_triggers=n_triggers,
            p_value=p_value,
            max_drawdown=max_drawdown,
            by_industry=by_industry,
            train_ic_ir=self._train_ic_ir,
            backtest_ic_ir=backtest_ic_ir,
            overfit_flag=overfit,
        )
