# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""BacktestResult: structured container for backtest output metrics."""

from dataclasses import dataclass, field
import pandas as pd
import numpy as np


@dataclass
class BacktestResult:
    signal_name: str
    horizon_days: list[int]
    hit_rate: dict[int, float]         # horizon → fraction correct direction
    mean_return: dict[int, float]      # horizon → mean return when triggered
    sharpe_proxy: float                # annualized Sharpe of triggered positions
    n_triggers: int                    # total (sym, date) positions taken
    p_value: float                     # vs random baseline (same frequency)
    max_drawdown: float                # max peak-to-trough of daily PnL
    by_industry: pd.DataFrame = field(default_factory=pd.DataFrame)
    train_ic_ir: float = float('nan')
    backtest_ic_ir: float = float('nan')
    overfit_flag: bool = False         # train IC_IR > 0.1 but backtest hit_rate < 51%

    def to_dict(self) -> dict:
        return dict(
            signal_name=self.signal_name,
            horizon_days=self.horizon_days,
            hit_rate={str(k): round(v, 4) for k, v in self.hit_rate.items()},
            mean_return={str(k): round(v, 6) for k, v in self.mean_return.items()},
            sharpe_proxy=round(self.sharpe_proxy, 4),
            n_triggers=self.n_triggers,
            p_value=round(self.p_value, 6),
            max_drawdown=round(self.max_drawdown, 4),
            train_ic_ir=round(self.train_ic_ir, 4) if not np.isnan(self.train_ic_ir) else None,
            backtest_ic_ir=round(self.backtest_ic_ir, 4) if not np.isnan(self.backtest_ic_ir) else None,
            overfit_flag=self.overfit_flag,
        )
