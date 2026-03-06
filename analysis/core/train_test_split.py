"""Train/test split boundary for backtesting isolation."""

import datetime

TRAIN_CUTOFF_STR = '20250301'

_dt = datetime.datetime(2025, 3, 1, 0, 0, 0)
TRAIN_CUTOFF_TS = int(_dt.timestamp())


def is_train(ts: float) -> bool:
    return ts < TRAIN_CUTOFF_TS


def is_backtest(ts: float) -> bool:
    return ts >= TRAIN_CUTOFF_TS
