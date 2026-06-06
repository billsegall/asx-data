# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Kronos inference wrapper: batch 5-day forward return forecasts for ASX symbols."""

import math
import pandas as pd


def _import_kronos():
    """Import Kronos model classes, adding the cloned repo to sys.path if needed."""
    import sys, os
    try:
        from model import Kronos, KronosTokenizer, KronosPredictor
        return Kronos, KronosTokenizer, KronosPredictor
    except ImportError:
        # Try the standard clone location used by setup.sh
        here = os.path.dirname(__file__)
        kronos_src = os.path.abspath(os.path.join(here, 'Kronos'))
        if kronos_src not in sys.path:
            sys.path.insert(0, kronos_src)
        from model import Kronos, KronosTokenizer, KronosPredictor
        return Kronos, KronosTokenizer, KronosPredictor


def build_predictor(model_dir: str, tokenizer_dir: str, device: str = 'cuda'):
    """Load KronosPredictor from local weight directories."""
    Kronos, KronosTokenizer, KronosPredictor = _import_kronos()
    tokenizer = KronosTokenizer.from_pretrained(tokenizer_dir)
    model = Kronos.from_pretrained(model_dir)
    return KronosPredictor(model, tokenizer, device=device)


def forecast_5d_returns(
    predictor,
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    eval_date: str,
    lookback: int = 400,
    pred_len: int = 5,
    batch_size: int = 200,
) -> dict[str, float]:
    """Forecast 5-trading-day forward returns for all symbols at eval_date.

    Uses only data up to and including eval_date (no lookahead).
    Returns {symbol: forecasted_5d_return}.
    Symbols with insufficient history before eval_date are omitted.
    """
    eval_dt = pd.Timestamp(eval_date)

    # Build per-symbol context slices up to eval_date
    eligible = {}
    for sym, df in ohlcv_by_symbol.items():
        past = df[df.index <= eval_dt]
        if len(past) < lookback:
            continue
        ctx = past.iloc[-lookback:][['open', 'high', 'low', 'close', 'volume']].copy()
        eligible[sym] = ctx

    if not eligible:
        return {}

    # Build y_timestamps: pred_len business days after eval_date (as Series — Kronos needs Series not DatetimeIndex)
    y_ts = pd.Series(pd.bdate_range(start=eval_dt, periods=pred_len + 1)[1:])

    symbols = list(eligible.keys())
    forecasts = {}

    # Process in batches to manage VRAM
    for batch_start in range(0, len(symbols), batch_size):
        batch_syms = symbols[batch_start: batch_start + batch_size]
        df_list        = [eligible[s] for s in batch_syms]
        x_ts_list      = [pd.Series(eligible[s].index) for s in batch_syms]
        y_ts_list      = [y_ts] * len(batch_syms)

        try:
            preds = predictor.predict_batch(
                df_list=df_list,
                x_timestamp_list=x_ts_list,
                y_timestamp_list=y_ts_list,
                pred_len=pred_len,
                T=1.0,
                top_p=0.9,
                sample_count=1,
                verbose=False,
            )
        except Exception as e:
            print(f"  [kronos] batch {batch_start//batch_size} error: {e}")
            continue

        for sym, pred_df in zip(batch_syms, preds):
            if pred_df is None or pred_df.empty:
                continue
            actual_close = eligible[sym].iloc[-1]['close']
            if not actual_close or actual_close <= 0:
                continue
            predicted_close = pred_df['close'].iloc[-1]
            if math.isnan(predicted_close):
                continue
            forecasts[sym] = (predicted_close - actual_close) / actual_close

    return forecasts
