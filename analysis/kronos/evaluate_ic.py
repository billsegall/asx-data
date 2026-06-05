# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""IC evaluation for Kronos zero-shot forecasts on ASX daily data."""

import json
import math
import time
from scipy import stats

from .loader import load_all_ohlcv, get_evaluation_dates, get_actual_5d_returns
from .inference import build_predictor, forecast_5d_returns


def spearman_ic(predicted: dict[str, float], actual: dict[str, float]) -> float | None:
    """Spearman rank correlation between predicted and actual returns for common symbols."""
    common = [s for s in predicted if s in actual]
    if len(common) < 10:
        return None
    pred_vals   = [predicted[s] for s in common]
    actual_vals = [actual[s]    for s in common]
    rho, _ = stats.spearmanr(pred_vals, actual_vals)
    return float(rho) if not math.isnan(rho) else None


def evaluate_ic(
    db_path: str,
    model_dir: str,
    tokenizer_dir: str,
    output_path: str,
    start: str = '2025-03-01',
    end: str | None = None,
    step_days: int = 5,
    lookback: int = 400,
    pred_len: int = 5,
    device: str = 'cuda',
    resume: bool = True,
) -> dict:
    """Run IC sweep for Kronos zero-shot forecasts.

    At each evaluation date t:
      1. Forecast 5d returns for all symbols using Kronos (data up to t only)
      2. Compute actual 5d returns from stockdb.db (data after t — held out)
      3. Spearman IC(t) = corr(forecasted, actual) across all symbols

    Saves results incrementally to output_path (JSON) so the run can be resumed.
    """
    # Load previously computed results for resumption
    completed = {}
    if resume:
        try:
            with open(output_path) as f:
                prev = json.load(f)
            completed = {r['date']: r['ic'] for r in prev.get('ic_series', [])}
            print(f"[kronos_ic] Resuming: {len(completed)} dates already computed.")
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    print("[kronos_ic] Loading OHLCV data...")
    t0 = time.time()
    ohlcv = load_all_ohlcv(db_path)
    print(f"[kronos_ic] Loaded {len(ohlcv)} symbols in {time.time()-t0:.1f}s")

    print("[kronos_ic] Loading Kronos model...")
    predictor = build_predictor(model_dir, tokenizer_dir, device=device)

    eval_dates = get_evaluation_dates(db_path, start=start, end=end, step_days=step_days)
    # Drop the last pred_len dates — can't compute actual returns for them yet
    eval_dates = eval_dates[:-pred_len] if len(eval_dates) > pred_len else []
    print(f"[kronos_ic] {len(eval_dates)} evaluation dates ({start} → {end or 'today'}, every {step_days}d)")

    ic_series = list(completed.items())  # [(date, ic), ...]
    all_syms = list(ohlcv.keys())

    for i, eval_date in enumerate(eval_dates):
        if eval_date in completed:
            continue

        t1 = time.time()
        forecasts = forecast_5d_returns(
            predictor, ohlcv, eval_date, lookback=lookback, pred_len=pred_len
        )
        actual = get_actual_5d_returns(db_path, eval_date, all_syms)
        ic = spearman_ic(forecasts, actual)
        elapsed = time.time() - t1

        n_common = len([s for s in forecasts if s in actual])
        ic_str = f"{ic:+.4f}" if ic is not None else "  None"
        print(f"  [{i+1}/{len(eval_dates)}] {eval_date}  IC={ic_str}  n={n_common}  ({elapsed:.0f}s)")

        if ic is not None:
            ic_series.append((eval_date, ic))

        # Save incrementally
        _save(output_path, ic_series, start, end, step_days, lookback, pred_len)

    return _save(output_path, ic_series, start, end, step_days, lookback, pred_len)


def _save(output_path, ic_series, start, end, step_days, lookback, pred_len) -> dict:
    """Compute summary stats and write JSON. Returns the result dict."""
    ic_values = [ic for _, ic in ic_series if ic is not None]

    if len(ic_values) >= 2:
        mean_ic = sum(ic_values) / len(ic_values)
        std_ic  = float(stats.tstd(ic_values))
        # Annualise IR: IC measured at pred_len-day intervals, 252 trading days/year
        periods_per_year = 252 / pred_len
        ic_ir   = (mean_ic / std_ic * math.sqrt(periods_per_year)) if std_ic > 0 else 0.0
        t_stat, p_value = stats.ttest_1samp(ic_values, 0.0)
    else:
        mean_ic = ic_values[0] if ic_values else None
        std_ic  = None
        ic_ir   = None
        t_stat  = None
        p_value = None

    result = {
        'mean_ic':   round(mean_ic, 6)  if mean_ic  is not None else None,
        'std_ic':    round(std_ic, 6)   if std_ic   is not None else None,
        'ic_ir':     round(ic_ir, 4)    if ic_ir    is not None else None,
        't_stat':    round(t_stat, 4)   if t_stat   is not None else None,
        'p_value':   round(p_value, 6)  if p_value  is not None else None,
        'n_dates':   len(ic_values),
        'ic_series': [{'date': d, 'ic': round(ic, 6)} for d, ic in ic_series],
        'params': {
            'start': start, 'end': end, 'step_days': step_days,
            'lookback': lookback, 'pred_len': pred_len,
        },
    }

    import os
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    return result
