# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Kronos portfolio backtest CLI.

Simulates a long-only top-N portfolio at each evaluation date using Kronos 5-day forecasts.
Measures actual outcomes and computes hit rate, mean return, Sharpe, max drawdown, IC IR.

Usage:
    python -m analysis.cli.run_kronos_backtest \\
        --db stockdb/stockdb.db \\
        --model-dir /abs/path/to/kronos-mini-asx \\
        --tokenizer-dir /abs/path/to/tokenizer

Output: analysis/results/backtest_kronos.json
"""

import argparse
import json
import math
import os
import time

from scipy import stats as scipy_stats

from analysis.kronos.loader import load_all_ohlcv, get_evaluation_dates, get_actual_5d_returns
from analysis.kronos.inference import build_predictor, forecast_5d_returns
from analysis.kronos.evaluate_ic import spearman_ic


TOP_N_VALUES = [10, 20, 50]

DEFAULT_MODEL_DIR     = 'analysis/kronos/weights/kronos-mini-asx'
DEFAULT_TOKENIZER_DIR = 'analysis/kronos/weights/tokenizer'
DEFAULT_OUTPUT_DIR    = 'analysis/results'


def _max_drawdown(equity: list[float]) -> float:
    """Max peak-to-trough drawdown from a cumulative equity series (fractional)."""
    if not equity:
        return 0.0
    peak = equity[0]
    dd = 0.0
    for v in equity:
        peak = max(peak, v)
        dd = min(dd, (v - peak) / (peak + 1e-10))
    return dd


def _sharpe(per_period_returns: list[float], periods_per_year: float = 252 / 5) -> float:
    if len(per_period_returns) < 2:
        return 0.0
    m = sum(per_period_returns) / len(per_period_returns)
    s = float(scipy_stats.tstd(per_period_returns))
    if s == 0:
        return 0.0
    return m / s * math.sqrt(periods_per_year)


def run_backtest(
    db_path: str,
    model_dir: str,
    tokenizer_dir: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    start: str = '2025-03-01',
    end: str | None = None,
    step_days: int = 5,
    lookback: int = 400,
    pred_len: int = 5,
    device: str = 'cuda',
    resume: bool = True,
) -> dict:

    output_path = os.path.join(output_dir, 'backtest_kronos.json')

    # Resume support: reload prior per-date results
    completed = {}      # date → {'ic': float, 'forecasts': {sym: score}, 'actual': {sym: ret}}
    if resume:
        try:
            with open(output_path) as f:
                prev = json.load(f)
            for row in prev.get('dates', []):
                completed[row['date']] = row
            print(f"[kronos_bt] Resuming: {len(completed)} dates already computed.")
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    print("[kronos_bt] Loading OHLCV data...")
    t0 = time.time()
    ohlcv = load_all_ohlcv(db_path)
    print(f"[kronos_bt] Loaded {len(ohlcv)} symbols in {time.time()-t0:.1f}s")

    print("[kronos_bt] Loading Kronos model...")
    predictor = build_predictor(model_dir, tokenizer_dir, device=device)

    eval_dates = get_evaluation_dates(db_path, start=start, end=end, step_days=step_days)
    eval_dates = eval_dates[:-pred_len] if len(eval_dates) > pred_len else []
    print(f"[kronos_bt] {len(eval_dates)} eval dates ({start} → {end or 'today'}, every {step_days}d)")

    all_syms = list(ohlcv.keys())
    date_rows = list(completed.values())

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

        row = {
            'date': eval_date,
            'ic': round(ic, 6) if ic is not None else None,
            'n_forecast': len(forecasts),
            'n_actual': len(actual),
            'forecasts': {s: round(v, 6) for s, v in forecasts.items()},
            'actual':    {s: round(v, 6) for s, v in actual.items()},
        }
        date_rows.append(row)
        completed[eval_date] = row

        ic_str = f"{ic:+.4f}" if ic is not None else "  None"
        print(f"  [{i+1}/{len(eval_dates)}] {eval_date}  IC={ic_str}  n={len(forecasts)}  ({elapsed:.0f}s)")

        # Save incrementally
        _save(output_path, date_rows, start, end, step_days, model_dir)

    return _save(output_path, date_rows, start, end, step_days, model_dir)


def _save(output_path, date_rows, start, end, step_days, model_dir) -> dict:
    ic_values = [r['ic'] for r in date_rows if r.get('ic') is not None]

    # IC summary
    if len(ic_values) >= 2:
        mean_ic = sum(ic_values) / len(ic_values)
        std_ic  = float(scipy_stats.tstd(ic_values))
        periods_per_year = 252 / step_days
        ic_ir   = (mean_ic / std_ic * math.sqrt(periods_per_year)) if std_ic > 0 else 0.0
        t_stat, p_val = scipy_stats.ttest_1samp(ic_values, 0.0)
    elif ic_values:
        mean_ic = ic_values[0]; std_ic = None; ic_ir = None; t_stat = None; p_val = None
    else:
        mean_ic = std_ic = ic_ir = t_stat = p_val = None

    ic_summary = {
        'mean_ic': round(mean_ic, 6) if mean_ic is not None else None,
        'std_ic':  round(std_ic, 6)  if std_ic  is not None else None,
        'ic_ir':   round(ic_ir, 4)   if ic_ir   is not None else None,
        't_stat':  round(t_stat, 4)  if t_stat  is not None else None,
        'p_value': round(p_val, 6)   if p_val   is not None else None,
        'n_dates': len(ic_values),
        'ic_series': [{'date': r['date'], 'ic': r['ic']} for r in date_rows if r.get('ic') is not None],
    }

    # Portfolio simulations
    portfolios = {}
    for top_n in TOP_N_VALUES:
        per_date = []
        all_individual = []

        for r in date_rows:
            f = r.get('forecasts', {})
            a = r.get('actual', {})
            if not f or not a:
                continue

            # Top-N by forecast score, must also have actual return
            ranked = sorted(f.keys(), key=lambda s: f[s], reverse=True)
            picks = [s for s in ranked[:top_n] if s in a]
            if not picks:
                continue

            rets = [a[s] for s in picks]
            per_date.append({'date': r['date'], 'mean_return': sum(rets) / len(rets), 'n_picks': len(picks)})
            all_individual.extend(rets)

        if not per_date:
            portfolios[f'top{top_n}'] = {}
            continue

        period_returns = [d['mean_return'] for d in per_date]
        hit_all = sum(1 for v in all_individual if v > 0) / len(all_individual) if all_individual else 0.0

        # Cumulative equity curve (compounding per-period returns)
        equity = [1.0]
        for pr in period_returns:
            equity.append(equity[-1] * (1 + pr))

        # t-test: per-period mean returns vs 0
        if len(period_returns) >= 2:
            _, p = scipy_stats.ttest_1samp(period_returns, 0.0)
        else:
            p = 1.0

        sharpe = _sharpe(period_returns, periods_per_year=252 / step_days)
        mdd    = _max_drawdown(equity)

        portfolios[f'top{top_n}'] = {
            'hit_rate':     round(hit_all, 4),
            'mean_return':  round(sum(period_returns) / len(period_returns), 6),
            'sharpe_proxy': round(sharpe, 4),
            'max_drawdown': round(mdd, 4),
            'n_trades':     len(all_individual),
            'n_dates':      len(per_date),
            'p_value':      round(float(p), 6),
            'final_equity': round(equity[-1], 4),
            'equity_curve': [{'date': per_date[j]['date'], 'equity': round(equity[j + 1], 6)}
                             for j in range(len(per_date))],
            'per_date':     per_date,
        }

    result = {
        'generated_at': int(time.time()),
        'params': {
            'start': start, 'end': end, 'step_days': step_days,
            'pred_len': 5, 'top_n_values': TOP_N_VALUES,
        },
        'ic': ic_summary,
        'portfolios': portfolios,
        'dates': date_rows,
    }

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"[kronos_bt] Saved: {output_path}")
    return result


def main():
    p = argparse.ArgumentParser(description="Kronos portfolio backtest")
    p.add_argument('--db', default='stockdb/stockdb.db')
    p.add_argument('--model-dir', default=DEFAULT_MODEL_DIR)
    p.add_argument('--tokenizer-dir', default=DEFAULT_TOKENIZER_DIR)
    p.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR)
    p.add_argument('--start', default='2025-03-01')
    p.add_argument('--end', default=None)
    p.add_argument('--step-days', type=int, default=5)
    p.add_argument('--lookback', type=int, default=400)
    p.add_argument('--device', default='cuda')
    p.add_argument('--no-resume', action='store_true')
    args = p.parse_args()

    result = run_backtest(
        db_path=args.db,
        model_dir=args.model_dir,
        tokenizer_dir=args.tokenizer_dir,
        output_dir=args.output_dir,
        start=args.start,
        end=args.end,
        step_days=args.step_days,
        lookback=args.lookback,
        device=args.device,
        resume=not args.no_resume,
    )

    ic = result.get('ic', {})
    print(f"\n=== IC Summary ===")
    print(f"  n_dates:  {ic.get('n_dates')}")
    print(f"  mean_IC:  {ic.get('mean_ic')}")
    print(f"  IC_IR:    {ic.get('ic_ir')}")
    print(f"  p-value:  {ic.get('p_value')}")

    print(f"\n=== Portfolio Simulations ===")
    for k, v in result.get('portfolios', {}).items():
        if not v:
            continue
        print(f"  {k}: hit={v.get('hit_rate'):.3f} mean_ret={v.get('mean_return'):.4f} "
              f"sharpe={v.get('sharpe_proxy'):.2f} mdd={v.get('max_drawdown'):.3f} "
              f"final_equity={v.get('final_equity'):.3f} p={v.get('p_value'):.4f}")


if __name__ == '__main__':
    main()
