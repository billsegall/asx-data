"""Backtest report serialization."""

import json
import os
import time
from .metrics import BacktestResult


def save_report(result: BacktestResult, output_dir: str = 'analysis/results') -> str:
    """Save BacktestResult as JSON. Returns the file path."""
    os.makedirs(output_dir, exist_ok=True)
    data = result.to_dict()
    data['generated_at'] = int(time.time())

    # Industry breakdown: convert DataFrame to records
    if not result.by_industry.empty:
        data['by_industry'] = result.by_industry.to_dict(orient='records')
    else:
        data['by_industry'] = []

    fname = os.path.join(output_dir, f"backtest_{result.signal_name}.json")
    with open(fname, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"[Report] Saved: {fname}")
    return fname


def to_report(result: BacktestResult) -> str:
    """Human-readable text report."""
    lines = [
        f"=== Backtest Report: {result.signal_name} ===",
        f"Triggers: {result.n_triggers}",
        f"Overfit flag: {result.overfit_flag}",
        f"Train IC_IR: {result.train_ic_ir:.4f}",
        f"Backtest IC_IR: {result.backtest_ic_ir:.4f}",
        f"P-value vs random: {result.p_value:.4f}",
        f"Sharpe proxy: {result.sharpe_proxy:.3f}",
        f"Max drawdown: {result.max_drawdown:.4f}",
        "",
        "By horizon:",
    ]
    for h in result.horizon_days:
        hr = result.hit_rate.get(h, float('nan'))
        mr = result.mean_return.get(h, float('nan'))
        lines.append(f"  {h:2d}d — hit_rate={hr:.3f}  mean_return={mr:.5f}")

    if not result.by_industry.empty:
        lines.append("\nTop industries by mean return:")
        top = result.by_industry.nlargest(5, 'mean_return')
        for _, row in top.iterrows():
            lines.append(f"  {row['industry']}: n={row['n']} hr={row['hit_rate']:.3f} ret={row['mean_return']:.5f}")

    return '\n'.join(lines)
