#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Kronos zero-shot IC evaluation on ASX data.

Usage:
    python -m analysis.cli.run_kronos_ic \\
        --db stockdb/stockdb.db \\
        --model-dir analysis/kronos/weights/kronos-mini \\
        --tokenizer-dir analysis/kronos/weights/tokenizer

Downloads weights on first run if --model-dir / --tokenizer-dir are HuggingFace IDs.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from analysis.kronos.evaluate_ic import evaluate_ic

KRONOS_MINI_MODEL     = 'NeoQuasar/Kronos-mini'
KRONOS_TOKENIZER_BASE = 'NeoQuasar/Kronos-Tokenizer-base'


def main():
    parser = argparse.ArgumentParser(description='Kronos IC evaluation')
    parser.add_argument('--db',            default='stockdb/stockdb.db')
    parser.add_argument('--model-dir',     default=KRONOS_MINI_MODEL,
                        help='Local path or HuggingFace model ID (default: Kronos-mini)')
    parser.add_argument('--tokenizer-dir', default=KRONOS_TOKENIZER_BASE,
                        help='Local path or HuggingFace tokenizer ID')
    parser.add_argument('--output',        default='analysis/results/kronos_ic.json')
    parser.add_argument('--start',         default='2025-03-01',
                        help='Start of evaluation period (backtest start)')
    parser.add_argument('--end',           default=None,
                        help='End date (default: today)')
    parser.add_argument('--step-days',     type=int, default=5,
                        help='Evaluate every N trading days (default: 5)')
    parser.add_argument('--lookback',      type=int, default=400,
                        help='Context window in trading days (≤512, default: 400)')
    parser.add_argument('--pred-len',      type=int, default=5,
                        help='Forecast horizon in trading days (default: 5)')
    parser.add_argument('--device',        default='cuda',
                        help='PyTorch device (default: cuda)')
    parser.add_argument('--no-resume',     action='store_true',
                        help='Rerun from scratch ignoring any saved progress')
    args = parser.parse_args()

    result = evaluate_ic(
        db_path       = args.db,
        model_dir     = args.model_dir,
        tokenizer_dir = args.tokenizer_dir,
        output_path   = args.output,
        start         = args.start,
        end           = args.end,
        step_days     = args.step_days,
        lookback      = args.lookback,
        pred_len      = args.pred_len,
        device        = args.device,
        resume        = not args.no_resume,
    )

    print()
    print("=" * 50)
    print("Kronos IC Evaluation Summary")
    print("=" * 50)
    print(f"  Dates evaluated : {result['n_dates']}")
    print(f"  Mean IC         : {result['mean_ic']}")
    print(f"  Std IC          : {result['std_ic']}")
    print(f"  IC IR           : {result['ic_ir']}")
    print(f"  t-stat          : {result['t_stat']}")
    print(f"  p-value         : {result['p_value']}")
    print()

    ic_ir = result.get('ic_ir')
    if ic_ir is None:
        print("Insufficient data to assess.")
    elif ic_ir > 0.10:
        print("Strong signal — integrate as KronosSignal.")
    elif ic_ir > 0.02:
        print("Modest signal — worth integrating; consider fine-tuning on ASX data.")
    elif ic_ir and ic_ir > 0:
        print("Weak positive signal — fine-tune before integrating.")
    else:
        print("No signal detected zero-shot.")

    print(f"\nFull results: {args.output}")


if __name__ == '__main__':
    main()
