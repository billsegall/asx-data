#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Run backtest on held-out period (2025-03-01 → now).

Usage:
    python -m analysis.cli.run_backtest --db stockdb/stockdb.db [--signal short_trend]
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import torch
from analysis.core.data_loader import DataLoader
from analysis.core.feature_matrix import FeatureMatrix
from analysis.core.gpu_ops import gpu_monitor
from analysis.backtest.engine import BacktestEngine
from analysis.backtest.report import save_report, to_report
from analysis.signals import ShortTrendSignal, ShortSqueezeSignal, VolumeAnomalySignal

SIGNAL_MAP = {
    'short_trend': ShortTrendSignal,
    'short_squeeze': ShortSqueezeSignal,
    'volume_anomaly': VolumeAnomalySignal,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--signal', default=None, help='Signal to backtest; default=all')
    parser.add_argument('--horizons', default='1,5,20', help='Comma-separated horizon days')
    parser.add_argument('--output-dir', default='analysis/results')
    args = parser.parse_args()

    horizons = [int(h) for h in args.horizons.split(',')]
    signals = [SIGNAL_MAP[args.signal]()] if args.signal else [cls() for cls in SIGNAL_MAP.values()]

    print(f"[run_backtest] GPU: {gpu_monitor()}")

    # Load training data
    print("[run_backtest] Loading training EOD + shorts...")
    t0 = time.time()
    train_loader = DataLoader(args.db, split='train')
    train_eod = train_loader.load_eod(min_history_days=252)
    train_shorts = train_loader.load_shorts()
    print(f"[run_backtest] Train data loaded in {time.time()-t0:.1f}s")

    fm_train = FeatureMatrix(train_eod, train_shorts, split='train', cache_dir='analysis/cache')

    # Load backtest data
    print("[run_backtest] Loading backtest EOD + shorts...")
    t1 = time.time()
    bt_loader = DataLoader(args.db, split='backtest')
    bt_eod = bt_loader.load_eod(min_history_days=0)
    bt_shorts = bt_loader.load_shorts()
    print(f"[run_backtest] Backtest data loaded in {time.time()-t1:.1f}s")

    fm_backtest = FeatureMatrix(bt_eod, bt_shorts, split='backtest', cache_dir='analysis/cache')

    # Load symbols for industry breakdown
    symbols_df = train_loader.load_symbols()

    os.makedirs(args.output_dir, exist_ok=True)

    for signal in signals:
        print(f"\n{'='*60}")
        print(f"[run_backtest] Signal: {signal.name}")
        engine = BacktestEngine(signal, fm_train, fm_backtest, horizons=horizons)
        result = engine.run(symbols_df=symbols_df)

        print(to_report(result))
        path = save_report(result, args.output_dir)
        print(f"[run_backtest] Saved → {path}")

        torch.cuda.empty_cache()

    print(f"\n[run_backtest] Done. GPU: {gpu_monitor()}")


if __name__ == '__main__':
    main()
