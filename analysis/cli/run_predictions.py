#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Generate current signal predictions (called by cron after EOD fetch).

Usage:
    python -m analysis.cli.run_predictions --db stockdb/stockdb.db
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
from analysis.predictions.predictor import Predictor
from analysis.signals import ShortTrendSignal, ShortSqueezeSignal, VolumeAnomalySignal
from analysis.signals.kronos import KronosSignal


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--lookback-days', type=int, default=252,
                        help='Days of history to load for signal computation')
    parser.add_argument('--output-dir', default='analysis/results')
    parser.add_argument('--kronos-model-dir',
                        default='analysis/kronos/weights/kronos-mini-asx',
                        help='Fine-tuned Kronos predictor weights dir (skip Kronos if absent)')
    parser.add_argument('--kronos-tokenizer-dir',
                        default='analysis/kronos/weights/tokenizer',
                        help='KronosTokenizer weights dir')
    args = parser.parse_args()

    print(f"[run_predictions] GPU: {gpu_monitor()}")
    t0 = time.time()

    loader = DataLoader(args.db, split='all')
    print("[run_predictions] Loading recent EOD + shorts...")
    eod_df = loader.load_eod(min_history_days=0)
    shorts_df = loader.load_shorts()
    symbols_df = loader.load_symbols()
    print(f"[run_predictions] Loaded in {time.time()-t0:.1f}s")

    fm = FeatureMatrix(eod_df, shorts_df, split='all', cache_dir='analysis/cache')

    signals = [ShortTrendSignal(), ShortSqueezeSignal(), VolumeAnomalySignal()]
    os.makedirs(args.output_dir, exist_ok=True)

    for sig in signals:
        print(f"[run_predictions] Scoring '{sig.name}'...")
        predictor = Predictor(sig, fm, symbols_df=symbols_df)
        path = predictor.save(args.output_dir)
        print(f"[run_predictions] → {path}")
        torch.cuda.empty_cache()

    # Kronos signal — standalone (loads own OHLCV, doesn't use FeatureMatrix)
    if os.path.isdir(args.kronos_model_dir) and os.path.isdir(args.kronos_tokenizer_dir):
        print(f"[run_predictions] Scoring 'kronos'...")
        kronos = KronosSignal(
            db_path=args.db,
            model_dir=args.kronos_model_dir,
            tokenizer_dir=args.kronos_tokenizer_dir,
        )
        path = kronos.save(args.output_dir)
        print(f"[run_predictions] → {path}")
        torch.cuda.empty_cache()
    else:
        print(f"[run_predictions] Skipping Kronos (weights not found at {args.kronos_model_dir})")

    print(f"\n[run_predictions] Done in {time.time()-t0:.1f}s. GPU: {gpu_monitor()}")


if __name__ == '__main__':
    main()
