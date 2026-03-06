#!/usr/bin/env python3
"""Compute named signals on training data and save JSON results.

Usage:
    python -m analysis.cli.run_signals --db stockdb/stockdb.db [--signal short_trend]
"""

import argparse
import json
import os
import sys
import time

# Allow running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from analysis.core.data_loader import DataLoader
from analysis.core.feature_matrix import FeatureMatrix
from analysis.core.gpu_ops import gpu_monitor
from analysis.signals import ALL_SIGNALS, ShortTrendSignal, ShortSqueezeSignal, VolumeAnomalySignal


SIGNAL_MAP = {
    'short_trend': ShortTrendSignal(),
    'short_squeeze': ShortSqueezeSignal(),
    'volume_anomaly': VolumeAnomalySignal(),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--split', default='train', choices=['train', 'backtest', 'all'])
    parser.add_argument('--signal', default=None, help='Specific signal name; default=all')
    parser.add_argument('--min-history', type=int, default=252)
    parser.add_argument('--output-dir', default='analysis/results')
    args = parser.parse_args()

    print(f"[run_signals] GPU: {gpu_monitor()}")

    loader = DataLoader(args.db, split=args.split)
    print(f"[run_signals] Loading EOD data ({args.split})...")
    t0 = time.time()
    eod_df = loader.load_eod(min_history_days=args.min_history)
    print(f"[run_signals] Loaded {len(eod_df)} EOD rows in {time.time()-t0:.1f}s")

    print(f"[run_signals] Loading shorts...")
    shorts_df = loader.load_shorts()
    print(f"[run_signals] Loaded {len(shorts_df)} shorts rows")

    fm = FeatureMatrix(eod_df, shorts_df, split=args.split, cache_dir='analysis/cache')
    features = fm.build()
    mask = fm.mask
    print(f"[run_signals] Feature matrix: {len(fm.symbols)} symbols × {len(fm.dates)} dates")

    signals = [SIGNAL_MAP[args.signal]] if args.signal else list(SIGNAL_MAP.values())
    os.makedirs(args.output_dir, exist_ok=True)

    for sig in signals:
        print(f"\n[run_signals] Computing '{sig.name}'...")
        t1 = time.time()
        result = sig.compute(features, mask)
        elapsed = time.time() - t1
        print(f"[run_signals] '{sig.name}' done in {elapsed:.2f}s. Shape: {result.shape}")

        # Save last-date scores
        last_col = result[:, -1].cpu().numpy()
        import numpy as np
        rows = [
            {'symbol': sym, 'score': round(float(s), 4)}
            for sym, s in zip(fm.symbols, last_col)
            if not np.isnan(s)
        ]
        rows.sort(key=lambda r: r['score'], reverse=True)
        out = dict(signal=sig.name, n=len(rows), scores=rows[:200])
        fname = os.path.join(args.output_dir, f"signal_{sig.name}.json")
        with open(fname, 'w') as f:
            json.dump(out, f, indent=2)
        print(f"[run_signals] Saved top {len(rows[:200])} scores → {fname}")

    import torch
    torch.cuda.empty_cache()
    print(f"\n[run_signals] Done. GPU: {gpu_monitor()}")


if __name__ == '__main__':
    main()
