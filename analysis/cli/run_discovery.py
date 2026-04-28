#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Auto-discovery IC sweep on training data.

Usage:
    python -m analysis.cli.run_discovery --db stockdb/stockdb.db [--max-lag 20]
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
from analysis.discovery.ic_sweep import ICSweep
from analysis.discovery.pca_factors import PCAFactors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--max-lag', type=int, default=20)
    parser.add_argument('--fdr-alpha', type=float, default=0.05)
    parser.add_argument('--pca-components', type=int, default=10)
    parser.add_argument('--output-dir', default='analysis/results')
    args = parser.parse_args()

    print(f"[run_discovery] GPU: {gpu_monitor()}")

    loader = DataLoader(args.db, split='train')
    print("[run_discovery] Loading training data...")
    t0 = time.time()
    eod_df = loader.load_eod(min_history_days=252)
    shorts_df = loader.load_shorts()
    print(f"[run_discovery] Loaded in {time.time()-t0:.1f}s")

    fm = FeatureMatrix(eod_df, shorts_df, split='train', cache_dir='analysis/cache')
    features = fm.build()
    mask = fm.mask
    print(f"[run_discovery] Matrix: {len(fm.symbols)} × {len(fm.dates)}")
    print(f"[run_discovery] GPU: {gpu_monitor()}")

    # Forward returns (1d) as target
    close = features['close']
    N, T = close.shape
    fwd = torch.full_like(close, float('nan'))
    valid = mask[:, :-1] & mask[:, 1:]
    fwd[:, :-1] = torch.where(
        valid,
        (close[:, 1:] - close[:, :-1]) / close[:, :-1].clamp(min=1e-8),
        torch.tensor(float('nan'), device=close.device)
    )

    # Optional: add PCA factors to feature set
    pca = PCAFactors(features['returns'], mask, n_components=args.pca_components)
    pca_tensors = pca.factor_tensors()
    features_extended = {**features, **pca_tensors}

    sweep = ICSweep(features_extended, fwd, mask, max_lag=args.max_lag, fdr_alpha=args.fdr_alpha)
    results_df = sweep.run()

    os.makedirs(args.output_dir, exist_ok=True)
    csv_path = os.path.join(args.output_dir, 'ic_sweep_results.csv')
    results_df.to_csv(csv_path, index=False)
    print(f"[run_discovery] Saved IC sweep results → {csv_path}")

    top = sweep.top_signals(n=20)
    print(f"\n[run_discovery] Top {len(top)} FDR-significant (feature, lag) pairs:")
    sig_df = results_df[results_df['fdr_significant']].head(20)
    print(sig_df[['feature', 'lag', 'mean_ic', 'ic_ir', 'p_value', 'fdr_corrected_p']].to_string(index=False))

    torch.cuda.empty_cache()
    print(f"\n[run_discovery] Done. GPU: {gpu_monitor()}")


if __name__ == '__main__':
    main()
