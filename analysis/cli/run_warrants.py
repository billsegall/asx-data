#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Generate current warrant recommendations.

Usage:
    python -m analysis.cli.run_warrants --db stockdb/stockdb.db
    python -m analysis.cli.run_warrants --db stockdb/stockdb.db --ic-sweep
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from analysis.warrants.predictor import generate_predictions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--output-dir', default='analysis/results')
    parser.add_argument('--ic-sweep', action='store_true',
                        help='Run IC sweep on training data (slow, ~1 min)')
    args = parser.parse_args()

    t0 = time.time()

    output_path = os.path.join(args.output_dir, 'predictions_warrants.json')
    generate_predictions(args.db, output_path)

    if args.ic_sweep:
        print('\n[warrants] Running IC sweep on training data...')
        from analysis.warrants.data import load_warrant_pairs
        from analysis.warrants.features import compute_features
        from analysis.warrants.backtest import run_ic_sweep

        pairs = load_warrant_pairs(args.db, active_only=False)
        pairs_features = []
        for p in pairs:
            feat = compute_features(p)
            if feat is not None:
                pairs_features.append(feat)

        print(f'[warrants] {len(pairs_features)} pairs with features for IC sweep')
        ic = run_ic_sweep(pairs_features)

        ic_path = os.path.join(args.output_dir, 'warrant_ic_sweep.json')
        with open(ic_path, 'w') as f:
            json.dump({'generated_at': int(time.time()), 'ic': ic}, f, indent=2)
        print(f'[warrants] IC sweep → {ic_path}')

        # Print summary
        for sig, horizons in ic.items():
            for h, stats in horizons.items():
                if stats['n'] > 0:
                    print(f'  {sig:25s} {h}: IC={stats["ic"]:+.3f}  IC-IR={stats["ic_ir"]:+.2f}  n={stats["n"]}')

    print(f'\n[warrants] Done in {time.time() - t0:.1f}s')


if __name__ == '__main__':
    main()
