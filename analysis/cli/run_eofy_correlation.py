# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""CLI: EOFY tax-loss/gain per-stock correlation search.

For every current ASX symbol, correlates its own Q1-3 (Jul-Mar) return
against its own Q4 (Apr-Jun) return across every financial year in its
history, and stores results in eofy_correlation.db.

Usage (from repo root):
    python -m analysis.cli.run_eofy_correlation \\
        --db stockdb/stockdb.db \\
        --output-dir analysis/results \\
        --min-years 5

Results land in analysis/results/eofy_correlation.db (rsynced to server by
sync.sh). The web frontend queries via /api/analysis/eofy-correlations.
"""

import argparse
import logging
import os
import sys


def main():
    parser = argparse.ArgumentParser(description='EOFY tax-loss/gain per-stock correlation search')
    parser.add_argument('--db', required=True, help='Path to stockdb.db')
    parser.add_argument('--output-dir', required=True, help='Directory for eofy_correlation.db/.csv/.json')
    parser.add_argument('--min-years', type=int, default=5, help='Minimum included FYs per symbol')
    parser.add_argument('--fdr-alpha', type=float, default=0.05, help='FDR alpha')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout,
    )

    from analysis.eofy_correlation.pipeline import init_eofy_db, run_pipeline, write_to_db

    os.makedirs(args.output_dir, exist_ok=True)
    db_out = os.path.join(args.output_dir, 'eofy_correlation.db')

    init_eofy_db(db_out)
    df, meta = run_pipeline(args.db, min_years=args.min_years, fdr_alpha=args.fdr_alpha)

    if len(df) == 0:
        logging.getLogger(__name__).warning('No symbols passed min_years=%d — nothing to write', args.min_years)
        return

    write_to_db(df, meta, db_out)

    csv_path = os.path.join(args.output_dir, 'eofy_correlation.csv')
    df.drop(columns=['fy_detail_json']).to_csv(csv_path, index=False)

    meta_path = os.path.join(args.output_dir, 'eofy_correlation_meta.json')
    import json
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)

    print(f'Wrote {len(df)} symbols to {db_out}')
    print(f'  n_significant (fdr<{args.fdr_alpha}): {meta["n_significant"]}')
    print(f'  elapsed: {meta["elapsed_seconds"]:.1f}s')


if __name__ == '__main__':
    main()
