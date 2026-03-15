"""CLI entry point: lead-lag correlation discovery pipeline.

Usage (from repo root):
    python -m analysis.cli.run_correlations \\
        --db stockdb/stockdb.db \\
        --output-dir analysis/results \\
        --max-lag 20 --min-r 0.15 --market-adjust
"""

import argparse
import logging
import sys


def main():
    parser = argparse.ArgumentParser(description='Lead-lag correlation discovery')
    parser.add_argument('--db',           required=True,  help='Path to stockdb.db')
    parser.add_argument('--output-dir',   required=True,  help='Directory for results')
    parser.add_argument('--max-lag',      type=int,   default=20,   help='Maximum lag in days')
    parser.add_argument('--min-r',        type=float, default=0.15, help='Minimum |r| threshold')
    parser.add_argument('--fdr-alpha',    type=float, default=0.05, help='FDR significance threshold')
    parser.add_argument('--market-adjust', action='store_true', default=False,
                        help='Subtract XAO returns (market-adjusted)')
    parser.add_argument('--no-market-adjust', dest='market_adjust', action='store_false',
                        help='Skip market adjustment')
    parser.add_argument('--device',       default=None,
                        help='PyTorch device (e.g. cuda, cpu). Auto-detected if omitted.')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout,
    )

    from analysis.correlations.pipeline import run_pipeline
    df, meta = run_pipeline(
        db_path=args.db,
        output_dir=args.output_dir,
        max_lag=args.max_lag,
        min_r=args.min_r,
        fdr_alpha=args.fdr_alpha,
        market_adjust=args.market_adjust,
        device=args.device,
    )

    print(f'\nDone. {len(df)} significant (leader, follower, lag) triplets.')
    print(f'  Stable (all 3 sub-periods): {meta["n_stable"]}')
    print(f'  Elapsed: {meta["elapsed_seconds"]}s')
    print(f'  Results: {args.output_dir}/correlations.csv')


if __name__ == '__main__':
    main()
