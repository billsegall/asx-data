# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Run the day57-70 / day71-91 EOFY sub-window tests and store results.

Usage:
    python -m analysis.cli.run_eofy_window_compare --db stockdb/stockdb.db \
        --eofy-db analysis/results/eofy_correlation.db --min-years 5
"""

import argparse

from analysis.eofy_correlation.pipeline import MIN_YEARS_FLOOR
from analysis.eofy_correlation.window_pipeline import (
    init_eofy_window_db,
    run_window_pipeline,
    write_window_to_db,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--eofy-db', default='analysis/results/eofy_correlation.db')
    parser.add_argument('--min-years', type=int, default=MIN_YEARS_FLOOR)
    parser.add_argument('--fdr-alpha', type=float, default=0.05)
    args = parser.parse_args()

    init_eofy_window_db(args.eofy_db)
    results = run_window_pipeline(args.db, min_years=args.min_years, fdr_alpha=args.fdr_alpha)
    write_window_to_db(results, args.eofy_db)

    for label, (df, meta) in results.items():
        print(f"Window {label} ({meta['label']}): n_tested={meta['n_tested']} "
              f"n_significant(fdr<{args.fdr_alpha})={meta['n_significant']} "
              f"{meta['elapsed_seconds']:.1f}s")


if __name__ == '__main__':
    main()
