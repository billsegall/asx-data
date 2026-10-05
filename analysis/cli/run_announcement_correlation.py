# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""CLI: announcement-type -> forward-return correlation.

For every ASX announcement, resolves a type (structured extraction or
headline-regex fallback) and correlates it against 1/5/20/60-trading-day
forward excess returns, storing results in announcement_correlation.db.

Usage (from repo root):
    python -m analysis.cli.run_announcement_correlation \\
        --db stockdb/stockdb.db \\
        --ann-db ../asx-announcements/announcements.db \\
        --output-dir analysis/results

Results land in analysis/results/announcement_correlation.db (rsynced to
server by sync.sh). The web frontend queries via
/api/analysis/announcement-correlations.
"""

import argparse
import logging
import os
import sys


def main():
    parser = argparse.ArgumentParser(description='Announcement-type -> forward-return correlation')
    parser.add_argument('--db', required=True, help='Path to stockdb.db')
    parser.add_argument('--ann-db', required=True, help='Path to announcements.db')
    parser.add_argument('--output-dir', required=True, help='Directory for announcement_correlation.db')
    parser.add_argument('--cache-dir', default='analysis/cache', help='FeatureMatrix parquet cache dir')
    parser.add_argument('--min-n-display', type=int, default=20, help='Cells below this n are flagged display_cutoff')
    parser.add_argument('--fdr-alpha', type=float, default=0.05, help='FDR alpha')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout,
    )
    log = logging.getLogger(__name__)

    if not os.path.exists(args.ann_db):
        # Not fatal -- sync.sh runs this after several other, independent
        # analysis steps have already completed; a missing/not-yet-synced
        # announcements.db must not make the whole run look like a failure.
        log.warning('announcements.db not found at %s -- skipping this run', args.ann_db)
        return

    from analysis.announcement_correlation.pipeline import init_db, run_pipeline, write_to_db

    os.makedirs(args.output_dir, exist_ok=True)
    db_out = os.path.join(args.output_dir, 'announcement_correlation.db')

    init_db(db_out)
    agg_df, detail_df, meta = run_pipeline(
        args.db, args.ann_db, args.cache_dir,
        min_n_display=args.min_n_display, fdr_alpha=args.fdr_alpha,
    )

    if len(agg_df) == 0:
        log.warning('No (type, horizon) cells produced -- nothing to write')
        return

    write_to_db(agg_df, detail_df, meta, db_out)

    print(f'Wrote {len(agg_df)} aggregate cells, {len(detail_df)} event rows to {db_out}')
    print(f'  n_events_total: {meta["n_events_total"]}, n_type_other: {meta["n_type_other"]}')
    print(f'  excluded: outside_range={meta["n_excluded_outside_range"]} '
          f'no_valid_base={meta["n_excluded_no_valid_base"]} '
          f'corporate_event={meta["n_excluded_corporate_event"]} '
          f'outlier_events={meta["n_excluded_outlier"]}')
    print(f'  elapsed: {meta["elapsed_seconds"]:.1f}s')


if __name__ == '__main__':
    main()
