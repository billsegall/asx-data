"""CLI: run lead-lag correlations for every ASX industry and store in SQLite.

Usage (from repo root):
    python -m analysis.cli.run_industry_correlations \\
        --db stockdb/stockdb.db \\
        --output-db analysis/results/correlations.db \\
        --max-lag 20 --min-r 0.15 --market-adjust

Results land in correlations.db (rsynced to server by sync.sh alongside other
analysis/results/ files).  The web frontend queries via /api/analysis/correlations/db.
"""

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Per-industry lead-lag correlation discovery')
    parser.add_argument('--db',           required=True,  help='Path to stockdb.db')
    parser.add_argument('--output-db',    required=True,  help='Path to output correlations.db')
    parser.add_argument('--max-lag',      type=int,   default=20,   help='Maximum lag in days')
    parser.add_argument('--min-r',        type=float, default=0.15, help='Minimum |r| threshold')
    parser.add_argument('--fdr-alpha',    type=float, default=0.05, help='FDR alpha')
    parser.add_argument('--market-adjust', action='store_true', default=False,
                        help='Subtract XAO returns')
    parser.add_argument('--no-market-adjust', dest='market_adjust', action='store_false')
    parser.add_argument('--min-symbols',  type=int,   default=5,
                        help='Minimum symbols in industry before running (pre-liquidity-filter)')
    parser.add_argument('--industry',     default=None,
                        help='Run only this industry (for testing, e.g. "Gold")')
    parser.add_argument('--device',       default=None,
                        help='PyTorch device (cuda/cpu). Auto-detected if omitted.')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout,
    )

    from analysis.correlations.pipeline import init_correlations_db, run_pipeline, write_to_db
    from analysis.core.data_loader import DataLoader

    # ── Init DB ──────────────────────────────────────────────────────────────
    init_correlations_db(args.output_db)
    logger.info('Correlations DB ready: %s', args.output_db)

    # ── Build industry → symbols mapping ─────────────────────────────────────
    logger.info('Loading symbol/industry metadata...')
    loader = DataLoader(args.db, split='train')
    sym_df = loader.load_symbols()

    # Restrict to symbols that actually have EOD price data
    active = set(loader.get_active_symbols(min_days=0))
    sym_df = sym_df[sym_df['symbol'].isin(active)].copy()
    sym_df['industry'] = sym_df['industry'].fillna('Unknown').str.strip()
    sym_df = sym_df[sym_df['industry'] != '']

    industry_map = (
        sym_df.groupby('industry')['symbol']
        .apply(list)
        .to_dict()
    )

    # ── Filter to requested industry or all with enough symbols ──────────────
    if args.industry:
        industry_map = {k: v for k, v in industry_map.items() if k == args.industry}
        if not industry_map:
            sys.exit(f'Industry not found: {args.industry!r}. '
                     f'Available: {sorted(sym_df["industry"].unique()[:10])}...')

    industries = sorted(k for k, v in industry_map.items() if len(v) >= args.min_symbols)
    logger.info('Industries to process: %d (min_symbols=%d)', len(industries), args.min_symbols)

    if not industries:
        sys.exit('No industries meet --min-symbols threshold.')

    # ── Per-industry loop ─────────────────────────────────────────────────────
    summary = []
    for idx, industry in enumerate(industries, 1):
        symbols_hint = industry_map[industry]
        print(f'\n[{idx}/{len(industries)}] {industry}  ({len(symbols_hint)} symbols total)')

        try:
            df, meta = run_pipeline(
                db_path=args.db,
                output_dir=None,              # skip CSV — DB only
                max_lag=args.max_lag,
                min_r=args.min_r,
                fdr_alpha=args.fdr_alpha,
                market_adjust=args.market_adjust,
                device=args.device,
                symbols_hint=symbols_hint,
            )
            write_to_db(df, meta, args.output_db, industry)
            summary.append({
                'industry': industry,
                'n_symbols': meta['n_symbols_tested'],
                'n_sig':     meta['n_significant'],
                'n_stable':  meta['n_stable'],
                'elapsed':   meta['elapsed_seconds'],
                'ok':        True,
            })
            print(f'  → {meta["n_significant"]} significant pairs, '
                  f'{meta["n_stable"]} stable  ({meta["elapsed_seconds"]:.1f}s)')

        except Exception as exc:
            logger.error('Industry %r failed: %s', industry, exc, exc_info=True)
            summary.append({'industry': industry, 'ok': False, 'error': str(exc)})

    # ── Summary table ─────────────────────────────────────────────────────────
    print('\n' + '=' * 72)
    print(f'{"Industry":<35} {"Sym":>4} {"Sig":>6} {"Stable":>7} {"Sec":>6}  Status')
    print('-' * 72)
    for s in summary:
        if s['ok']:
            print(f'{s["industry"]:<35} {s["n_symbols"]:>4} {s["n_sig"]:>6} '
                  f'{s["n_stable"]:>7} {s["elapsed"]:>6.1f}  OK')
        else:
            print(f'{s["industry"]:<35} {"":>4} {"":>6} {"":>7} {"":>6}  '
                  f'FAILED: {s.get("error", "?")}')
    print('=' * 72)

    ok = [s for s in summary if s['ok']]
    total_sig    = sum(s['n_sig']    for s in ok)
    total_stable = sum(s['n_stable'] for s in ok)
    print(f'Processed {len(ok)}/{len(summary)} industries — '
          f'{total_sig} significant pairs, {total_stable} stable')
    print(f'Results: {args.output_db}')


if __name__ == '__main__':
    main()
