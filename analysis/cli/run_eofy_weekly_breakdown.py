# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Weekly decomposition of the EOFY Q4 effect for the most-correlated stocks.

Takes the top-N stocks by |r| from eofy_correlation.db and asks a finer
question than the quarterly pipeline: within Q4 (Apr-Jun), which specific
week(s) carry the Q1-3-vs-Q4 correlation? Reuses the exact same per-FY
inclusion guards as the quarterly pipeline (split/outlier exclusion) so the
FY set matches; only the Q4 side is broken into its 13 weeks instead of one
aggregate return. Investigative script, not part of sync.sh.

Usage:
    python -m analysis.cli.run_eofy_weekly_breakdown --db stockdb/stockdb.db \
        --eofy-db analysis/results/eofy_correlation.db --top 50
"""

import argparse
import sqlite3

import numpy as np
from scipy import stats

from analysis.eofy_correlation.pipeline import (
    _asof_close,
    _fy_boundaries,
    _load_corporate_event_dates,
    _load_eod,
    compute_fy_returns,
)

WEEKS_IN_Q4 = 13  # Apr 1 -> Jun 30 is exactly 91 days in a non-leap year


def _week_boundaries(fy_year: int):
    _, _, q4_start, q4_end = _fy_boundaries(fy_year)
    return [q4_start + pd_offset(7 * k) for k in range(WEEKS_IN_Q4 + 1)]


def pd_offset(days):
    import pandas as pd
    return pd.Timedelta(days=days)


def compute_weekly_returns(dates_arr, closes_arr, fy_year):
    """Return list of 13 week returns (or None where data is missing)."""
    boundaries = _week_boundaries(fy_year)
    closes = [_asof_close(dates_arr, closes_arr, b)[0] for b in boundaries]
    out = []
    for k in range(WEEKS_IN_Q4):
        a, b = closes[k], closes[k + 1]
        if a is None or b is None or a <= 0:
            out.append(None)
        else:
            out.append(b / a - 1)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='stockdb/stockdb.db')
    parser.add_argument('--eofy-db', default='analysis/results/eofy_correlation.db')
    parser.add_argument('--top', type=int, default=50)
    args = parser.parse_args()

    eofy_conn = sqlite3.connect(args.eofy_db)
    top_symbols = [
        row[0] for row in eofy_conn.execute(
            'SELECT symbol FROM eofy_correlation ORDER BY ABS(r) DESC LIMIT ?',
            (args.top,),
        ).fetchall()
    ]
    eofy_conn.close()
    print(f'Top {len(top_symbols)} symbols by |r|: {", ".join(top_symbols[:10])}...')

    conn = sqlite3.connect(args.db)
    eod = _load_eod(conn, top_symbols)
    corp_events = _load_corporate_event_dates(conn, top_symbols)
    conn.close()

    import datetime
    from analysis.eofy_correlation.pipeline import _max_completed_fy_year
    max_fy_year = _max_completed_fy_year(datetime.date.today())

    # pooled[week_idx] = list of (q13_return, week_return)
    pooled = {k: [] for k in range(WEEKS_IN_Q4)}
    n_fy_pairs = 0

    for symbol, sub in eod.groupby('symbol', sort=False):
        dates_arr = sub['date'].values
        closes_arr = sub['close'].values
        records = compute_fy_returns(
            dates_arr, closes_arr, corp_events.get(symbol, []), max_fy_year
        )
        for rec in records:
            if rec['excluded']:
                continue
            week_rets = compute_weekly_returns(dates_arr, closes_arr, rec['fy_year'])
            if any(w is None for w in week_rets):
                continue
            n_fy_pairs += 1
            for k, wr in enumerate(week_rets):
                pooled[k].append((rec['q13_return'], wr))

    print(f'Pooled (symbol, FY) pairs with complete weekly data: {n_fy_pairs}\n')

    q4_start_label = 'Apr 1'
    print(f'{"Week":<6}{"Date range":<16}{"n":>6}{"r":>10}{"p-value":>12}')
    for k in range(WEEKS_IN_Q4):
        pairs = pooled[k]
        n = len(pairs)
        if n < 5:
            print(f'{k+1:<6}{"":<16}{n:>6}{"—":>10}{"—":>12}')
            continue
        q13 = np.array([p[0] for p in pairs])
        wk = np.array([p[1] for p in pairs])
        r_value, p_value = stats.pearsonr(q13, wk)
        start_day = 1 + 7 * k
        end_day = start_day + 6
        label = f'day {start_day}-{end_day}'
        flag = ' ***' if p_value < 0.001 else (' **' if p_value < 0.01 else (' *' if p_value < 0.05 else ''))
        print(f'{k+1:<6}{label:<16}{n:>6}{r_value:>10.3f}{p_value:>12.2e}{flag}')


if __name__ == '__main__':
    main()
