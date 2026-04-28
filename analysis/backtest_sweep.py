#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
Correlation backtest parameter sweep.

Runs a grid of 729 combinations of (min_train_r, min_backtest_r, min_lag_days),
with no_overlap=True and deduplicate_pairs=True fixed.

Usage:
    cd /home/bill/code/asx/asx-data
    python3 analysis/backtest_sweep.py

Output: analysis/results/backtest_sweep.json
"""

import datetime
import json
import multiprocessing
import os
import sqlite3
import sys

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR        = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT         = os.path.dirname(SCRIPT_DIR)  # asx-data/
CORR_DB_PATH      = os.path.join(SCRIPT_DIR, 'results', 'correlations.db')
STOCKDB_PATH      = os.path.join(REPO_ROOT, 'stockdb', 'stockdb.db')
OUTPUT_PATH       = os.path.join(SCRIPT_DIR, 'results', 'backtest_sweep.json')

BACKTEST_START_TS = 1740787200  # 2025-03-01 UTC
_BT_POSITION_SIZE = 1000
_BT_FEE_FLAT      = 6.0
_BT_FEE_PCT       = 0.0008   # 0.08%
_BT_START_BALANCE = 50000

# ---------------------------------------------------------------------------
# Parameter grid
# ---------------------------------------------------------------------------

MIN_TRAIN_R_VALUES    = [0.80, 0.82, 0.84, 0.86, 0.88, 0.90, 0.92, 0.94, 0.96]
MIN_BACKTEST_R_VALUES = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
MIN_LAG_DAYS_VALUES   = [1, 2, 3, 4, 5, 7, 10, 15, 20]

# ---------------------------------------------------------------------------
# Global data (loaded once, shared via module-level globals in each worker)
# ---------------------------------------------------------------------------

_ALL_PAIRS  = None  # list of (leader, follower, lag_days, train_r, backtest_r)
_EOD        = None  # dict: symbol -> {date_str -> (open_p, close_p)}
_CALENDAR   = None  # sorted list of date strings


def _bt_fee(trade_value):
    return max(_BT_FEE_FLAT, _BT_FEE_PCT * trade_value)


def _load_global_data():
    """Load all positive pairs and all EOD prices. Call once in main process."""
    global _ALL_PAIRS, _EOD, _CALENDAR

    if not os.path.exists(CORR_DB_PATH):
        sys.exit(f'ERROR: correlations.db not found at {CORR_DB_PATH}')
    if not os.path.exists(STOCKDB_PATH):
        sys.exit(f'ERROR: stockdb.db not found at {STOCKDB_PATH}')

    # Load all positive-direction pairs (no r filter — we filter per combo)
    cconn = sqlite3.connect(CORR_DB_PATH)
    rows = cconn.execute(
        """SELECT leader, follower, lag_days, train_r, backtest_r
           FROM correlations
           WHERE direction = 'positive' AND backtest_r IS NOT NULL"""
    ).fetchall()
    cconn.close()
    _ALL_PAIRS = rows
    print(f'Loaded {len(_ALL_PAIRS)} positive pairs from correlations.db')

    # Collect all symbols
    all_symbols = set()
    for leader, follower, *_ in _ALL_PAIRS:
        all_symbols.add(leader)
        all_symbols.add(follower)

    # Load EOD prices since backtest start
    sconn = sqlite3.connect(STOCKDB_PATH)
    placeholders = ','.join('?' * len(all_symbols))
    eod_rows = sconn.execute(
        f"""SELECT symbol, date, open, close FROM endofday
            WHERE symbol IN ({placeholders}) AND date >= ?
            ORDER BY symbol, date ASC""",
        list(all_symbols) + [BACKTEST_START_TS]
    ).fetchall()
    sconn.close()

    _EOD = {}
    date_set = set()
    for symbol, ts, open_p, close_p in eod_rows:
        ds = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
        _EOD.setdefault(symbol, {})[ds] = (open_p, close_p)
        date_set.add(ds)

    _CALENDAR = sorted(date_set)
    print(f'Loaded EOD data: {len(_EOD)} symbols, {len(_CALENDAR)} trading days')


# ---------------------------------------------------------------------------
# Worker function — runs in child process
# ---------------------------------------------------------------------------

def _sweep_one(params):
    """
    Run one backtest combination. Uses module-level _ALL_PAIRS, _EOD, _CALENDAR.
    Returns a summary dict.
    """
    min_train_r    = params['min_train_r']
    min_backtest_r = params['min_backtest_r']
    min_lag_days   = params['min_lag_days']

    # Filter pairs
    pairs = [
        p for p in _ALL_PAIRS
        if p[2] >= min_lag_days and p[3] >= min_train_r and p[4] > min_backtest_r
    ]

    if not pairs:
        return {
            'min_train_r':    min_train_r,
            'min_backtest_r': min_backtest_r,
            'min_lag_days':   min_lag_days,
            'n_pairs':        0,
            'n_trades':       0,
            'end_balance':    _BT_START_BALANCE,
            'total_return_pct': 0.0,
            'win_rate':       0.0,
            'avg_pnl':        0.0,
            'total_fees':     0.0,
        }

    # Deduplicate pairs: for each unordered {A,B} keep highest |train_r|
    best = {}
    for p in pairs:
        key = tuple(sorted([p[0], p[1]]))
        if key not in best or abs(p[3]) > abs(best[key][3]):
            best[key] = p
    pairs = list(best.values())

    calendar = _CALENDAR
    eod      = _EOD

    if len(calendar) < 2:
        return {
            'min_train_r': min_train_r, 'min_backtest_r': min_backtest_r,
            'min_lag_days': min_lag_days, 'n_pairs': len(pairs),
            'n_trades': 0, 'end_balance': _BT_START_BALANCE,
            'total_return_pct': 0.0, 'win_rate': 0.0, 'avg_pnl': 0.0, 'total_fees': 0.0,
        }

    follower_busy_until = {}
    pnl_list   = []
    total_fees = 0.0

    for i in range(1, len(calendar)):
        d      = calendar[i]
        d_prev = calendar[i - 1]

        for leader, follower, lag_days, train_r, backtest_r in pairs:
            if i + lag_days >= len(calendar):
                continue

            lc_today = eod.get(leader, {}).get(d)
            lc_prev  = eod.get(leader, {}).get(d_prev)
            if not lc_today or not lc_prev or lc_prev[1] == 0:
                continue

            leader_ret = (lc_today[1] - lc_prev[1]) / lc_prev[1]
            if leader_ret <= 0:
                continue

            buy_date  = calendar[i + 1]
            sell_date = calendar[i + lag_days]

            if buy_date < follower_busy_until.get(follower, ''):
                continue

            buy_data  = eod.get(follower, {}).get(buy_date)
            sell_data = eod.get(follower, {}).get(sell_date)
            if not buy_data or not sell_data:
                continue

            buy_price  = buy_data[0]
            sell_price = sell_data[1]
            if not buy_price or buy_price <= 0:
                continue

            shares  = int((_BT_POSITION_SIZE - _BT_FEE_FLAT) / buy_price)
            buy_fee = _bt_fee(shares * buy_price)
            while shares > 0 and shares * buy_price + buy_fee > _BT_POSITION_SIZE:
                shares -= 1
                buy_fee = _bt_fee(shares * buy_price)
            if shares < 1:
                continue

            sell_fee = _bt_fee(shares * sell_price)
            cost     = shares * buy_price + buy_fee
            proceeds = shares * sell_price - sell_fee
            pnl      = proceeds - cost
            fees     = buy_fee + sell_fee

            if sell_date > follower_busy_until.get(follower, ''):
                follower_busy_until[follower] = sell_date

            pnl_list.append(pnl)
            total_fees += fees

    n_trades  = len(pnl_list)
    n_wins    = sum(1 for p in pnl_list if p > 0)
    total_pnl = sum(pnl_list)
    end_bal   = round(_BT_START_BALANCE + total_pnl, 2)
    total_ret = round((end_bal - _BT_START_BALANCE) / _BT_START_BALANCE * 100, 2)

    return {
        'min_train_r':      min_train_r,
        'min_backtest_r':   min_backtest_r,
        'min_lag_days':     min_lag_days,
        'n_pairs':          len(pairs),
        'n_trades':         n_trades,
        'end_balance':      end_bal,
        'total_return_pct': total_ret,
        'win_rate':         round(n_wins / n_trades * 100, 1) if n_trades else 0.0,
        'avg_pnl':          round(total_pnl / n_trades, 2) if n_trades else 0.0,
        'total_fees':       round(total_fees, 2),
    }


# ---------------------------------------------------------------------------
# Initialiser for multiprocessing workers
# ---------------------------------------------------------------------------

def _worker_init(all_pairs, eod, calendar):
    global _ALL_PAIRS, _EOD, _CALENDAR
    _ALL_PAIRS = all_pairs
    _EOD       = eod
    _CALENDAR  = calendar


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    _load_global_data()

    # Build grid
    grid = [
        {'min_train_r': tr, 'min_backtest_r': br, 'min_lag_days': lag}
        for tr  in MIN_TRAIN_R_VALUES
        for br  in MIN_BACKTEST_R_VALUES
        for lag in MIN_LAG_DAYS_VALUES
    ]
    print(f'Running {len(grid)} combinations with {multiprocessing.cpu_count()} workers...')

    ncpu = multiprocessing.cpu_count()
    with multiprocessing.Pool(
        processes=ncpu,
        initializer=_worker_init,
        initargs=(_ALL_PAIRS, _EOD, _CALENDAR),
    ) as pool:
        results = pool.map(_sweep_one, grid)

    output = {
        'generated_at':   datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'n_combinations': len(results),
        'results':        results,
    }

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f)

    print(f'Saved {len(results)} results to {OUTPUT_PATH}')

    # Quick sanity: find best
    best = max(results, key=lambda r: r['total_return_pct'])
    worst = min(results, key=lambda r: r['total_return_pct'])
    print(f'Best:  train_r={best["min_train_r"]} backtest_r={best["min_backtest_r"]} '
          f'lag={best["min_lag_days"]} → {best["total_return_pct"]:+.2f}% ({best["n_trades"]} trades)')
    print(f'Worst: train_r={worst["min_train_r"]} backtest_r={worst["min_backtest_r"]} '
          f'lag={worst["min_lag_days"]} → {worst["total_return_pct"]:+.2f}%')


if __name__ == '__main__':
    main()
