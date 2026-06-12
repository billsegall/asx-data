#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
Score-threshold backtest for Kronos 5d forecasts.

Slices trades by score thresholds (1.0, 1.5, 2.0) and by market-cap bucket,
using per-date forecast+actual data already stored in backtest_kronos.json.

Usage:
    cd /home/bill/code/asx/asx-data
    python3 analysis/threshold_backtest.py
"""

import json
import sqlite3
import sys
from pathlib import Path
import numpy as np

SCRIPT_DIR   = Path(__file__).parent
REPO_ROOT    = SCRIPT_DIR.parent
BACKTEST_JSON = SCRIPT_DIR / 'results' / 'backtest_kronos.json'
STOCKDB_PATH  = REPO_ROOT / 'stockdb' / 'stockdb.db'

THRESHOLDS = [1.0, 1.5, 2.0]

# Market cap buckets (AUD)
MCAP_BUCKETS = {
    'large':  (2_000_000_000,   None),
    'mid':    (  300_000_000,   2_000_000_000),
    'small':  (   50_000_000,     300_000_000),
    'micro':  (   10_000_000,      50_000_000),
    'nano':   (            0,      10_000_000),
}


def load_mcap(conn: sqlite3.Connection) -> dict[str, float]:
    """Return {symbol: mcap_aud} using most recent close * shares."""
    rows = conn.execute(
        "SELECT s.symbol, s.shares, e.close "
        "FROM symbols s "
        "JOIN endofday e ON e.symbol = s.symbol "
        "WHERE s.shares > 0 "
        "AND e.date = (SELECT MAX(date) FROM endofday WHERE symbol = s.symbol)"
    ).fetchall()
    return {sym: shares * close for sym, shares, close in rows}


def bucket(mcap_aud: float) -> str:
    for name, (lo, hi) in MCAP_BUCKETS.items():
        if mcap_aud >= lo and (hi is None or mcap_aud < hi):
            return name
    return 'nano'


def portfolio_stats(returns: list[float]) -> dict:
    if not returns:
        return dict(n=0, hit_rate=None, mean_return=None, sharpe=None, total_return=None)
    r = np.array(returns)
    hit = float((r > 0).mean())
    mean_r = float(r.mean())
    sharpe = float(r.mean() / (r.std() + 1e-10) * (252 ** 0.5)) if len(r) > 1 else None
    total = float((1 + r).prod() - 1)
    return dict(n=len(r), hit_rate=round(hit, 4), mean_return=round(mean_r, 5),
                sharpe=round(sharpe, 3) if sharpe else None, total_return=round(total, 4))


def run():
    data = json.loads(BACKTEST_JSON.read_text())
    dates = data['dates']
    params = data.get('params', {})
    print(f"Loaded {len(dates)} backtest dates  "
          f"({dates[0]['date']} → {dates[-1]['date']})")
    print(f"Params: step_days={params.get('step_days')}, pred_len={params.get('pred_len')}\n")

    conn = sqlite3.connect(str(STOCKDB_PATH))
    mcap = load_mcap(conn)
    conn.close()
    print(f"Market cap loaded for {len(mcap)} symbols\n")

    # Collect all (score, return, mcap_bucket) triples
    all_trades: list[dict] = []
    for day in dates:
        forecasts = day.get('forecasts', {})
        actuals   = day.get('actual', {})
        for sym, score in forecasts.items():
            if sym not in actuals:
                continue
            ret = actuals[sym]
            if ret is None:
                continue
            m = mcap.get(sym)
            bkt = bucket(m) if m else 'unknown'
            all_trades.append(dict(
                date=day['date'], symbol=sym, score=score,
                ret=ret, mcap=m, bucket=bkt,
            ))

    print(f"Total trade opportunities: {len(all_trades)}\n")

    # ── Score-threshold results ──────────────────────────────────────────────
    print("=" * 65)
    print(f"{'Threshold':>12}  {'N':>6}  {'HitRate':>8}  {'MeanRet':>9}  {'Sharpe':>7}  {'TotalRet':>9}")
    print("-" * 65)

    threshold_results = {}
    for thr in THRESHOLDS:
        bucket_trades = [t for t in all_trades if t['score'] >= thr]
        stats = portfolio_stats([t['ret'] for t in bucket_trades])
        threshold_results[thr] = dict(threshold=thr, **stats)
        hr   = f"{stats['hit_rate']:.1%}" if stats['hit_rate'] is not None else '—'
        mr   = f"{stats['mean_return']:+.2%}" if stats['mean_return'] is not None else '—'
        sh   = f"{stats['sharpe']:.2f}" if stats['sharpe'] is not None else '—'
        tot  = f"{stats['total_return']:+.1%}" if stats['total_return'] is not None else '—'
        print(f"  score≥{thr:<5}  {stats['n']:>6}  {hr:>8}  {mr:>9}  {sh:>7}  {tot:>9}")

    # Also show "all signals" baseline
    stats_all = portfolio_stats([t['ret'] for t in all_trades])
    hr  = f"{stats_all['hit_rate']:.1%}"
    mr  = f"{stats_all['mean_return']:+.2%}"
    sh  = f"{stats_all['sharpe']:.2f}"
    tot = f"{stats_all['total_return']:+.1%}"
    print(f"  {'(all)':>10}  {stats_all['n']:>6}  {hr:>8}  {mr:>9}  {sh:>7}  {tot:>9}")
    print()

    # ── Score-threshold × market-cap breakdown ───────────────────────────────
    print("=" * 75)
    print("Score threshold × market-cap breakdown")
    print("=" * 75)

    for thr in THRESHOLDS:
        bucket_trades_all = [t for t in all_trades if t['score'] >= thr]
        print(f"\n  score ≥ {thr}  (n={len(bucket_trades_all)})")
        print(f"  {'Bucket':>8}  {'N':>6}  {'HitRate':>8}  {'MeanRet':>9}  {'Sharpe':>7}  {'TotalRet':>9}")
        print(f"  {'-'*63}")
        for bkt in ['large', 'mid', 'small', 'micro', 'nano']:
            trades = [t for t in bucket_trades_all if t['bucket'] == bkt]
            stats = portfolio_stats([t['ret'] for t in trades])
            if stats['n'] == 0:
                print(f"  {bkt:>8}  {'—':>6}")
                continue
            hr   = f"{stats['hit_rate']:.1%}"
            mr   = f"{stats['mean_return']:+.2%}"
            sh   = f"{stats['sharpe']:.2f}" if stats['sharpe'] else '—'
            tot  = f"{stats['total_return']:+.1%}"
            print(f"  {bkt:>8}  {stats['n']:>6}  {hr:>8}  {mr:>9}  {sh:>7}  {tot:>9}")


if __name__ == '__main__':
    run()
