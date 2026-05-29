# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Generate current-day warrant recommendations → JSON."""

import json
import os
import sqlite3
import time
from datetime import date
import numpy as np

from .data import load_warrant_pairs
from .features import compute_features
from .signals.core import compute_all_signals


def _sigmoid(x: float, scale: float = 1.5) -> float:
    return 1.0 / (1.0 + np.exp(-x / scale))


def _load_industry_map(db_path: str) -> dict[str, str]:
    """Return {symbol: industry} from the symbols table."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT symbol, industry FROM symbols WHERE industry IS NOT NULL").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def generate_predictions(db_path: str, output_path: str) -> str:
    """Score all active warrants and write ranked JSON.

    Returns output_path on success.
    """
    print('[warrants] Loading active warrant pairs...')
    pairs = load_warrant_pairs(db_path, active_only=True)
    print(f'[warrants] {len(pairs)} pairs loaded')
    industry_map = _load_industry_map(db_path)

    today = date.today()
    recommendations = []
    n_skipped = 0

    for pair in pairs:
        feat = compute_features(pair)
        if feat is None:
            n_skipped += 1
            continue

        signals = compute_all_signals(feat)

        # Find last non-stale date
        last_idx = None
        for i in range(len(feat['dates']) - 1, -1, -1):
            if not feat['stale'][i]:
                last_idx = i
                break
        if last_idx is None:
            n_skipped += 1
            continue

        last_dte = feat['dte'][last_idx]
        if last_dte < 15:
            n_skipped += 1
            continue

        # Collect signal scores at last date
        scores = {}
        for sig_name, arr in signals.items():
            v = arr[last_idx]
            if not np.isnan(v):
                scores[sig_name] = round(float(v), 3)

        if not scores:
            n_skipped += 1
            continue

        composite = float(np.mean([_sigmoid(v) for v in scores.values()]))

        def _safe(v):
            return None if (v is None or np.isnan(v)) else round(float(v), 4)

        recommendations.append({
            'option_symbol':    pair['option_symbol'],
            'share_symbol':     pair['share_symbol'],
            'share_name':       pair['share_name'],
            'industry':         industry_map.get(pair['share_symbol'], ''),
            'expiry':           pair['expiry'].isoformat(),
            'strike':           pair['strike'],
            'call_put':         pair['call_put'],
            'dte':              int(last_dte),
            'moneyness':        _safe(feat['moneyness'][last_idx]),
            'warrant_price':    _safe(feat['w_close'][last_idx]),
            'underlying_price': _safe(feat['u_close'][last_idx]),
            'premium_ratio':    _safe(feat['premium_ratio'][last_idx]),
            'signals':          scores,
            'composite_score':  round(composite, 3),
        })

    recommendations.sort(key=lambda r: r['composite_score'], reverse=True)

    out = {
        'generated_at':   int(time.time()),
        'n_considered':   len(pairs),
        'n_skipped':      n_skipped,
        'n_recommended':  len(recommendations),
        'predictions':    recommendations,
    }

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(out, f, indent=2)

    print(f'[warrants] {len(recommendations)} recommendations → {output_path}')
    return output_path
