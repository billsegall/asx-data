# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Load (warrant, underlying) price pairs from stockdb."""

import sqlite3
from datetime import date, datetime
import numpy as np
import pandas as pd


def load_warrant_pairs(db_path: str, active_only: bool = False) -> list[dict]:
    """Load warrant metadata + aligned price series.

    Expiry handling:
      - Active warrants (expiry >= today): use expiry from asx_options.
      - Expired/historical: infer as last date where warrant close > 0.001.

    Filters applied:
      - Both warrant and underlying have >= 30 common price dates.
      - Warrant last price > 0.001 (skip permanently zero-priced).
      - Underlying median daily traded value >= $100k.

    Returns list of dicts:
        option_symbol, share_symbol, share_name, expiry (date),
        strike (float), call_put ('C'/'P'),
        warrant_df (DataFrame: date_unix, close),
        underlying_df (DataFrame: date_unix, close, volume, shorts_pct)
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    today_ts = int(datetime.combine(date.today(), datetime.min.time()).timestamp())

    warrants = pd.read_sql_query(
        """SELECT option_symbol, share_symbol, share_name,
                  expiry, exercise AS strike,
                  UPPER(COALESCE(note, 'C')) AS call_put
           FROM asx_options""",
        conn,
    )

    if active_only:
        warrants = warrants[warrants['expiry'] >= date.today().isoformat()]

    if warrants.empty:
        conn.close()
        return []

    opt_syms = warrants['option_symbol'].tolist()
    shr_syms = warrants['share_symbol'].unique().tolist()

    ph_opt = ','.join('?' * len(opt_syms))
    ph_shr = ','.join('?' * len(shr_syms))

    warrant_prices = pd.read_sql_query(
        f"SELECT symbol, date, close FROM endofday WHERE symbol IN ({ph_opt}) ORDER BY symbol, date",
        conn, params=opt_syms,
    )

    underlying_prices = pd.read_sql_query(
        f"SELECT symbol, date, close, volume FROM endofday WHERE symbol IN ({ph_shr}) ORDER BY symbol, date",
        conn, params=shr_syms,
    )

    shorts = pd.read_sql_query(
        f"SELECT symbol, date, short AS shorts_pct FROM shorts WHERE symbol IN ({ph_shr}) ORDER BY symbol, date",
        conn, params=shr_syms,
    )
    conn.close()

    today = date.today()
    pairs = []

    for _, w in warrants.iterrows():
        opt_sym = w['option_symbol']
        shr_sym = w['share_symbol']

        wp = warrant_prices[warrant_prices['symbol'] == opt_sym].sort_values('date').copy()
        if len(wp) < 20:
            continue

        # Determine expiry
        try:
            expiry = datetime.strptime(w['expiry'], '%Y-%m-%d').date()
        except Exception:
            expiry = None

        if expiry and expiry >= today:
            # Active: use recorded expiry
            pass
        else:
            # Expired or unknown: infer from last day price > 0.001
            active_wp = wp[wp['close'] > 0.001]
            if active_wp.empty:
                continue
            last_ts = int(active_wp['date'].max())
            expiry = datetime.fromtimestamp(last_ts).date()

        # Filter to rows where price > 0.001 (stale zeros not useful as returns)
        wp = wp[wp['close'] > 0.001].reset_index(drop=True)
        if len(wp) < 20:
            continue

        up = underlying_prices[underlying_prices['symbol'] == shr_sym].sort_values('date').copy()
        if len(up) < 30:
            continue

        # Liquidity: underlying must have some trading activity
        daily_value = up['close'] * up['volume']
        if daily_value.median() < 1_000:
            continue

        # Align on common dates
        common = set(wp['date'].values) & set(up['date'].values)
        if len(common) < 30:
            continue

        wp = wp[wp['date'].isin(common)].sort_values('date').reset_index(drop=True)
        up = up[up['date'].isin(common)].sort_values('date').reset_index(drop=True)

        # Merge shorts onto underlying
        sp = (
            shorts[shorts['symbol'] == shr_sym][['date', 'shorts_pct']]
            .sort_values('date')
        )
        up = up.merge(sp, on='date', how='left')
        up['shorts_pct'] = up['shorts_pct'].fillna(0.0)

        # Normalise call_put: keep first char, default C
        cp_raw = str(w['call_put'])
        call_put = 'C' if 'C' in cp_raw else ('P' if 'P' in cp_raw else 'C')

        pairs.append({
            'option_symbol': opt_sym,
            'share_symbol':  shr_sym,
            'share_name':    w.get('share_name', ''),
            'expiry':        expiry,
            'strike':        float(w['strike']),
            'call_put':      call_put,
            'warrant_df':    wp[['date', 'close']],
            'underlying_df': up[['date', 'close', 'volume', 'shorts_pct']],
        })

    return pairs
