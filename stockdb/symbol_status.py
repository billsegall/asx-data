# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
symbols.current maintenance shared by the backend startup refresh,
fetch_symbol_changes.py and fetch_eod_daily.py.

current=1 means "fetch EOD for this code". Several things turn it off (no EOD
for a while, repeated fetch misses, being renamed away); this module holds the
two rules that need care because ASX reissues codes:

  apply_rename_status   — what symbol_changes says about a code
  listed_inactive       — codes still on the ASX official list but switched off,
                          which fetch_eod_daily probes and re-enables when
                          Yahoo has prices for them (relistings, resumed
                          suspensions, new listings that hadn't traded yet)
"""

import csv
import datetime
import os

# Same window fetch_symbol_changes.migrate_history uses: EOD under the old code
# up to this long after the effective date is the renamed company's own history
# (migrated to the new code). EOD under the old code later than that means the
# code has been reissued to an unrelated listing.
RENAME_GRACE_DAYS = 14

# A code renamed *to* is switched on only while the rename is recent. Older ones
# are left to the normal EOD-based liveness checks — many were later delisted
# and would otherwise be re-enabled on every run.
RENAME_PROMOTE_DAYS = 30

OFFICIAL_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'symbols', 'asx-official.csv')


def apply_rename_status(conn, today=None):
    """Set symbols.current from each code's most recent effective rename event.

    Every code's *latest* event decides, so a reused code (ZIP renamed away in
    2020, then Z1P renamed to ZIP in 2022) isn't switched off by its older
    history:
      - latest event is "renamed to" this code, within RENAME_PROMOTE_DAYS -> current=1
      - latest event is "renamed away" from this code, and it has no EOD after
        effective_date + RENAME_GRACE_DAYS -> current=0
    A code renamed away that has EOD after the grace window has been reissued
    to a new listing, so it's left alone.

    Returns (promoted, demoted) row counts. Caller commits.
    """
    today = today or datetime.date.today().isoformat()
    promote_since = (datetime.date.fromisoformat(today)
                     - datetime.timedelta(days=RENAME_PROMOTE_DAYS)).isoformat()
    status_cte = '''
        WITH ev AS (
            SELECT old_symbol AS sym, effective_date AS d, 0 AS is_new
              FROM symbol_changes WHERE effective_date <= :today
            UNION ALL
            SELECT new_symbol, effective_date, 1
              FROM symbol_changes WHERE effective_date <= :today
        ),
        latest AS (SELECT sym, MAX(d) AS d FROM ev GROUP BY sym),
        st AS (
            -- MAX(is_new): a same-day swap where the code is both old and new counts as new
            SELECT l.sym, l.d, MAX(e.is_new) AS is_new
              FROM latest l JOIN ev e ON e.sym = l.sym AND e.d = l.d
             GROUP BY l.sym
        )
    '''
    params = {'today': today, 'since': promote_since, 'grace': f'+{RENAME_GRACE_DAYS} days'}
    # total_changes deltas: cursor.rowcount is -1 for statements starting with WITH
    before = conn.total_changes
    conn.execute(status_cte + '''
        UPDATE symbols SET current = 1
         WHERE current = 0
           AND symbol IN (SELECT sym FROM st WHERE is_new = 1 AND d >= :since)
    ''', params)
    promoted = conn.total_changes - before
    before = conn.total_changes
    conn.execute(status_cte + '''
        UPDATE symbols SET current = 0
         WHERE current = 1
           AND symbol IN (
               SELECT sym FROM st
                WHERE is_new = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM endofday e
                       WHERE e.symbol = st.sym
                         AND e.date > CAST(strftime('%s', st.d, :grace) AS INTEGER)
                  )
           )
    ''', params)
    return promoted, conn.total_changes - before


def official_codes(path=OFFICIAL_CSV):
    """ASX codes in the official listed-companies CSV (fetch_symbols.py), or None if unavailable."""
    try:
        with open(path, newline='') as f:
            codes = {row[1].strip() for row in csv.reader(f)
                     if len(row) >= 3 and row[1].strip() and row[1].strip() != 'ASX code'}
    except OSError:
        return None
    return codes or None


def listed_inactive(conn, official):
    """current=0 codes that are still on the ASX official list (excluding options/warrants)."""
    rows = conn.execute(
        "SELECT symbol FROM symbols WHERE current = 0"
        " AND name NOT LIKE '%OPTION%' AND name NOT LIKE '%WARRANT%'"
    ).fetchall()
    return sorted(r[0] for r in rows if r[0] in official)
