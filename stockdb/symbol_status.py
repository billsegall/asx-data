# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
symbols.current maintenance shared by the backend startup refresh,
fetch_symbol_changes.py and fetch_eod_daily.py.

current=1 means "fetch EOD for this code". Several things turn it off (no EOD
for a while, repeated fetch misses, being renamed away); this module holds the
two rules that need care because ASX reissues codes:

  apply_rename_status   — what symbol_changes says about a code
  reenable_candidates   — switched-off codes that fetch_eod_daily probes and
                          re-enables when Yahoo has trades for them (relistings,
                          resumed suspensions, new listings that hadn't traded
                          yet, ETFs switched off by a run of fetch misses)
  update_delisted       — symbols.delisted_on: off the official list and no
                          trade for DELIST_AFTER_DAYS. History is kept; the
                          code just stops being fetched, probed and shown as live

Only fetch_eod_daily's re-enable check and recent renames switch codes back
on. The backend's startup refresh only switches codes off.
"""

import csv
import datetime
import os
import sqlite3

# Same window fetch_symbol_changes.migrate_history uses: EOD under the old code
# up to this long after the effective date is the renamed company's own history
# (migrated to the new code). EOD under the old code later than that means the
# code has been reissued to an unrelated listing.
RENAME_GRACE_DAYS = 14

# A code renamed *to* is switched on only while the rename is recent. Older ones
# are left to the normal EOD-based liveness checks — many were later delisted
# and would otherwise be re-enabled on every run.
RENAME_PROMOTE_DAYS = 30

# Off the official list with no trade for this long = delisted (update_delisted).
# Matches fetch_eod_daily's 90-day no-EOD switch-off.
DELIST_AFTER_DAYS = 90

# Off-list codes (ETFs, funds, hybrids) that were switched off are probed for
# trades while their last trade is this recent (reenable_candidates).
RECENT_TRADE_DAYS = 365

OFFICIAL_CSV =os.path.join(os.path.dirname(os.path.abspath(__file__)), 'symbols', 'asx-official.csv')


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


def last_trade_dates(conn):
    """{symbol: date of its latest endofday row}, using the local-midnight timestamps endofday stores."""
    return {sym: datetime.date.fromtimestamp(ts)
            for sym, ts in conn.execute('SELECT symbol, MAX(date) FROM endofday GROUP BY symbol')
            if ts is not None}


def reenable_candidates(conn, official, today, last_trade=None):
    """current=0 codes worth probing for trades (excluding options/warrants and delisted codes).

    Codes on the ASX official list always qualify (suspensions, relistings, new
    listings). That list only covers companies, so ETFs, funds and hybrids never
    appear on it: those qualify while they traded within RECENT_TRADE_DAYS, so
    one switched off by a run of fetch misses (a Yahoo outage) comes back.
    """
    today = _as_date(today)
    last_trade = last_trade if last_trade is not None else last_trade_dates(conn)
    recent = today - datetime.timedelta(days=RECENT_TRADE_DAYS)
    rows = conn.execute(
        "SELECT symbol FROM symbols WHERE current = 0 AND delisted_on IS NULL"
        " AND name NOT LIKE '%OPTION%' AND name NOT LIKE '%WARRANT%'"
    ).fetchall()
    return sorted(sym for (sym,) in rows
                  if sym in official or (sym in last_trade and last_trade[sym] > recent))


def ensure_delisted_column(conn):
    """Add symbols.delisted_on if missing (older databases)."""
    cols = {r[1] for r in conn.execute('PRAGMA table_info(symbols)')}
    if 'delisted_on' not in cols:
        conn.execute('ALTER TABLE symbols ADD COLUMN delisted_on TEXT')


def update_delisted(conn, official, today, last_trade=None):
    """Set or clear symbols.delisted_on. Returns (newly delisted codes, cleared codes).

    A code is delisted when it's off the ASX official list and hasn't traded for
    DELIST_AFTER_DAYS; delisted_on is its last trade date, the closest thing we
    have to the real delisting date. Both conditions are needed because the
    official list leaves out ETFs, funds and hybrids. Codes with no EOD at all
    are left alone (nothing to date them by), as are codes renamed away around
    their last trade — a rename isn't a delisting, and symbol_changes covers it.
    Delisted codes are switched off and no longer probed by the re-enable check.

    Cleared when the code is back on the list or trading again (ASX reissues codes).
    """
    today = _as_date(today)
    last_trade = last_trade if last_trade is not None else last_trade_dates(conn)
    cutoff = today - datetime.timedelta(days=DELIST_AFTER_DAYS)
    try:
        renamed_away = {old: d for old, d in conn.execute(
            'SELECT old_symbol, MAX(effective_date) FROM symbol_changes WHERE effective_date <= ? '
            'GROUP BY old_symbol', (today.isoformat(),))}
    except sqlite3.OperationalError:
        renamed_away = {}  # symbol_changes not created yet (fresh DB)

    newly, cleared = [], []
    for sym, delisted_on in conn.execute('SELECT symbol, delisted_on FROM symbols').fetchall():
        last = last_trade.get(sym)
        gone = sym not in official and last is not None and last <= cutoff
        if gone and sym in renamed_away:
            renamed = datetime.date.fromisoformat(renamed_away[sym])
            if renamed >= last - datetime.timedelta(days=RENAME_GRACE_DAYS):
                gone = False
        if gone and delisted_on is None:
            newly.append((sym, last.isoformat()))
        elif not gone and delisted_on is not None:
            cleared.append(sym)
    conn.executemany('UPDATE symbols SET delisted_on = ?, current = 0 WHERE symbol = ?',
                     [(d, sym) for sym, d in newly])
    conn.executemany('UPDATE symbols SET delisted_on = NULL WHERE symbol = ?', [(s,) for s in cleared])
    return [sym for sym, _ in newly], cleared


def _as_date(d):
    return datetime.date.fromisoformat(d) if isinstance(d, str) else d
