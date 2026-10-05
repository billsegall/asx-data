# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Resolve a single `type` label per announcement, combining:
  1. asx-announcements' 15 structured extracted_* tables (most reliable —
     derived from full PDF parsing), joined on ids_id alone.
  2. The headline-regex classifier in classify.py, for events not covered
     by any extracted_* table (trading halts, placements, etc).
  3. 'other' for everything else (mostly the ~46k extraction_log
     status='skipped' rows — expected, not a bug).

ids_id alone (not ids_id+ticker) is the correct join key here: type is a
property of the announcement itself, and a handful of ids_ids legitimately
span two tickers (dual-listed/cross-published notices) — both legs should
resolve to the same type.
"""

import sqlite3

from .classify import classify_headline

# table name -> canonical type label (label = table name minus 'extracted_' prefix,
# except where a shorter/clearer label reads better)
TABLE_TO_TYPE = {
    'extracted_dividends':                 'dividend',
    'extracted_director_notices':          'director',
    'extracted_nta':                       'nta',
    'extracted_meetings':                  'meetings',
    'extracted_splits':                    'splits',
    'extracted_spp':                       'spp',
    'extracted_investor_calls':            'investor_calls',
    'extracted_quarterly_cashflow':        'quarterly_cashflow',
    'extracted_cleansing':                 'cleansing',
    'extracted_suspensions':               'suspensions',
    'extracted_substantial_holdings':      'substantial_holdings',
    'extracted_buybacks':                  'buybacks',
    'extracted_half_year':                 'half_year',
    'extracted_guidance':                  'guidance',
    'extracted_financial_reporting_calendar': 'financial_reporting_calendar',
}

OTHER_TYPE = 'other'


def build_type_index(ann_db_path: str) -> dict:
    """Return {ids_id: type_label} by scanning all extracted_* tables.

    Tables are queried defensively (a future announcements.db schema change
    shouldn't crash this read-only analysis module) — a missing table is
    just skipped, not an error.
    """
    conn = sqlite3.connect(ann_db_path)
    index: dict = {}
    try:
        existing = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for table, label in TABLE_TO_TYPE.items():
            if table not in existing:
                continue
            for (ids_id,) in conn.execute(f'SELECT ids_id FROM {table}'):
                index.setdefault(ids_id, label)
    finally:
        conn.close()
    return index


def resolve_type(ids_id: str, headline: str, extracted_index: dict) -> str:
    """Precedence: extracted-table type > headline-regex type > 'other'."""
    if ids_id in extracted_index:
        return extracted_index[ids_id]
    regex_type = classify_headline(headline)
    return regex_type if regex_type else OTHER_TYPE
