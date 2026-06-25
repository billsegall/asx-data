# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Unit tests for fetch_options_ib consolidation-adjustment logic (Phase 3).

Tests the DB query and arithmetic without needing IB Gateway.
"""
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))


def _make_db():
    """In-memory DB with asx_options and corporate_events tables."""
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    db.executescript("""
        CREATE TABLE asx_options (
            option_symbol TEXT PRIMARY KEY,
            share_symbol  TEXT,
            exercise      REAL,
            note          TEXT,
            fetched_at    TEXT
        );
        CREATE TABLE corporate_events (
            symbol      TEXT,
            date        INTEGER,
            event_type  TEXT,
            ratio       REAL,
            description TEXT
        );
    """)
    return db


def _phase3_query(db):
    """Run the Phase 3 stale-detection query and return pending dict."""
    stale_rows = db.execute("""
        SELECT o.option_symbol, o.exercise,
               e.date AS event_ts, e.ratio, e.description
        FROM asx_options o
        JOIN corporate_events e ON e.symbol = o.share_symbol
        WHERE e.event_type IN ('consolidation', 'split')
          AND e.date > strftime('%s', o.fetched_at)
        ORDER BY o.option_symbol, e.date
    """).fetchall()

    pending = {}
    for row in stale_rows:
        sym = row['option_symbol']
        if sym not in pending:
            pending[sym] = {'original': row['exercise'], 'exercise': row['exercise'], 'descriptions': []}
        pending[sym]['exercise'] = round(pending[sym]['exercise'] / row['ratio'], 6)
        pending[sym]['descriptions'].append(row['description'])
    return pending


def _ts(date_str):
    """Return unix timestamp for a YYYY-MM-DD string."""
    import datetime
    return int(datetime.datetime.strptime(date_str, '%Y-%m-%d').timestamp())


# ---------------------------------------------------------------------------
# Consolidation adjustment
# ---------------------------------------------------------------------------

class TestConsolidationAdjustment:
    def test_1_in_50_consolidation(self):
        """1:50 consolidation should multiply exercise by 50."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('ATHOA','ATH',0.028,NULL,'2026-05-30 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('ATH',?,  'consolidation', 0.02, '1:50 Consolidation')",
                   (_ts('2026-06-02'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'ATHOA' in pending
        assert pending['ATHOA']['exercise'] == pytest.approx(1.40, rel=1e-4)

    def test_1_in_35_consolidation(self):
        """1:35 consolidation should multiply exercise by 35."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('HLXO','HLX',0.006,NULL,'2026-05-23 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('HLX',?,'consolidation', 0.0285714285714286, '1:35 Consolidation')",
                   (_ts('2026-05-31'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'HLXO' in pending
        assert pending['HLXO']['exercise'] == pytest.approx(0.21, rel=1e-3)

    def test_split_reduces_exercise(self):
        """2:1 split (ratio=2.0) should halve the exercise price."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('XYZOA','XYZ',2.00,NULL,'2026-01-01 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('XYZ',?,'split', 2.0, '2:1 Split')",
                   (_ts('2026-06-01'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'XYZOA' in pending
        assert pending['XYZOA']['exercise'] == pytest.approx(1.00, rel=1e-4)

    def test_event_before_fetch_ignored(self):
        """Consolidation that happened BEFORE fetched_at should not trigger adjustment."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('OLDOA','OLD',1.00,NULL,'2026-06-10 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('OLD',?,'consolidation', 0.1, '1:10 Consolidation')",
                   (_ts('2026-06-01'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'OLDOA' not in pending

    def test_multiple_events_applied_in_order(self):
        """Two consolidations applied sequentially: 1:10 then 1:5 = ×50 total."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('MLTOA','MLT',0.01,NULL,'2026-01-01 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('MLT',?,'consolidation', 0.1, '1:10 Consolidation')",
                   (_ts('2026-03-01'),))
        db.execute("INSERT INTO corporate_events VALUES ('MLT',?,'consolidation', 0.2, '1:5 Consolidation')",
                   (_ts('2026-06-01'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'MLTOA' in pending
        # 0.01 / 0.1 / 0.2 = 0.01 * 10 * 5 = 0.50
        assert pending['MLTOA']['exercise'] == pytest.approx(0.50, rel=1e-4)
        assert len(pending['MLTOA']['descriptions']) == 2

    def test_no_events_returns_empty(self):
        """No events after fetched_at → nothing pending."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('NOPOA','NOP',1.00,NULL,'2026-06-01 00:00:00')")
        db.commit()
        pending = _phase3_query(db)
        assert pending == {}

    def test_multiple_warrants_same_underlying(self):
        """Two warrants on same underlying both get adjusted."""
        db = _make_db()
        db.execute("INSERT INTO asx_options VALUES ('HLXO', 'HLX',0.006,NULL,'2026-05-23 00:00:00')")
        db.execute("INSERT INTO asx_options VALUES ('HLXOA','HLX',0.002,NULL,'2026-05-23 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('HLX',?,'consolidation',0.0285714285714286,'1:35 Consolidation')",
                   (_ts('2026-05-31'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'HLXO'  in pending
        assert 'HLXOA' in pending
        assert pending['HLXO']['exercise']  == pytest.approx(0.21, rel=1e-3)
        assert pending['HLXOA']['exercise'] == pytest.approx(0.07, rel=1e-3)

    def test_already_fetched_post_consolidation_skipped(self):
        """Warrant fetched AFTER consolidation (IB gave adjusted price) not re-adjusted."""
        db = _make_db()
        # ATHO fetched on June 20, consolidation was June 2 — should NOT be adjusted
        db.execute("INSERT INTO asx_options VALUES ('ATHO','ATH',0.5,NULL,'2026-06-20 00:00:00')")
        db.execute("INSERT INTO corporate_events VALUES ('ATH',?,'consolidation',0.02,'1:50 Consolidation')",
                   (_ts('2026-06-02'),))
        db.commit()
        pending = _phase3_query(db)
        assert 'ATHO' not in pending
