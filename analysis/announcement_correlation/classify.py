# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Headline-regex classifier for announcement types not yet covered by
asx-announcements' structured extraction pipeline (extract_structured.py).

Deliberately kept local to this analysis module rather than extended into
extract_structured.py's _categorise() — these five types are fully
identifiable from headline text alone, and promoting them into the
production scraper would mean new extraction functions, new extracted_*
tables, and a backfill over ~46k already-skipped rows, for a feature that
is explicitly experimental. Promote later if the experiment proves out.

Read-only: never writes to announcements.db.
"""

import re

# First-match-wins, in this order. trading_halt is checked before
# capital_raising/placement since a halt headline often ALSO mentions a
# capital raising it's pending — the halt itself is the more specific and
# more urgent event to isolate.
HEADLINE_PATTERNS = [
    ('trading_halt',    re.compile(r'trading halt', re.I)),
    ('capital_raising', re.compile(r'capital raising', re.I)),
    ('placement',       re.compile(r'\bplacement\b', re.I)),
    ('rights_issue',    re.compile(r'rights issue|entitlement offer', re.I)),
    ('takeover_scheme', re.compile(r'takeover|bidder.?s statement|scheme of arrangement', re.I)),
]


def classify_headline(headline: str) -> str | None:
    """Return the first matching type label, or None if nothing matches."""
    if not headline:
        return None
    for label, pattern in HEADLINE_PATTERNS:
        if pattern.search(headline):
            return label
    return None
