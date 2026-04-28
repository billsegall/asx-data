# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""ASX market holidays by year (verify annually at asx.com.au/about/trading-calendar)"""

ASX_HOLIDAYS: dict[int, set] = {
    2026: {
        '2026-01-01',  # New Year's Day
        '2026-01-26',  # Australia Day
        '2026-04-03',  # Good Friday
        '2026-04-06',  # Easter Monday
        '2026-06-08',  # King's Birthday (NSW)
        '2026-12-25',  # Christmas Day
        '2026-12-28',  # Boxing Day (substitute — Dec 26 is Saturday)
    },
}


def is_asx_closed(date_obj) -> bool:
    """Check if ASX is closed on a given date object."""
    date_str = date_obj.strftime('%Y-%m-%d')
    return date_str in ASX_HOLIDAYS.get(date_obj.year, set())
