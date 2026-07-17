# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Exchange registry — single source of truth for exchange-specific config.

Only ASX is defined today. This module exists so exchange-specific
assumptions (timezone, holidays, IB contract args, yfinance ticker suffix)
live in one place instead of scattered literals, making it cheaper to add
a second exchange later. See ../../CLAUDE.md multi-exchange prep plan.
"""

EXCHANGES: dict[str, dict] = {
    "ASX": {
        "timezone": "Australia/Sydney",
        "holidays": {
            2026: {
                '2026-01-01',  # New Year's Day
                '2026-01-26',  # Australia Day
                '2026-04-03',  # Good Friday
                '2026-04-06',  # Easter Monday
                '2026-06-08',  # King's Birthday (NSW)
                '2026-12-25',  # Christmas Day
                '2026-12-28',  # Boxing Day (substitute — Dec 26 is Saturday)
            },
        },
        "default_currency": "AUD",
        "yf_suffix": ".AX",
        "yf_index_overrides": {"XAO": "^AORD", "XJO": "^AXJO"},
        "ib_exchange": "ASX",
        "ib_currency": "AUD",
        "index_overlay_symbol": "XAO",
        "ical_prodid": "ASX Toolkit",
    },
}

DEFAULT_EXCHANGE = "ASX"


def get_exchange(code: str = DEFAULT_EXCHANGE) -> dict:
    return EXCHANGES[code]


def is_market_closed(date_obj, exchange: str = DEFAULT_EXCHANGE) -> bool:
    """Check if the given exchange is closed on a given date object (holiday only, not weekends)."""
    date_str = date_obj.strftime('%Y-%m-%d')
    holidays = get_exchange(exchange)["holidays"]
    return date_str in holidays.get(date_obj.year, set())


def yf_ticker(symbol: str, exchange: str = DEFAULT_EXCHANGE) -> str:
    """Map a bare symbol to its yfinance ticker for the given exchange."""
    cfg = get_exchange(exchange)
    override = cfg["yf_index_overrides"].get(symbol)
    if override:
        return override
    return f"{symbol}{cfg['yf_suffix']}"


def ib_contract_args(exchange: str = DEFAULT_EXCHANGE) -> tuple[str, str]:
    """Return (ib_exchange, ib_currency) for building an IB Contract()."""
    cfg = get_exchange(exchange)
    return cfg["ib_exchange"], cfg["ib_currency"]
