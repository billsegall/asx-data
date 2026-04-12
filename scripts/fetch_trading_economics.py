#!/usr/bin/env python3
"""
Fetch commodity prices from Trading Economics.

Supports: coal (thermal), coking-coal, copper, aluminum, zinc, nickel, lead, iron-ore,
          natural-gas, liquefied-natural-gas-japan-korea, lithium, uranium, wheat, corn, soybeans
(Note: oil and brent-oil are excluded as they're already fetched via yfinance and their TE pages require JavaScript rendering)

Usage: python3 fetch_trading_economics.py --db /path/to/stockdb.db [--commodity COAL] [--all]
"""

import argparse
import sqlite3
import sys
import logging
from datetime import datetime, timezone
import re

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Mapping: Trading Economics slug → (stockdb commodity ID, unit)
COMMODITY_MAPPINGS = {
    'coal': ('THERMAL-COAL', 'USD/MT'),
    'coking-coal': ('COKING-COAL', 'USD/MT'),
    'copper': ('COPPER', 'USD/lb'),
    'aluminum': ('ALUMINIUM', 'USD/MT'),
    'zinc': ('ZINC', 'USD/MT'),
    'nickel': ('NICKEL', 'USD/MT'),
    'lead': ('LEAD', 'USD/MT'),
    'iron-ore': ('IRON-ORE', 'USD/MT'),
    'natural-gas': ('NATURAL-GAS', 'USD/MMBtu'),
    'liquefied-natural-gas-japan-korea': ('LNG', 'USD/MMBtu'),
    'lithium': ('LITHIUM', 'CNY/tonne'),
    'uranium': ('URANIUM', 'USD/lb'),
    'wheat': ('WHEAT', 'USc/bushel'),
    'corn': ('CORN', 'USc/bushel'),
    'soybeans': ('SOYBEANS', 'USc/bushel'),
}


def init_tables(conn: sqlite3.Connection) -> None:
    """Ensure commodity tables exist."""
    conn.execute('''CREATE TABLE IF NOT EXISTS commodity_meta (
        id              TEXT PRIMARY KEY,
        name            TEXT NOT NULL,
        unit            TEXT,
        te_symbol       TEXT,
        yf_symbol       TEXT,
        te_no_access    INTEGER DEFAULT 0,
        metals_dev_key  TEXT
    )''')

    conn.execute('''CREATE TABLE IF NOT EXISTS commodity_prices (
        id    TEXT    NOT NULL,
        date  INTEGER NOT NULL,
        price REAL    NOT NULL,
        PRIMARY KEY (id, date)
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_commodity_prices ON commodity_prices(id, date)')
    conn.commit()


def fetch_te_price(slug: str) -> tuple:
    """
    Fetch commodity price from Trading Economics.

    Returns:
        (price, date_str) or (None, None) if failed
    """
    url = f"https://tradingeconomics.com/commodity/{slug}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch Trading Economics {slug}: {e}")
        return None, None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        # Extract price - try multiple patterns
        price_match = None

        # Pattern 1: "trading at XXX.XX"
        price_match = re.search(r'trading at\s*([\d,.]+)', text, re.IGNORECASE)

        # Pattern 2: "fell to XXX.XX USd/Bu" or "rose to XXX.XX"
        if not price_match:
            price_match = re.search(r'(?:fell|rose|up|down) to\s+([\d,.]+)', text, re.IGNORECASE)

        # Pattern 3: Just a number followed by USD/currency indicator
        if not price_match:
            price_match = re.search(r'(\d+\.?\d*)\s*(?:USD|USd|CNY)', text, re.IGNORECASE)

        if not price_match:
            log.warning(f"Could not find price for {slug}")
            return None, None

        price = float(price_match.group(1).replace(',', ''))

        # Extract date - look for patterns like "April 10, 2026" or "April 10, 2026, HH:MM"
        date_match = re.search(r'(\w+ \d+, \d{4})', text)

        if not date_match:
            # Fallback to today's date
            date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
            log.warning(f"Could not find date for {slug}, using today: {date_str}")
        else:
            date_str = date_match.group(1)

        log.info(f"Fetched {slug}: {price} on {date_str}")
        return price, date_str

    except Exception as e:
        log.error(f"Error parsing Trading Economics {slug}: {e}")
        return None, None


def parse_date_str(date_str: str) -> int:
    """Convert 'Month DD, YYYY' to Unix timestamp."""
    try:
        dt = datetime.strptime(date_str, "%B %d, %Y")
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return int(dt_utc.timestamp())
    except ValueError as e:
        log.error(f"Could not parse date '{date_str}': {e}")
        return None


def run():
    parser = argparse.ArgumentParser(description="Fetch commodity prices from Trading Economics")
    parser.add_argument("--db", required=True, help="Path to stockdb.db")
    parser.add_argument("--commodity", help="Specific commodity to fetch (e.g., coal, copper)")
    parser.add_argument("--all", action="store_true", help="Fetch all commodities")
    args = parser.parse_args()

    if not args.commodity and not args.all:
        log.error("Specify --commodity or --all")
        sys.exit(1)

    try:
        conn = sqlite3.connect(args.db, timeout=10)
        init_tables(conn)

        # Determine which commodities to fetch
        slugs = list(COMMODITY_MAPPINGS.keys()) if args.all else [args.commodity.lower()]

        inserted = 0
        skipped = 0
        errors = 0

        for slug in slugs:
            if slug not in COMMODITY_MAPPINGS:
                log.warning(f"Unknown commodity: {slug}")
                errors += 1
                continue

            commodity_id, unit = COMMODITY_MAPPINGS[slug]

            # Fetch price
            price, date_str = fetch_te_price(slug)
            if price is None:
                errors += 1
                continue

            # Parse date
            unix_date = parse_date_str(date_str)
            if unix_date is None:
                errors += 1
                continue

            # Check if this date already exists
            existing = conn.execute(
                "SELECT 1 FROM commodity_prices WHERE id = ? AND date = ?",
                (commodity_id, unix_date)
            ).fetchone()

            if existing:
                log.debug(f"{commodity_id}: price for {date_str} already exists, skipping")
                skipped += 1
                continue

            # Insert new price
            conn.execute(
                "INSERT INTO commodity_prices (id, date, price) VALUES (?, ?, ?)",
                (commodity_id, unix_date, price)
            )
            log.info(f"{commodity_id}: inserted {price} {unit} for {date_str}")
            inserted += 1

        conn.commit()
        conn.close()

        log.info(f"Done — inserted: {inserted}, skipped: {skipped}, errors: {errors}")
        if errors > 0:
            sys.exit(1)

    except sqlite3.Error as e:
        log.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
