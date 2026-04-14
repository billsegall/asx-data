#!/usr/bin/env python3
"""
Fetch manganese prices from Jupiter Mines Shanghai Metals Market data.

URL: https://www.jupitermines.com/tshipi-manganese/tshipi/manganese-price-information
Source: Shanghai Metals Market via Jupiter Mines
Unit: CNY/mtu (Chinese Yuan per metric tonne unit)

Usage: python3 fetch_manganese.py --db /path/to/stockdb.db
"""

import argparse
import sqlite3
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
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

JUPITER_MINES_URL = "https://www.jupitermines.com/tshipi-manganese/tshipi/manganese-price-information"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def init_tables(conn: sqlite3.Connection) -> None:
    """Ensure commodity_meta and commodity_prices tables exist."""
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


def fetch_manganese_price() -> tuple:
    """
    Fetch manganese price from Jupiter Mines / Shanghai Metals Market.

    The price on the page includes 13% VAT. We extract the VAT-included price
    and convert it to VAT-excluded by dividing by 1.13.

    Returns:
        (price_cny_vat_excluded, date_str) or (None, None) if failed
    """
    try:
        resp = requests.get(JUPITER_MINES_URL, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch Jupiter Mines page: {e}")
        return None, None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        # Look for pattern: "as reported by Shanghai Metals Market on DD Month YYYY was:"
        # followed by "CNY/mtu, traded price" and then the price value
        date_match = re.search(r'as reported by Shanghai Metals Market on (\d+ \w+ \d{4})', text)
        if not date_match:
            log.warning("Could not find date in page")
            return None, None

        date_str = date_match.group(1)

        # Find the price value after "CNY/mtu, traded price"
        # Look for pattern: "CNY/mtu, traded price\n\nXX.XX"
        price_match = re.search(
            r'CNY/mtu,\s*traded price\s*\n\s*(\d+\.?\d*)',
            text,
            re.IGNORECASE | re.MULTILINE
        )

        if not price_match:
            log.warning("Could not find price in page")
            return None, None

        price_with_vat = float(price_match.group(1))
        # Convert from VAT-included to VAT-excluded (13% VAT)
        price_vat_excluded = price_with_vat / 1.13

        log.info(f"Fetched manganese price: {price_with_vat} CNY/mtu (incl. 13% VAT) → {price_vat_excluded:.2f} CNY/mtu (excl. VAT) on {date_str}")
        return price_vat_excluded, date_str

    except Exception as e:
        log.error(f"Error parsing page: {e}")
        return None, None


def parse_date_str(date_str: str) -> int:
    """Convert 'DD Month YYYY' to Unix timestamp."""
    try:
        # Parse format like "7 April 2026"
        dt = datetime.strptime(date_str, "%d %B %Y")
        # Convert to UTC timestamp
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return int(dt_utc.timestamp())
    except ValueError as e:
        log.error(f"Could not parse date '{date_str}': {e}")
        return None


def get_last_price_date(conn: sqlite3.Connection) -> int:
    """Get the most recent Unix date for manganese."""
    row = conn.execute(
        "SELECT MAX(date) FROM commodity_prices WHERE id = 'MANGANESE'"
    ).fetchone()
    return row[0] if row[0] else 0


def run():
    parser = argparse.ArgumentParser(description="Fetch manganese price from Jupiter Mines")
    parser.add_argument("--db", required=True, help="Path to stockdb.db")
    parser.add_argument("--backfill", action="store_true", help="Backfill mode (allow overwriting)")
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.db, timeout=10)
        init_tables(conn)

        # Fetch price from Jupiter Mines
        price, date_str = fetch_manganese_price()
        if price is None:
            log.error("Failed to fetch manganese price")
            sys.exit(1)

        # Parse date
        unix_date = parse_date_str(date_str)
        if unix_date is None:
            log.error("Failed to parse date")
            sys.exit(1)

        # Check if this date already exists
        existing = conn.execute(
            "SELECT 1 FROM commodity_prices WHERE id = 'MANGANESE' AND date = ?",
            (unix_date,)
        ).fetchone()

        if existing:
            log.info(f"MANGANESE: price for {date_str} ({unix_date}) already exists, skipping")
            conn.close()
            sys.exit(0)

        # Insert new price
        conn.execute(
            "INSERT INTO commodity_prices (id, date, price) VALUES (?, ?, ?)",
            ('MANGANESE', unix_date, price)
        )
        conn.commit()
        log.info(f"MANGANESE: inserted price {price} CNY/mtu for {date_str}")
        conn.close()

    except sqlite3.Error as e:
        log.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
