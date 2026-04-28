#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
Fetch commodity prices from metals.dev API and store in stockdb.

Phase 1: Fetch LEAD only (test integration pattern)
Phase 2: Extend to ALUMINIUM, ZINC, NICKEL

API: https://api.metals.dev/v1/latest?api_key=<KEY>
Free tier: 100 requests/month
Usage: python3 fetch_metals_dev.py --db /path/to/stockdb.db --api-key <KEY> [--symbol LEAD] [--backfill]
"""

import argparse
import sqlite3
import sys
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# API configuration
METALS_DEV_API_URL = "https://api.metals.dev/v1/latest"

# Commodity mappings: metals.dev API key (lowercase) → stockdb commodity ID
# Note: metals.dev uses US spelling (aluminum) while stockdb uses British (ALUMINIUM)
METAL_MAPPINGS = {
    "lead": "LEAD",
    "aluminum": "ALUMINIUM",
    "zinc": "ZINC",
    "nickel": "NICKEL",
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


def fetch_metals_dev_data(api_key: str) -> dict:
    """
    Fetch all metals from metals.dev API.
    Returns: dict with metal symbols as keys, each containing price, currency, timestamp, etc.
    """
    try:
        resp = requests.get(METALS_DEV_API_URL, params={"api_key": api_key}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        log.info(f"Fetched metals.dev data: {len(data)} items")
        return data
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch metals.dev API: {e}")
        raise


def parse_timestamp(ts_str: str) -> int:
    """Convert ISO timestamp string to Unix timestamp (seconds)."""
    try:
        # Parse ISO format: "2026-04-12T10:30:00Z"
        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        return int(dt.timestamp())
    except (ValueError, AttributeError):
        # Fallback: use today's date
        log.warning(f"Could not parse timestamp {ts_str}, using today's date")
        return int(datetime.now(timezone.utc).timestamp())


def get_last_price_date(conn: sqlite3.Connection, commodity_id: str) -> int:
    """Get the most recent Unix date (in seconds) for a commodity."""
    row = conn.execute(
        "SELECT MAX(date) FROM commodity_prices WHERE id = ?",
        (commodity_id,)
    ).fetchone()
    return row[0] if row[0] else 0


def fetch_and_store_metals(
    conn: sqlite3.Connection,
    api_data: dict,
    api_key: str,
    backfill: bool = False,
    symbol: str = None
) -> tuple:
    """
    Parse metals.dev API response and store prices in commodity_prices table.

    API response structure:
    {
        "status": "success",
        "metals": {
            "lead": 1919.3,
            "copper": 13027.6107,
            ...
        },
        "timestamps": {
            "metal": "2026-04-12T03:27:06.264Z",
            ...
        }
    }

    Args:
        conn: SQLite connection
        api_data: JSON response from metals.dev API
        api_key: API key used (for logging)
        backfill: If True, allow overwriting existing prices (not used in Phase 1)
        symbol: If specified, only fetch this symbol (e.g., "LEAD")

    Returns:
        (rows_inserted, rows_skipped, errors)
    """
    rows_inserted = 0
    rows_skipped = 0
    errors = 0

    # Get metals dict and common timestamp
    metals = api_data.get("metals", {})
    timestamp_str = api_data.get("timestamps", {}).get("metal")

    if not timestamp_str:
        log.warning("No metal timestamp in API response")
        errors += 1
        return rows_inserted, rows_skipped, errors

    unix_date = parse_timestamp(timestamp_str)

    # Process each metal in METAL_MAPPINGS
    for api_key_name, commodity_id in METAL_MAPPINGS.items():
        # Phase 1: only if requested symbol or all requested
        if symbol and symbol.upper() != commodity_id.upper():
            continue

        if api_key_name not in metals:
            log.warning(f"Metal '{api_key_name}' not in API response")
            errors += 1
            continue

        price = metals[api_key_name]

        if price is None:
            log.warning(f"No price for {api_key_name}")
            errors += 1
            continue

        try:
            # Check if this (commodity_id, date) pair already exists
            existing = conn.execute(
                "SELECT 1 FROM commodity_prices WHERE id = ? AND date = ?",
                (commodity_id, unix_date)
            ).fetchone()

            if existing:
                log.debug(f"  {commodity_id}: {unix_date} already exists, skipping")
                rows_skipped += 1
                continue

            # Insert new price
            conn.execute(
                "INSERT INTO commodity_prices (id, date, price) VALUES (?, ?, ?)",
                (commodity_id, unix_date, price)
            )
            log.info(f"  {commodity_id}: inserted price ${price:.2f} for date {unix_date} ({timestamp_str})")
            rows_inserted += 1

        except Exception as e:
            log.error(f"  {api_key_name}: error processing: {e}")
            errors += 1

    return rows_inserted, rows_skipped, errors


def run():
    parser = argparse.ArgumentParser(description="Fetch metals from metals.dev API and store in stockdb")
    parser.add_argument("--db", required=True, help="Path to stockdb.db")
    parser.add_argument("--api-key", required=True, help="metals.dev API key")
    parser.add_argument("--symbol", default=None, help="Metal symbol to fetch (default: all configured metals)")
    parser.add_argument("--backfill", action="store_true", help="Backfill mode (allow overwriting)")
    args = parser.parse_args()

    # Validate API key
    if not args.api_key or args.api_key.startswith("$"):
        log.error("API key not provided or not expanded. Set METALS_DEV_API_KEY env var or pass via --api-key")
        sys.exit(1)

    try:
        conn = sqlite3.connect(args.db, timeout=10)
        init_tables(conn)

        # Fetch data from metals.dev API
        api_data = fetch_metals_dev_data(args.api_key)

        # Parse and store
        log.info(f"Processing metals (symbol={args.symbol}, backfill={args.backfill})...")
        inserted, skipped, errors = fetch_and_store_metals(
            conn, api_data, args.api_key, backfill=args.backfill, symbol=args.symbol
        )

        conn.commit()
        conn.close()

        # Summary
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
