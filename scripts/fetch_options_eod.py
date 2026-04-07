#!/usr/bin/env python3
"""
fetch_options_eod.py — Record live Markit option prices as EOD close.

Runs at end of day to capture the final option prices from Markit and store them
in the endofday table, providing historical EOD data for options contracts.

Usage:
  python3 fetch_options_eod.py [--db /path/to/stockdb.db] [--markit-token TOKEN]

Env vars:
  STOCKDB        (default: ../stockdb/stockdb.db)
  MARKIT_TOKEN   (required: Bearer token for Markit API)
"""

import argparse
import os
import sqlite3
import requests
import logging
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.parent
DB_PATH = SCRIPT_DIR / "stockdb" / "stockdb.db"
MARKIT_URL = "https://asx.api.markitdigital.com/asx-research/1.0/companies/{}/header"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def fetch_option_price(symbol: str, token: str) -> dict | None:
    """Fetch live option price from Markit API."""
    try:
        resp = requests.get(
            MARKIT_URL.format(symbol.upper()),
            headers={'Authorization': f'Bearer {token}'},
            timeout=5,
        )
        if not resp.ok:
            log.warning(f"{symbol}: HTTP {resp.status_code}")
            return None

        d = resp.json().get('data', {})
        return {
            'price': d.get('priceLast'),
            'volume': d.get('volume'),
        }
    except Exception as e:
        log.warning(f"{symbol}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=str(DB_PATH))
    parser.add_argument('--markit-token', default=None)
    args = parser.parse_args()

    # Get MARKIT_TOKEN from arg, env var, or asx-web .env
    token = args.markit_token or os.environ.get('MARKIT_TOKEN')
    if not token:
        # Try to read from asx-web .env
        env_file = Path(__file__).parent.parent.parent / "asx-web" / ".env"
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    if line.startswith('MARKIT_TOKEN='):
                        token = line.split('=', 1)[1].strip()
                        break

    if not token:
        log.error("MARKIT_TOKEN not found in env var, --markit-token, or asx-web/.env")
        return 1

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        return 1

    # Connect to stockdb to get options list
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get all active options
    cursor = conn.cursor()
    cursor.execute("""
        SELECT option_symbol FROM asx_options
        WHERE expiry >= date('now')
        ORDER BY option_symbol
    """)
    options = [row['option_symbol'] for row in cursor.fetchall()]
    conn.close()

    log.info(f"Found {len(options)} active options")

    # Fetch prices from Markit
    today_ts = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    inserted = 0
    failed = []

    for option_symbol in options:
        data = fetch_option_price(option_symbol, token)
        if not data or not data['price']:
            failed.append(option_symbol)
            continue

        # Insert into endofday table
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO endofday
                (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                option_symbol,
                today_ts,
                data['price'],  # use close price for all OHLC
                data['price'],
                data['price'],
                data['price'],
                data['volume'] or 0,
            ))
            conn.commit()
            inserted += 1
            log.info(f"{option_symbol}: {data['price']}")
        except Exception as e:
            log.error(f"{option_symbol}: {e}")
            failed.append(option_symbol)
        finally:
            conn.close()

    log.info(f"✓ Inserted {inserted} option prices")
    if failed:
        log.warning(f"Failed: {', '.join(failed)}")

    return 0 if not failed else 1


if __name__ == '__main__':
    exit(main())
