#!/usr/bin/env python3
"""
fetch_options_eod.py — Record EOD close prices for ASX warrants.

Runs at end of day (4pm AEST). Tries IB Gateway first, falls back to Markit.
Stores in the endofday table for use as default prices on the /options page.

Usage:
  python3 fetch_options_eod.py [--db /path/to/stockdb.db] [--markit-token TOKEN]
                                [--host HOST] [--port PORT]

Env vars:
  STOCKDB        (default: ../stockdb/stockdb.db)
  MARKIT_TOKEN   (required for Markit fallback)
"""

import argparse
import os
import re
import sqlite3
import requests
import logging
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.parent
DB_PATH    = SCRIPT_DIR / "stockdb" / "stockdb.db"
MARKIT_URL = "https://asx.api.markitdigital.com/asx-research/1.0/companies/{}/header"
IB_HOST    = '127.0.0.1'
IB_PORT    = 4001

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def option_to_underlying(sym: str) -> str | None:
    m = re.match(r'^(.+)O[A-Z]$', sym)
    if m:
        return m.group(1)
    m = re.match(r'^(.+)O$', sym)
    if m:
        return m.group(1)
    return None


def fetch_ib_prices(symbols: list[str], host: str, port: int) -> dict[str, float]:
    """Fetch EOD close prices from IB Gateway for a list of ASX warrant symbols.
    Returns {symbol: close_price}. Raises on connection failure.
    """
    import asyncio, random
    from ib_insync import IB, Contract

    asyncio.set_event_loop(asyncio.new_event_loop())
    ib = IB()
    ib.connect(host, port, clientId=random.randint(100, 199), readonly=True, timeout=10)
    ib.reqMarketDataType(1)

    result = {}
    chunk_size = 50
    try:
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]
            contracts = [
                Contract(secType='WAR', localSymbol=sym, exchange='ASX', currency='AUD')
                for sym in chunk
            ]
            qualified = [c for c in ib.qualifyContracts(*contracts) if c.conId]
            if not qualified:
                continue

            tickers = {c.localSymbol: ib.reqMktData(c, genericTickList='', snapshot=False)
                       for c in qualified}
            ib.sleep(2)

            for sym, ticker in tickers.items():
                def _p(v):
                    if v is None:
                        return None
                    import math
                    return None if math.isnan(v) or v == -1.0 else v

                close = _p(ticker.close)
                last  = _p(ticker.last)
                price = close if close is not None else last
                if price is not None and price > 0:
                    result[sym] = price

            for c in qualified:
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass

        return result
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def fetch_markit_price(symbol: str, token: str) -> float | None:
    try:
        resp = requests.get(
            MARKIT_URL.format(symbol.upper()),
            headers={'Authorization': f'Bearer {token}'},
            timeout=5,
        )
        if not resp.ok:
            return None
        d = resp.json().get('data', {})
        return d.get('priceLast')
    except Exception as e:
        log.warning(f"{symbol}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',           default=str(DB_PATH))
    parser.add_argument('--markit-token', default=None)
    parser.add_argument('--host',         default=IB_HOST)
    parser.add_argument('--port',         type=int, default=IB_PORT)
    args = parser.parse_args()

    token = args.markit_token or os.environ.get('MARKIT_TOKEN')
    if not token:
        env_file = Path(__file__).parent.parent.parent / "asx-web" / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith('MARKIT_TOKEN='):
                    token = line.split('=', 1)[1].strip()
                    break

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    options = [r['option_symbol'] for r in conn.execute(
        "SELECT option_symbol FROM asx_options WHERE expiry >= date('now') ORDER BY option_symbol"
    ).fetchall()]
    conn.close()

    log.info(f"Active options: {len(options)}")

    # Phase 1: try IB
    ib_prices = {}
    try:
        ib_prices = fetch_ib_prices(options, args.host, args.port)
        log.info(f"IB returned {len(ib_prices)}/{len(options)} prices")
    except Exception as e:
        log.warning(f"IB unavailable: {e}")

    # Phase 2: Markit for any IB missed
    need_markit = [s for s in options if s not in ib_prices]
    markit_prices = {}
    if need_markit:
        if not token:
            log.warning("MARKIT_TOKEN not set — skipping Markit fallback")
        else:
            log.info(f"Fetching {len(need_markit)} from Markit...")
            for sym in need_markit:
                p = fetch_markit_price(sym, token)
                if p is not None:
                    markit_prices[sym] = p

    all_prices = {sym: p for sym, p in {**markit_prices, **ib_prices}.items() if p and p > 0}  # IB takes priority; drop zeros

    if not all_prices:
        log.error("No prices fetched from either source")
        return 1

    # Store in endofday (midnight AEST as Unix timestamp)
    today_ts = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    conn = sqlite3.connect(db_path)
    try:
        for sym, price in all_prices.items():
            conn.execute(
                "INSERT OR REPLACE INTO endofday (symbol, date, open, high, low, close, volume)"
                " VALUES (?, ?, ?, ?, ?, ?, 0)",
                (sym, today_ts, price, price, price, price)
            )
        conn.commit()
    finally:
        conn.close()

    ib_count     = sum(1 for s in all_prices if s in ib_prices)
    markit_count = sum(1 for s in all_prices if s in markit_prices)
    log.info(f"Stored {len(all_prices)} prices (IB:{ib_count} Markit:{markit_count})")
    missing = [s for s in options if s not in all_prices]
    if missing:
        log.warning(f"No price for {len(missing)} symbols: {', '.join(missing[:20])}")

    return 0


if __name__ == '__main__':
    exit(main())
