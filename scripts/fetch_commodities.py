#!/usr/bin/env python3
"""
fetch_commodities.py — Fetch daily commodity price history from Trading Economics.

Stores prices in commodity_prices table; commodity metadata in commodity_meta.
INSERT OR IGNORE makes re-runs safe — existing rows are never overwritten.

First run: use --backfill to load full history from 2000-01-01.
Subsequent runs: incremental (fetches only since last stored date per commodity).

Usage:
  python3 fetch_commodities.py [--db /path/to/stockdb.db] [--backfill] [--symbol GOLD] [--te-key KEY]

Requirements:
  pip install tradingeconomics yfinance
  TE_API_KEY env var (or --te-key flag) — get a key at developer.tradingeconomics.com
  guest:guest works for limited sample data during testing
"""

import argparse, os, sqlite3, time, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).parent
DEFAULT_DB  = SCRIPT_DIR.parent / 'stockdb' / 'stockdb.db'
BACKFILL_START = '2000-01-01'
DELAY = 1.2  # TE rate limit: 1 req/sec

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Commodity master list
# id          — our internal key (used in API URLs)
# name        — display name
# unit        — price unit for display
# te_symbol   — Trading Economics symbol (pass to getHistoricalMarkets)
# yf_symbol   — yfinance fallback (e.g. 'GC=F'); None if no futures contract
# ---------------------------------------------------------------------------
COMMODITIES = [
    # Precious metals
    {'id': 'GOLD',         'name': 'Gold',              'unit': 'USD/troy oz', 'te_symbol': 'XAUUSD:CUR',       'yf_symbol': 'GC=F'},
    {'id': 'SILVER',       'name': 'Silver',            'unit': 'USD/troy oz', 'te_symbol': 'XAGUSD:CUR',       'yf_symbol': 'SI=F'},
    {'id': 'PLATINUM',     'name': 'Platinum',          'unit': 'USD/troy oz', 'te_symbol': 'XPTUSD:CUR',       'yf_symbol': 'PL=F'},
    {'id': 'PALLADIUM',    'name': 'Palladium',         'unit': 'USD/troy oz', 'te_symbol': 'XPDUSD:CUR',       'yf_symbol': 'PA=F'},
    # Base metals
    {'id': 'COPPER',       'name': 'Copper',            'unit': 'USD/lb',      'te_symbol': 'HG1:COM',          'yf_symbol': 'HG=F'},
    {'id': 'ALUMINIUM',    'name': 'Aluminium',         'unit': 'USD/tonne',   'te_symbol': 'LMAHDS03:COM',     'yf_symbol': None},
    {'id': 'NICKEL',       'name': 'Nickel',            'unit': 'USD/tonne',   'te_symbol': 'LMNIDS03:COM',     'yf_symbol': None},
    {'id': 'ZINC',         'name': 'Zinc',              'unit': 'USD/tonne',   'te_symbol': 'LMZSDS03:COM',     'yf_symbol': None},
    {'id': 'LEAD',         'name': 'Lead',              'unit': 'USD/tonne',   'te_symbol': 'LMPBDS03:COM',     'yf_symbol': None},
    # Bulk commodities (ASX critical)
    {'id': 'IRON-ORE',     'name': 'Iron Ore',          'unit': 'USD/tonne',   'te_symbol': 'IRONORE:COM',      'yf_symbol': None},
    {'id': 'COKING-COAL',  'name': 'Coking Coal',       'unit': 'USD/tonne',   'te_symbol': 'COALCSC:COM',      'yf_symbol': None},
    {'id': 'THERMAL-COAL', 'name': 'Thermal Coal',      'unit': 'USD/tonne',   'te_symbol': 'COALAUUS:COM',     'yf_symbol': None},
    # Energy
    {'id': 'WTI-OIL',      'name': 'Crude Oil (WTI)',   'unit': 'USD/bbl',     'te_symbol': 'CL1:COM',          'yf_symbol': 'CL=F'},
    {'id': 'BRENT-OIL',    'name': 'Crude Oil (Brent)', 'unit': 'USD/bbl',     'te_symbol': 'CO1:COM',          'yf_symbol': 'BZ=F'},
    {'id': 'NATURAL-GAS',  'name': 'Natural Gas',       'unit': 'USD/MMBtu',   'te_symbol': 'NG1:COM',          'yf_symbol': 'NG=F'},
    {'id': 'LNG',          'name': 'LNG (Japan Korea)', 'unit': 'USD/MMBtu',   'te_symbol': 'LNGJKMTF:COM',     'yf_symbol': None},
    # Other metals / minerals
    {'id': 'URANIUM',      'name': 'Uranium',           'unit': 'USD/lb',      'te_symbol': 'URANIUM:COM',      'yf_symbol': 'UX=F'},
    {'id': 'LITHIUM',      'name': 'Lithium',           'unit': 'USD/tonne',   'te_symbol': 'LITHIUM:COM',      'yf_symbol': None},
    # Agriculture
    {'id': 'WHEAT',        'name': 'Wheat',             'unit': 'USc/bushel',  'te_symbol': 'W1:COM',           'yf_symbol': 'ZW=F'},
    {'id': 'CORN',         'name': 'Corn',              'unit': 'USc/bushel',  'te_symbol': 'C1:COM',           'yf_symbol': 'ZC=F'},
    {'id': 'SOYBEANS',     'name': 'Soybeans',          'unit': 'USc/bushel',  'te_symbol': 'S1:COM',           'yf_symbol': 'ZS=F'},
]


def init_tables(conn: sqlite3.Connection) -> None:
    conn.execute('''CREATE TABLE IF NOT EXISTS commodity_meta (
        id        TEXT PRIMARY KEY,
        name      TEXT NOT NULL,
        unit      TEXT,
        te_symbol TEXT,
        yf_symbol TEXT
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS commodity_prices (
        id    TEXT    NOT NULL,
        date  INTEGER NOT NULL,
        price REAL    NOT NULL,
        PRIMARY KEY (id, date)
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_commodity_prices ON commodity_prices(id, date)')
    # Upsert commodity metadata
    conn.executemany(
        'INSERT OR REPLACE INTO commodity_meta (id, name, unit, te_symbol, yf_symbol) VALUES (?,?,?,?,?)',
        [(c['id'], c['name'], c['unit'], c['te_symbol'], c['yf_symbol']) for c in COMMODITIES]
    )
    conn.commit()


def last_stored_date(conn: sqlite3.Connection, commodity_id: str) -> str | None:
    """Return YYYY-MM-DD of the most recent stored price, or None."""
    row = conn.execute(
        'SELECT MAX(date) FROM commodity_prices WHERE id = ?', (commodity_id,)
    ).fetchone()
    ts = row[0]
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')


def _te_ticker(te_symbol: str) -> str:
    """Extract the short ticker from a TE symbol, e.g. 'XAUUSD:CUR' → 'XAUUSD', 'CL1:COM' → 'CL1'."""
    return te_symbol.split(':')[0]


def fetch_te(te_symbol: str, start_date: str) -> list[tuple[int, float]]:
    """Fetch from Trading Economics. Returns [(unix_ts, price), ...]."""
    import tradingeconomics as te  # noqa: PLC0415
    ticker = _te_ticker(te_symbol)
    df = te.getHistoricalByTicker(ticker=ticker, start_date=start_date, output_type='df')
    if df is None or len(df) == 0:
        return []
    rows = []
    date_col  = next((c for c in df.columns if c.lower() == 'date'), None)
    price_col = next((c for c in df.columns if c.lower() in ('close', 'price', 'value', 'last')), None)
    if date_col is None or price_col is None:
        log.warning(f'{ticker}: unexpected columns {list(df.columns)}')
        return []
    for _, row in df.iterrows():
        try:
            dt = row[date_col]
            if hasattr(dt, 'timestamp'):
                ts = int(dt.timestamp())
            else:
                ts = int(datetime.strptime(str(dt)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
            price = float(row[price_col])
            rows.append((ts, price))
        except Exception as e:
            log.debug(f'{ticker} row parse error: {e}')
    return rows


def fetch_yf(yf_symbol: str, start_date: str) -> list[tuple[int, float]]:
    """Fetch from Yahoo Finance as fallback. Returns [(unix_ts, price), ...]."""
    import yfinance as yf
    df = yf.download(yf_symbol, start=start_date, auto_adjust=True, progress=False)
    if df is None or len(df) == 0:
        return []
    # yfinance ≥0.2 returns MultiIndex columns: ('Close', 'GC=F') etc. Flatten to single level.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    rows = []
    for dt, row in df.iterrows():
        try:
            if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                ts = int(dt.timestamp())
            else:
                ts = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
            price = float(row['Close'])
            rows.append((ts, price))
        except Exception as e:
            log.debug(f'{yf_symbol} row parse error: {e}')
    return rows


def run() -> None:
    parser = argparse.ArgumentParser(description='Fetch commodity prices from Trading Economics')
    parser.add_argument('--db',       default=str(DEFAULT_DB), help='Path to stockdb.db')
    parser.add_argument('--backfill', action='store_true',     help='Fetch full history from 2000-01-01')
    parser.add_argument('--symbol',   default=None,            help='Only fetch this commodity ID (e.g. GOLD)')
    parser.add_argument('--te-key',   default=None,            help='Trading Economics API key (overrides TE_API_KEY env var)')
    parser.add_argument('--source',   choices=['te', 'yf'],    default='te', help='Data source (default: te)')
    args = parser.parse_args()

    api_key = args.te_key or os.environ.get('TE_API_KEY', 'guest:guest')
    if args.source == 'te':
        import tradingeconomics as te  # noqa: PLC0415
        te.login(api_key)
        log.info(f'Logged in to Trading Economics (key: {"guest" if api_key == "guest:guest" else "****"})')

    conn = sqlite3.connect(args.db)
    init_tables(conn)

    commodities = [c for c in COMMODITIES if args.symbol is None or c['id'] == args.symbol]
    if not commodities:
        log.error(f'No commodity found with id={args.symbol!r}')
        return

    new_total = skip_total = error_total = 0

    for i, c in enumerate(commodities):
        cid = c['id']
        try:
            if args.backfill:
                start = BACKFILL_START
            else:
                last = last_stored_date(conn, cid)
                if last:
                    # start day after last stored date
                    start = (datetime.strptime(last, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    start = BACKFILL_START

            today = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')
            if start > today:
                log.info(f'{cid}: up to date')
                skip_total += 1
                continue

            if args.source == 'yf':
                if not c['yf_symbol']:
                    log.info(f'{cid}: no yfinance symbol, skipping')
                    skip_total += 1
                    continue
                rows = fetch_yf(c['yf_symbol'], start)
                source_label = f'yf({c["yf_symbol"]})'
            elif c['te_symbol']:
                rows = fetch_te(c['te_symbol'], start)
                source_label = f'te({c["te_symbol"]})'
                time.sleep(DELAY)
            elif c['yf_symbol']:
                log.warning(f'{cid}: no TE symbol, falling back to yfinance')
                rows = fetch_yf(c['yf_symbol'], start)
                source_label = f'yf({c["yf_symbol"]})'
            else:
                log.warning(f'{cid}: no data source configured, skipping')
                skip_total += 1
                continue

            if not rows:
                log.info(f'{cid}: no data returned from {source_label}')
                skip_total += 1
                continue

            cur = conn.executemany(
                'INSERT OR IGNORE INTO commodity_prices (id, date, price) VALUES (?, ?, ?)',
                [(cid, ts, price) for ts, price in rows]
            )
            conn.commit()
            new_rows = cur.rowcount
            new_total += new_rows
            log.info(f'{cid}: +{new_rows} rows from {source_label} (start={start})')

        except Exception as e:
            log.error(f'{cid}: {e}')
            error_total += 1

    log.info(f'Done — new: {new_total}, up-to-date: {skip_total}, errors: {error_total}')
    conn.close()


if __name__ == '__main__':
    run()
