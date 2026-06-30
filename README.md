# asx-data
ASX data pipeline and stock data REST API.

The web frontend lives in a separate repo: [asx-web](https://github.com/billsegall/asx-web).

## ASX short data

Daily short positions fetched from [ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-position-reports-table/) via `fetch_shorts.py`. Data goes back to 2010. Raw CSV files stored in `shorts/YYYY.csv`.

## ASX price data

Historical OHLCV data was originally sourced from eoddata.com (purchased, proprietary; zip files in `asx-eod-data/zips/`). Ongoing daily prices are fetched via IB Gateway at end of day.

## ASX symbol data

Symbol data is fetched from the [ASX official listed companies CSV](https://www.asx.com.au/asx/research/ASXListedCompanies.csv),
which is updated nightly. Run `make fetch_symbols` (or `make fetch_all`) to refresh it.

Shares outstanding are derived at DB build time from the most recent ListCorp
snapshot in `stockdb/symbols/ASXListedCompanies-YYYYMMDD.csv` using
`shares = mcap / last_trade_price`. Market cap is then computed live at query
time as `shares × latest close price`, so it stays current as price data is refreshed.

## Stock splits and consolidations

Split/consolidation events are fetched from Yahoo Finance via `stockdb/fetch_splits.py`.
When a new event is detected, the full adjusted OHLCV history for that symbol is
re-downloaded so pre-split prices are correctly adjusted in `endofday`.

## The database

See `Database.md` for the full schema. Summary of tables:

### symbols
symbol | name | industry | shares | current
------ | ---- | -------- | ------ | -------
BHP    | BHP GROUP LIMITED | Materials | 5,102,905,054 | 1

`current = 0` for delisted or renamed symbols.

### shorts
symbol | date | short
------ | ---- | -------
BHP    | 1771509600 | 0.95

### endofday
symbol | date | open | high | low | close | volume
------ | ---- | ---- | ---- | --- | ----- | ------
BHP    | 1769522400 | 50.50 | 50.86 | 49.88 | 50.60 | 10,136,377

### endofmonth
symbol | date | close
------ | ---- | -----
BHP    | 1769522400 | 50.60

Last trading day of each calendar month — used for efficient multi-period return calculations.

### corporate_events
symbol | date | event_type | ratio | description
------ | ---- | ---------- | ----- | -----------
BTR    | 1704067200 | consolidation | 0.1 | 1:10 Consolidation

## Commodity prices

Global commodity prices relevant to ASX mining and energy sectors. 25 commodities tracked via 4 data sources, updated via scheduled cron jobs.

### commodity_meta
id | name | unit | te_symbol | yf_symbol | metals_dev_key
-- | ---- | ---- | --------- | --------- | ---------------
GOLD | Gold | USD/troy oz | — | GC=F | —
COPPER | Copper | USD/lb | copper | HG=F | —
ALUMINIUM | Aluminium | USD/tonne | aluminum | — | ALUMINUM

`metals_dev_key` stores the metals.dev API key name for commodities sourced from that API (used by `fetch_metals_dev.py`).

### commodity_prices
id | date | price
-- | ---- | -----
GOLD | 1775779200 | 4782.10
LEAD | 1775964665 | 1919.30

Unix timestamps for date; prices in commodity-specific units (see `commodity_meta.unit`).

### Data sources

| Source | Frequency | Commodities | Script |
|--------|-----------|-------------|--------|
| **yfinance** | Daily (weekdays 21:00 UTC) | Gold, Silver, Platinum, Palladium, WTI-Oil, Brent-Oil (6 total) | `fetch_commodities.py --source yf` |
| **Trading Economics** | Weekly (Wed 22:00 UTC) | Thermal Coal, Coking Coal, Copper, Aluminium, Zinc, Nickel, Lead, Iron-Ore, Natural-Gas, LNG (Japan-Korea), Lithium, Uranium, Wheat, Corn, Soybeans (15 total) | `fetch_trading_economics.py --all` |
| **metals.dev API** | Weekly (Sun 22:00 UTC) | Lead, Aluminium, Zinc, Nickel (4 total, free tier: 100 req/month) | `fetch_metals_dev.py --api-key $METALS_DEV_API_KEY --all` |
| **Jupiter Mines** | Weekly (Sat 22:00 UTC) | Manganese (1 total, CNY/mtu, VAT-excluded) | `fetch_manganese.py` |

**Total: 25 commodities** with no source duplication (each commodity sourced from best available API).

### Fetch scripts

#### `fetch_commodities.py --db <db> --source yf`
Fetches yfinance OHLCV data. Default source for precious metals and oils.
- Symbols: GC=F (gold), SI=F (silver), PL=F (platinum), PA=F (palladium), CL=F (WTI oil), BZ=F (Brent oil)
- Incremental mode: skips already-fetched dates
- No API key required

#### `fetch_trading_economics.py --db <db> --all`
Scrapes commodity prices from tradingeconomics.com. Supports 15 commodities with HTML parsing (BeautifulSoup).
- **Bulk commodities**: coal (thermal), coking-coal, iron-ore
- **Metals**: copper, aluminum, zinc, nickel, lead
- **Energy**: natural-gas, liquefied-natural-gas-japan-korea (LNG JKM)
- **Critical minerals**: lithium (CNY/tonne from Shanghai Metals Market), uranium
- **Agriculture**: wheat, corn, soybeans
- Note: oil and brent-oil not supported (pages require JavaScript rendering; use yfinance instead)
- Regex patterns match multiple price formats: "trading at XXX", "fell to XXX", "USD/unit"
- Incremental mode: skips duplicate (commodity_id, date) pairs
- No API key required

#### `fetch_metals_dev.py --db <db> --api-key <KEY> --all`
Fetches industrial metal prices from metals.dev API.
- Commodities: LEAD, ALUMINIUM, ZINC, NICKEL
- Free tier: 100 requests/month (weekly fetch = ~4 requests/month, 96% quota headroom)
- Incremental mode: skips duplicate (commodity_id, date) pairs
- Requires: `METALS_DEV_API_KEY` environment variable or `--api-key` flag
- API endpoint: `https://api.metals.dev/v1/latest?api_key=<KEY>`

#### `fetch_manganese.py --db <db>`
Scrapes manganese prices from Jupiter Mines (Shanghai Metals Market data).
- Commodity: MANGANESE (CNY/mtu, VAT-excluded)
- Converts VAT-included price by dividing by 1.13
- HTML parsing extracts: price, date from "as reported by Shanghai Metals Market on DD Month YYYY"
- Weekly fetch only (historical data sparse on source page)
- No API key required

### Cron schedule

```bash
# Daily commodity prices (yfinance) — weekdays 21:00 UTC (7am AEST next day)
0 21 * * 1-5 python3 $DATA/scripts/fetch_commodities.py --db $STOCKDB/stockdb.db --source yf

# Trading Economics commodities — weekly Wednesday 22:00 UTC (Thursday 8am AEST)
0 22 * * 3 python3 $DATA/scripts/fetch_trading_economics.py --db $STOCKDB/stockdb.db --all

# Manganese from Jupiter Mines — weekly Saturday 22:00 UTC (Sunday 8am AEST)
0 22 * * 6 python3 $DATA/scripts/fetch_manganese.py --db $STOCKDB/stockdb.db

# Industrial metals from metals.dev API — weekly Sunday 22:00 UTC (Monday 8am AEST)
0 22 * * 0 python3 $DATA/scripts/fetch_metals_dev.py --db $STOCKDB/stockdb.db --api-key $METALS_DEV_API_KEY
```

**Why staggered?** Prevents timing conflicts and quota issues. metals.dev single request fetches all 4 metals efficiently.

### Frontend display

Commodity prices displayed on `/commodities` page (asx-web) with:
- Latest price and 24h change percentage
- 52-week high/low with dates (e.g., "Jan 29 — May 14")
- 30-day sparkline chart
- Dashboard pinning (localStorage-based user preferences)
- Group filtering (Metals, Bulk, Energy, Agriculture)

Detail chart at `/commodity/<id>` shows full historical price series with range selector.

### Data quality notes

- **yfinance fallback**: Used as primary source for precious metals (gold, silver) and oils (WTI, Brent) due to continuous high-quality data
- **Duplication avoided**: COPPER also in Trading Economics; yfinance used as primary source to maintain historical consistency
- **Bad data cleaned**: Removed incorrect yfinance contract prices for ALUMINIUM (511→3497), NICKEL (290→17275), ZINC (327→3331)
- **VAT adjustments**: Manganese prices from Jupiter Mines include 13% VAT; script divides by 1.13 to get VAT-excluded value
- **Date granularity**: Most sources provide daily prices; some (like manganese) provide weekly/sparse data only

## ASX Warrant Data

ASX-listed warrants (structured products traded like options) are tracked in the `asx_options` table in `stockdb.db`. Data comes from IB Gateway with Markit as fallback.

### asx_options
option_symbol | expiry | exercise | share_symbol | share_name | note | fetched_at
------------- | ------ | -------- | ------------ | ---------- | ---- | ----------
ACWOC | 2027-09-30 | 0.05 | ACW | ACER CARNEGIE LIMITED | | 2026-04-26

ASX warrant codes encode the underlying: `XXXO` → underlying `XXX`; `XXXO[A-Z]` → underlying `XXX`. IB's `localSymbol` field equals the ASX code exactly, enabling unambiguous matching.

The `note` field is for human annotations only (e.g. `post 1:50 consol`). IB's option type field ('C'/'P') is intentionally not stored here — ASX warrants are almost all Calls.

### Fetch scripts

#### `fetch_options_ib.py --db <db>`
Weekly warrant metadata refresh from IB Gateway. Queries by underlying symbol, matches results by `localSymbol`, updates expiry/strike/name in `asx_options`.
- **Cron**: Sunday 6am AEST (`0 20 * * 6 UTC`)
- **Requires**: IB Gateway running on `127.0.0.1:4001`

**Consolidation/split adjustment (Phase 3)**: After the IB fetch, the script automatically corrects exercise prices for any warrant whose underlying had a corporate event (consolidation or split) recorded in `corporate_events` after the warrant was last fetched. Formula: `new_exercise = old_exercise / ratio` (where `ratio < 1` for consolidations, `ratio > 1` for splits). Warrants successfully updated by IB in the same run are skipped — IB already provides the adjusted strike. `fetched_at` is updated on adjustment so the correction is not re-applied next run.

#### `fetch_options_eod.py --db <db>`
Captures warrant closing prices at end of each trading day. IB Gateway primary source; Markit API fallback for any IB misses. Stores prices in `endofday` table (same as equities).
- **Cron**: Weekdays 4pm AEST (`0 6 * * 2-6 UTC`)
- **Requires**: IB Gateway and/or `MARKIT_TOKEN` env var

### API

`GET /options[?symbol=XXX][?option_symbol=XXXOA]` — returns all warrants (or filtered by underlying / by warrant code) with latest EOD price and date from `endofday` join. The `option_symbol=` reverse lookup is used by asx-web to resolve warrant announcements to the underlying (e.g. VR1O → VR1).

```json
[{"option_symbol": "ACWOC", "expiry": "2027-09-30", "exercise": 0.05,
  "share_symbol": "ACW", "share_name": "ACER CARNEGIE LIMITED",
  "eod_price": 0.004, "eod_date": "2026-04-26", ...}]
```

Live price endpoint in asx-web: `POST /api/option-quotes` — IB Gateway primary, Markit fallback, smart market-hours caching.

## Symbol Changes

ASX ticker renames (e.g. company rebrands, mergers). Displayed on the stock chart page as a historical annotation.

### `symbol_changes`

| Column | Type | Notes |
|--------|------|-------|
| `old_symbol` | `TEXT NOT NULL` | Previous ASX ticker |
| `new_symbol` | `TEXT NOT NULL` | New ASX ticker |
| `effective_date` | `TEXT NOT NULL` | YYYY-MM-DD |
| PRIMARY KEY | `(old_symbol, new_symbol, effective_date)` | |

Populated by `asx-data/scripts/fetch_symbol_changes.py` (daily cron).

`GET /symbol-changes?symbol=XXX` — returns changes for one symbol (old or new).

---

## Crypto Prices

Top-100 cryptocurrencies by market cap, with daily OHLCV history. Data from CoinGecko (metadata + rankings) and yfinance (price history). Displayed at `/crypto`.

### `crypto_meta`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT PRIMARY KEY` | Ticker symbol (e.g. `BTC`, `ETH`) |
| `name` | `TEXT NOT NULL` | Full name (e.g. `Bitcoin`) |
| `cg_id` | `TEXT` | CoinGecko slug (e.g. `bitcoin`) |
| `yf_symbol` | `TEXT` | yfinance symbol (e.g. `BTC-USD`) |
| `rank` | `INTEGER` | Market cap rank |
| `price` | `REAL` | Latest price (USD) |
| `change_pct_24h` | `REAL` | 24h change as a percentage |
| `market_cap` | `REAL` | Market capitalisation (USD) |
| `volume_24h` | `REAL` | 24h trading volume (USD) |
| `updated_at` | `TEXT` | ISO datetime of last fetch |

### `crypto_prices`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT NOT NULL` | Matches `crypto_meta.id` |
| `date` | `INTEGER NOT NULL` | Unix timestamp (midnight UTC) |
| `open` / `high` / `low` / `close` | `REAL` | OHLCV (USD) |
| `volume` | `REAL` | |
| PRIMARY KEY | `(id, date)` | |

Populated by `scripts/fetch_crypto.py` (daily 21:30 UTC). First run: `--backfill` loads 2 years.

`GET /api/crypto` — all coins with 30-day sparklines. `GET /api/crypto/<id>` — full history for one coin.

---

## Currency Pairs (FX)

Daily FX rates for 14 pairs: 8 AUD-centric pairs and 6 major crosses. Data from Yahoo Finance via yfinance. Displayed at `/currencies`.

### `currency_meta`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT PRIMARY KEY` | Pair ID (e.g. `AUDUSD`, `EURUSD`) |
| `base` | `TEXT NOT NULL` | Base currency (e.g. `AUD`) |
| `quote` | `TEXT NOT NULL` | Quote currency (e.g. `USD`) |
| `yf_symbol` | `TEXT NOT NULL` | yfinance ticker (e.g. `AUDUSD=X`) |
| `group_name` | `TEXT` | `'AUD Pairs'` or `'Majors'` |
| `price` | `REAL` | Latest rate (quote per 1 base unit) |
| `change_pct_24h` | `REAL` | 24h change as a percentage |
| `updated_at` | `TEXT` | ISO datetime of last fetch |

### `currency_prices`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT NOT NULL` | Matches `currency_meta.id` |
| `date` | `INTEGER NOT NULL` | Unix timestamp (midnight UTC) |
| `close` | `REAL NOT NULL` | Daily close rate |
| PRIMARY KEY | `(id, date)` | |

Populated by `scripts/fetch_currencies.py` (daily 21:35 UTC). First run: `--backfill` loads 2 years.

Pairs tracked:

| Group | Pairs |
|-------|-------|
| AUD Pairs | AUD/USD, AUD/EUR, AUD/GBP, AUD/JPY, AUD/NZD, AUD/CNY, AUD/CAD, AUD/SGD |
| Majors | EUR/USD, GBP/USD, USD/JPY, USD/CAD, USD/CHF, NZD/USD |

`GET /api/currencies` — all pairs with 30-day sparklines. `GET /api/currencies/<id>` — full history + 52w high/low.

---

## Kronos Predictions

Snapshot history of Kronos model prediction runs. One row per (run, symbol). Enables comparison across runs on the dashboard Kronos Top Picks panel.

### `kronos_predictions`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `generated_at` | `TEXT NOT NULL` | ISO datetime of the prediction run |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `score` | `REAL NOT NULL` | Predicted 5-day forward return score |
| `date` | `INTEGER NOT NULL` | Unix timestamp of the prediction date |
| `name` | `TEXT` | Company name |
| `industry` | `TEXT` | Industry group |

Populated by the Kronos analysis pipeline on realiti (GPU machine), synced to harri via `analysis/sync.sh`. Predictions stored via `POST /api/analysis/kronos-predictions`.

`GET /api/analysis/kronos-predictions` — latest run. `GET /api/analysis/kronos-predictions/history` — all historical runs for comparison.
