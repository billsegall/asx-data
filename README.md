# asx-data
ASX data pipeline and stock data REST API.

The web frontend lives in a separate repo: [asx-web](https://github.com/billsegall/asx-web).

## ASX short data

The **raw** short data was obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-position-reports-table/),
and contains some
[inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were then massaged a little (ascii/latin, trailing comma on
dates). See the Makefile for how they're then put into the database.

## ASX price data

The price data currently in use is purchased from eoddata.com and is
proprietary. Should you purchase it you should place the zip files in
a directory named asx-eod-data/zips/ and we should be able to work
with that. Please contact me if that isn't the case.

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

## Known Limitations

### ASX options data

Options data is **not currently available** due to:
- **rosser.com.au blocked by reCAPTCHA** — the only free public source uses reCAPTCHA which prevents automated access
- **No public ASX API** — the ASX does not provide a public API for options
- **Paid alternatives only** — third-party vendors (WebLink, EODHD) require subscriptions

This affects:
- Options charts on `/stock/<symbol>` pages (e.g., `/stock/GNMO`)
- EOD price updates for options

**Potential solutions**:
1. Contact ASX or rosser.com.au requesting API access for data providers
2. Manually download options list periodically and import as CSV
3. Subscribe to a third-party data vendor

See `Database.md` → Known Limitations → Options Data Limitation for details.
