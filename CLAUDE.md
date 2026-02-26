# ASX Data

Database utilities and web visualiser for ASX stock market data.

## Project Structure
- `stockdb/` — data ingestion pipeline and SQLite3 database
- `www/` — Flask web application

## Running

### Locally
```bash
cd www && ./asx
```

### Docker
```bash
docker compose up
```

## Rules
- App must run both inside Docker (via compose) and directly via `./asx`
- DB is never copied into the Docker image — always mounted as a read-only volume
- DATABASE path is configured via env var; defaults to `../stockdb/stockdb.db` for local use
- Always rebuild the DB and restart the server before committing, so changes can be tested first

## Data Pipeline (`stockdb/`)

### Refreshing data
```bash
make fetch_all       # fetch latest symbols (ASX official) + ASIC shorts CSVs
make                 # rebuild stockdb.db from all source data
```

### Data sources
- **Symbols**: `fetch_symbols.py` downloads from `asx.com.au` → `symbols/asx-official.csv` (updated nightly by ASX)
- **Shares outstanding**: derived at DB build time from the most recent `symbols/ASXListedCompanies-YYYYMMDD.csv` (ListCorp snapshot) using `shares = mcap / last_trade_price`
- **Shorts**: `fetch_shorts.py` downloads ASIC daily YTD CSVs → `shorts/YYYY.csv`
- **Prices**: purchased from eoddata.com; zip files placed in `stockdb/asx-eod-data/zips/`

### Database schema
- `symbols(symbol PK, name, industry, shares)` — shares outstanding
- `shorts(symbol, date, short%)` — daily short positions 2010–present
- `endofday(symbol, date, open, high, low, close, volume)` — daily OHLCV
- `endofmonth(symbol, date, close)` — last trading day of each month

### Market cap
Computed live at query time: `shares × latest close price from endofday`. No stale snapshot date needed.

## Web App (`www/`)

### Routes
- `/` — symbol search
- `/stock/<symbol>` — interactive Plotly chart (candlestick/line, range selector, shorts overlay)
- `/api/stock/<symbol>` — JSON: ohlcv, xao, shorts
- `/shorts-now` — latest short positions table
- `/shorts-historical` — peak short positions table
- `/api/shorts-now`, `/api/shorts-historical` — JSON endpoints for the above
