# ASX Data (backend)

Data ingestion pipeline and stock data REST API for ASX market data.
The web frontend lives in a separate repo: `github.com/billsegall/asx-web`.

## Project Structure
- `stockdb/` — data pipeline (fetch scripts, Makefile, SQLite DB)
- `backend/` — Flask stock data API (port 8082)

## Running the backend

### Locally
```bash
cd backend
FLASK_APP=api.py PYTHONPATH=../stockdb DATABASE=../stockdb/stockdb.db \
  flask run --host=0.0.0.0 --port=8082
```

### On harri (systemd)
```bash
sudo systemctl start asx-backend    # start
sudo systemctl status asx-backend   # check
sudo journalctl -u asx-backend -f   # logs
```
Service file: `/etc/systemd/system/asx-backend.service`
Virtualenv: `backend/venv/`

### Docker
```bash
docker compose up
```

## Rules
- Backend serves stock data only — no user auth, no users.db
- DB is never copied into the Docker image — always mounted as a read-only volume
- `DATABASE` path configured via env var; defaults to `../stockdb/stockdb.db`

## Backend API (`backend/api.py`) — port 8082

### Endpoints
- `GET /api/stock/<symbol>?start=YYYYMMDD&end=YYYYMMDD` — OHLCV, XAO overlay, shorts
- `GET /api/symbols?q=` — symbol search/autocomplete
- `GET /api/shorts` — latest short positions (3-char tickers)
- `POST /api/enrich` — batch enrichment `{"symbols": ["BHP", ...]}` → metrics dict
- `GET /api/symbol/<symbol>` — name, industry, mcap for a single symbol

## Data Pipeline (`stockdb/`)

### Refreshing data
```bash
make update      # incremental: fetch symbols + shorts only (fast, no EOD rebuild)
make fetch_all   # alias for make update
make             # full rebuild of stockdb.db from all source data (slow, needed after new EOD zips)
```

### Data sources
- **Symbols**: `fetch_symbols.py` → `symbols/asx-official.csv` (ASX official, nightly)
- **Shares outstanding**: derived at build time from `symbols/ASXListedCompanies-YYYYMMDD.csv` using `shares = mcap / last_trade_price`
- **Shorts**: `fetch_shorts.py` → `shorts/YYYY.csv` (ASIC public CSVs, 2010–present)
- **Prices**: purchased from eoddata.com; zip files in `asx-eod-data/zips/` (private submodule)

### Database schema
See `Database.md` for full schema. Summary:
- `symbols(symbol PK, name, industry, shares)` — shares outstanding
- `shorts(symbol, date, short%)` — daily short positions 2010–present
- `endofday(symbol, date, open, high, low, close, volume)` — daily OHLCV
- `endofmonth(symbol, date, close)` — last trading day of each month

### Market cap
Computed live: `shares × latest close from endofday`. No stale snapshot.
