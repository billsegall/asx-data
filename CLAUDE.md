# ASX Data (backend)

Data ingestion pipeline, stock data REST API, and GPU-accelerated signal analysis for ASX market data.
The web frontend lives in a separate repo: `github.com/billsegall/asx-web`.

## Project Structure
- `stockdb/` — data pipeline (fetch scripts, Makefile, SQLite DB)
- `backend/` — Flask stock data API (port 8082) + analysis web pages
- `analysis/` — GPU signal analysis framework (local machine with RTX 4070)

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

### Stock data endpoints
- `GET /api/stock/<symbol>?start=YYYYMMDD&end=YYYYMMDD` — OHLCV, XAO overlay, shorts
- `GET /api/symbols?q=` — symbol search/autocomplete
- `GET /api/shorts` — latest short positions (3-char tickers)
- `POST /api/enrich` — batch enrichment `{"symbols": ["BHP", ...]}` → metrics dict
- `GET /api/symbol/<symbol>` — name, industry, mcap for a single symbol

### Analysis endpoints (serve pre-computed JSON from `analysis/results/`)
- `GET /api/analysis/signals?signal=&industry=&top=` — signal rankings
- `GET /api/analysis/signal/<symbol>` — per-symbol signal scores
- `GET /api/analysis/backtest` — backtest reports
- `GET /api/analysis/discovery` — IC sweep results
- `GET /api/analysis/portfolio` — portfolio backtest series data

### Analysis web pages
- `GET /signals` — signal rankings dashboard (`backend/signals.html`)
- `GET /portfolio` — portfolio backtest chart (`backend/portfolio.html`)

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

## Analysis Framework (`analysis/`)

GPU-accelerated signal research pipeline. Runs locally (RTX 4070, CUDA 12.1, PyTorch 2.4).
DB lives on harri; results are rsynced back for the API to serve.

### Workflow
```bash
./analysis/sync.sh              # pull DB from harri → run predictions → push results (~2.5 min)
./analysis/sync.sh --skip-pull  # skip DB download (use cached local DB)
```

### Signals (three implemented, two placeholders)
- **ShortTrendSignal** — negated 20d slope of short%, cross-sectionally ranked
- **ShortSqueezeSignal** — top-decile short% AND positive 5d momentum
- **VolumeAnomalySignal** — log-volume z-score × sign(5d return)
- CommodityLeadSignal, AnnouncementSignal — placeholders

### CLI scripts (run from repo root)
```bash
python -m analysis.cli.run_predictions --db stockdb/stockdb.db      # current signal scores
python -m analysis.cli.run_signals --db stockdb/stockdb.db          # signals on training data
python -m analysis.cli.run_backtest --db stockdb/stockdb.db         # backtest all signals
python -m analysis.cli.run_discovery --db stockdb/stockdb.db        # IC sweep (slow, ~30 min)
python -m analysis.cli.run_portfolio_backtest --db stockdb/stockdb.db  # portfolio backtest
```

### Train/test split
- **TRAIN_CUTOFF**: `2025-03-01` — signals fitted on data before this date
- **Backtest period**: 2025-03-01 → present (~12 months held-out)
- `BacktestEngine.run()` raises `ValueError` if passed training data

### Portfolio backtest settings
- Entry date: last training day (~2025-02-27)
- $1,000 per stock, top 10 per signal
- **Min short% filter**: only symbols with ≥0.5% short interest at entry are eligible
- Results saved to `analysis/results/portfolio_backtest.json`

### Performance notes
- SQLite cold load: ~90s; parquet cache (in `analysis/cache/`) reduces to ~4s
- Cache auto-invalidates when DB max_date changes
- Duplicate (date, symbol) rows exist in EOD data — dropped at pivot time (~8M rows)
- `rolling_slope` is a Python loop: ~37s for full matrix; acceptable for batch use

### Cron (harri) — predictions refresh
- Commented entry in `stockdb/command.cron`: weekdays at 20:00 UTC after EOD fetch
- Enable by uncommenting and running `./analysis/sync.sh` from local machine instead
