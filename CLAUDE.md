# ASX Data (backend)

> **Project-wide guidance**: see `../CLAUDE.md` (parent `asx` repo) for architecture, restart commands, and crontab.

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

### On server (systemd)
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
- `GET /api/analysis/eofy-correlations` — per-stock EOFY tax-loss/gain correlation (own Q1-3 return vs own Q4 return, across FYs); filters: `industry`, `mcap_min`, `mcap_max`, `min_r`, `min_n_years`, `max_fdr_p`, `sort`, `order`, `limit`
- `GET /api/analysis/eofy-correlations/industries` — per-industry counts for the above
- `GET /api/analysis/eofy-correlations/<symbol>` — per-symbol FY-by-FY detail + OLS slope/intercept
- `GET /api/analysis/eofy-correlations/windows` — sub-window definitions (Late May day57-70 / Rest of Q4 day71-91) + run stats
- `GET /api/analysis/eofy-correlations/window/<A|B>` — same filters as `/eofy-correlations`, tested against Q1-3 vs that sub-window instead of full Q4
- `GET /api/analysis/eofy-correlations/window/<A|B>/<symbol>` — per-symbol sub-window FY detail + OLS fit
- `GET /api/analysis/announcement-correlations` — announcement type vs forward-return (1/5/20/60 trading days, excess of XAO) by type×horizon×price_sensitive; filters: `type`, `horizon`, `price_sensitive`, `min_n`, `max_fdr_p`, `sort`, `order`, `limit`
- `GET /api/analysis/announcement-correlations/meta` — latest run's event count + exclusion-reason breakdown
- `GET /api/analysis/announcement-correlations/<type>/<horizon>` — per-cell detail across price_sensitive values + sample events for drill-down

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
- **Splits/consolidations**: `fetch_splits.py` → Yahoo Finance (run periodically; re-downloads adjusted history on new event)

### Database schema
See `Database.md` for full schema. Summary:
- `symbols(symbol PK, name, industry, shares, current)` — `current=0` for delisted/renamed
- `shorts(symbol, date, short%)` — daily short positions 2010–present
- `endofday(symbol, date, open, high, low, close, volume)` — daily OHLCV
- `endofmonth(symbol, date, close)` — last trading day of each month
- `corporate_events(symbol, date PK, event_type, ratio, description)` — splits/consolidations

### Market cap
Computed live: `shares × latest close from endofday`. No stale snapshot.

## Analysis Framework (`analysis/`)

GPU-accelerated signal research pipeline. Runs on a local GPU machine; results rsynced to the server for the API to serve.

### Workflow
```bash
# Set ASX_SERVER=user@your-server in .env first
./analysis/sync.sh              # pull DB from server → run predictions → push results (~2.5 min)
./analysis/sync.sh --skip-pull  # skip DB download (use cached local DB)
```

### Signals (three implemented, two placeholders)
- **ShortTrendSignal** — negated 20d slope of short%, cross-sectionally ranked
- **ShortSqueezeSignal** — top-decile short% AND positive 5d momentum
- **VolumeAnomalySignal** — log-volume z-score × sign(5d return)
- CommodityLeadSignal, AnnouncementSignal — placeholders

### EOFY tax-loss/gain correlation (`analysis/eofy_correlation/`)
Per-stock (not cross-sectional) test: does a symbol's own Jul-Mar (Q1-3) return
correlate with its own Apr-Jun (Q4) return, across every FY in its history?
Plain CPU pipeline (pandas/scipy Pearson r + BH-FDR) — deliberately does not
import `analysis.core` (avoids the torch dependency pulled in by
`feature_matrix`/`gpu_ops`), so it runs fine on machines without a GPU/torch.
Guards: excludes any FY with a `corporate_events` split/consolidation inside
its window, and any single-quarter return with `abs(return) > 300%` (likely
an un-adjusted-split artifact). Results in `analysis/results/eofy_correlation.db`.

`analysis/eofy_correlation/window_pipeline.py` tests three windows against
the same Q1-3 return, for every eligible current symbol (not a pre-selected
subset): Late May (day57-70) and Rest of Q4 (day71-91, to Jun 30) — both
found by an earlier investigative weekly breakdown (13 weeks of Q4, pooled
over the top-50 |r| stocks) that showed the Full-Quarter effect concentrated
in two adjacent weeks rather than spread evenly — plus New FY (window 'C',
Jul 1 - Aug 11 of the FY that follows), added after a follow-up weekly
breakdown extended past Jun 30 and found the in-quarter effect does not
carry over (only weak, scattered, non-FDR-surviving weekly hits). Confirmed
at full-universe scale: window C has 0 FDR-significant symbols vs 21 (A) /
9 (B). Window C differs structurally from A/B — it needs the *next* FY's
data too, so its per-symbol `n_years` is a subset of A/B's eligible years
for the same symbol (recently-listed or current-in-progress FYs may lack
it). Same exclusion guards as above otherwise; results in the same DB
(`eofy_window_correlation` / `eofy_window_definitions` tables, `window`
column value `'C'`). Run via `run_eofy_window_compare`, below.

### Announcement-type correlation (`analysis/announcement_correlation/`, experimental)
Does an announcement's *type* (trading halt, placement, dividend, director
dealing, etc.) predict price direction/magnitude over the following 1/5/20/60
trading days? Unlike `eofy_correlation`, this DOES import `analysis.core`
(FeatureMatrix/DataLoader) — it wants GPU tensor vectorization, computing
forward returns for every symbol/date/horizon once, then gathering at each
announcement's specific (symbol, date) cell, rather than looping per-event
SQL ~80k times. Headline-space-only (`.gitignore`d) `announcements.db` copy
pulled from the `asx-announcements` sibling repo by `sync.sh`, to
`analysis/data/announcements.db` — not checked out as a full repo on realiti.

Type resolution (`types.py`): the 15 structured `extracted_*` tables in
`announcements.db` (joined on `ids_id` alone — a few ids legitimately span
two tickers, both legs get the same type) take precedence over a small
headline-regex classifier (`classify.py`) covering types `extract_structured.py`
doesn't have yet (`trading_halt`, `placement`, `rights_issue`,
`capital_raising`, `takeover_scheme`) — kept local to this module rather than
extended into the production scraper, since these are experimental and fully
identifiable from headline text alone. Everything else buckets to `'other'`
(excluded from results, just counted).

Alignment (`alignment.py`) is the one real correctness trap: `FeatureMatrix.mask`
fails on a halted stock's own halt day (`close` non-NaN & `volume>0`), so a
trading-halt announcement's nominal day is usually itself invalid. Separates
"nominal day" (date-string match, rolling to next trading day if announced
at/after 4pm or on a non-trading day) from "first valid base price"
(scans forward up to 10 trading days). `t0_lag_days` and a same-day
`event_day_reaction` (distinct from the forward-horizon returns) are both
kept per-event specifically to make this visible for halts.

`endofday.close` is already split-adjusted at the source (see
`fetch_splits.py` above) — `corporate_events` is used purely as an exclusion
guard here too (drop, don't ratio-adjust), same as `eofy_correlation`.
Excess return = raw return minus XAO's own forward return over the same
window (XAO is a regular `endofday` symbol). BH-FDR applied only within the
pooled (`price_sensitive='both'`) cells — the PS-split cells are reported
but not included in that correction, to avoid both underpowering and
over-correcting the already-thin rare-type cells. Results in
`analysis/results/announcement_correlation.db`. Run via
`run_announcement_correlation`, below.

### CLI scripts (run from repo root)
```bash
python -m analysis.cli.run_predictions --db stockdb/stockdb.db      # current signal scores
python -m analysis.cli.run_signals --db stockdb/stockdb.db          # signals on training data
python -m analysis.cli.run_backtest --db stockdb/stockdb.db         # backtest all signals
python -m analysis.cli.run_discovery --db stockdb/stockdb.db        # IC sweep (slow, ~30 min)
python -m analysis.cli.run_portfolio_backtest --db stockdb/stockdb.db  # portfolio backtest
python -m analysis.cli.run_eofy_correlation --db stockdb/stockdb.db --output-dir analysis/results  # EOFY correlation
python -m analysis.cli.run_eofy_window_compare --db stockdb/stockdb.db --eofy-db analysis/results/eofy_correlation.db  # EOFY sub-window (Late May / Rest of Q4)
python -m analysis.cli.run_announcement_correlation --db stockdb/stockdb.db --ann-db ../asx-announcements/announcements.db --output-dir analysis/results  # Announcement-type correlation
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

### Cron — predictions refresh
- Commented entry in `asx/crontab`: weekdays at 20:00 UTC after EOD fetch
- Enable by uncommenting and running `./analysis/sync.sh` from local machine instead
