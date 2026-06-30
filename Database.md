# Databases

The project uses three SQLite databases, each with a distinct concern.

---

## 1. `stockdb/stockdb.db` — Market data (~934 MB)

Read-only from the web frontend. Built and maintained by the data pipeline on the backend server.

### `symbols`
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT PRIMARY KEY` | ASX ticker (e.g. `BHP`) |
| `name` | `TEXT` | Company name |
| `industry` | `TEXT` | Industry group |
| `shares` | `REAL` | Shares outstanding, derived from ListCorp snapshot: `mcap / last_trade_price` |
| `current` | `INTEGER NOT NULL DEFAULT 1` | 1 = active listing, 0 = delisted/renamed |

### `shorts`
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT` | ASX ticker |
| `date` | `DATETIME` | Unix timestamp |
| `short` | `REAL` | Short position as a percentage of issued capital |

Indexes:
- `idx_shorts_symbol_date` on `(symbol, date)`
- `idx_shorts_3char_peak` on `(symbol, short DESC) WHERE length(symbol) = 3` — partial index for peak-shorts queries

### `endofday`
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT` | ASX ticker |
| `date` | `DATETIME` | Unix timestamp |
| `open` | `REAL` | |
| `high` | `REAL` | |
| `low` | `REAL` | |
| `close` | `REAL` | |
| `volume` | `INT` | |

Index: `idx_endofday_symbol_date` on `(symbol, date)`

### `endofmonth`
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT` | ASX ticker |
| `date` | `DATETIME` | Unix timestamp (last trading day of each month) |
| `close` | `REAL` | |

Subset of `endofday` — only the last trading day of each calendar month.
Used for efficient computation of 1m, 3m, 6m, 1y, 3y, 5y returns without scanning all daily rows.

### `corporate_events`
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `date` | `INTEGER NOT NULL` | Unix timestamp of the event |
| `event_type` | `TEXT NOT NULL` | `'split'` or `'consolidation'` |
| `ratio` | `REAL NOT NULL` | Split ratio (e.g. 2.0 for 2:1 split, 0.5 for 1:2 consolidation) |
| `description` | `TEXT` | Human-readable (e.g. `"2:1 Split"`, `"1:4 Consolidation"`) |
| PRIMARY KEY | `(symbol, date)` | |

Populated by `stockdb/fetch_splits.py` using Yahoo Finance.
When a new split is detected, the full adjusted OHLCV history for that symbol is re-downloaded.

### `fundamentals`
Fundamental and analyst data from Yahoo Finance. One row per symbol (upserted weekly). Fetched by `asx-data/scripts/fetch_fundamentals.py` every Friday evening.

| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT PRIMARY KEY` | ASX ticker |
| `fetched_at` | `TEXT` | ISO datetime of last fetch |
| `market_cap` | `REAL` | |
| `enterprise_value` | `REAL` | |
| `trailing_pe` | `REAL` | |
| `forward_pe` | `REAL` | |
| `price_to_book` | `REAL` | |
| `price_to_sales` | `REAL` | Trailing 12 months |
| `enterprise_to_revenue` | `REAL` | EV/Revenue |
| `enterprise_to_ebitda` | `REAL` | EV/EBITDA |
| `profit_margins` | `REAL` | Net margin |
| `operating_margins` | `REAL` | |
| `gross_margins` | `REAL` | |
| `ebitda_margins` | `REAL` | |
| `return_on_assets` | `REAL` | ROA |
| `return_on_equity` | `REAL` | ROE |
| `revenue_growth` | `REAL` | YoY |
| `earnings_growth` | `REAL` | YoY |
| `total_revenue` | `REAL` | |
| `ebitda` | `REAL` | |
| `net_income` | `REAL` | |
| `free_cashflow` | `REAL` | |
| `operating_cashflow` | `REAL` | |
| `total_cash` | `REAL` | |
| `total_debt` | `REAL` | |
| `debt_to_equity` | `REAL` | |
| `current_ratio` | `REAL` | |
| `quick_ratio` | `REAL` | |
| `eps_trailing` | `REAL` | |
| `eps_forward` | `REAL` | |
| `dividend_yield` | `REAL` | As a percentage |
| `dividend_rate` | `REAL` | Annual dividend per share |
| `payout_ratio` | `REAL` | |
| `five_year_avg_div_yield` | `REAL` | |
| `ex_dividend_date` | `INTEGER` | Unix timestamp |
| `last_dividend_value` | `REAL` | |
| `recommendation_mean` | `REAL` | 1=Strong Buy … 5=Strong Sell |
| `recommendation_key` | `TEXT` | e.g. `'buy'`, `'hold'`, `'sell'` |
| `analyst_count` | `INTEGER` | Number of analyst opinions |
| `target_mean_price` | `REAL` | Consensus 12-month price target |
| `target_high_price` | `REAL` | |
| `target_low_price` | `REAL` | |
| `target_median_price` | `REAL` | |
| `beta` | `REAL` | 5-year monthly vs S&P 500 |
| `week52_change` | `REAL` | 52-week price change as a fraction |
| `shares_outstanding` | `REAL` | |
| `float_shares` | `REAL` | |
| `held_pct_insiders` | `REAL` | Fraction held by insiders |
| `held_pct_institutions` | `REAL` | Fraction held by institutions |

Many fields will be NULL for micro-caps and ETFs where Yahoo Finance has limited coverage.

### `dividends`
Historical per-share dividend payments from Yahoo Finance. Fetched by `asx-data/scripts/fetch_dividends.py` monthly.

| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `ex_date` | `INTEGER NOT NULL` | Unix timestamp of ex-dividend date |
| `amount` | `REAL NOT NULL` | Per-share dividend amount in AUD |
| `currency` | `TEXT NOT NULL DEFAULT 'AUD'` | Currency (always AUD for ASX stocks) |
| PRIMARY KEY | `(symbol, ex_date)` | |

Index: `idx_dividends_symbol` on `(symbol)`

API endpoint: `GET /api/dividends/<symbol>` — returns `ex_date` in milliseconds for JS/Plotly.

Note: franking percentage is not available from Yahoo Finance; would require scraping ASX.com.au.

### `shares_history`
Year-end shares-outstanding snapshots derived from annual reports. One row per (symbol, year).

| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `year` | `INTEGER NOT NULL` | Calendar year (year-end snapshot) |
| `date` | `TEXT NOT NULL` | YYYY-MM-DD of the actual last data point |
| `shares` | `INTEGER NOT NULL` | Shares outstanding |
| `fetched_at` | `TEXT NOT NULL` | ISO datetime of fetch |
| PRIMARY KEY | `(symbol, year)` | |

### `events`
Corporate calendar events from Yahoo Finance (ex-dividend dates, earnings dates, etc.).

| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `event_date` | `INTEGER NOT NULL` | Unix timestamp |
| `end_date` | `INTEGER` | Unix timestamp (for multi-day events) |
| `event_type` | `TEXT NOT NULL` | e.g. `'Dividends'`, `'Earnings Date'` |
| `title` | `TEXT NOT NULL` | Display label |
| `description` | `TEXT` | Additional detail |
| `is_estimate` | `INTEGER NOT NULL DEFAULT 0` | 1 if date is estimated |
| `source` | `TEXT NOT NULL DEFAULT 'yfinance'` | Data source |
| `fetched_at` | `TEXT NOT NULL` | ISO datetime of fetch |
| UNIQUE | `(symbol, event_date, event_type)` | |

### `financials_annual`
Annual income statement, cash flow, and balance sheet from Yahoo Finance. One row per (symbol, fiscal_year_end).

| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `fiscal_year_end` | `TEXT NOT NULL` | YYYY-MM-DD |
| `fetched_at` | `TEXT NOT NULL` | ISO datetime of fetch |
| `total_revenue` | `REAL` | |
| `gross_profit` | `REAL` | |
| `operating_income` | `REAL` | |
| `net_income` | `REAL` | |
| `ebitda` | `REAL` | |
| `basic_eps` | `REAL` | |
| `interest_expense` | `REAL` | |
| `tax_provision` | `REAL` | |
| `operating_cashflow` | `REAL` | |
| `free_cashflow` | `REAL` | |
| `capital_expenditure` | `REAL` | |
| `dividends_paid` | `REAL` | |
| `total_assets` | `REAL` | |
| `total_debt` | `REAL` | |
| `stockholders_equity` | `REAL` | |
| `cash` | `REAL` | |
| `total_liabilities` | `REAL` | |
| PRIMARY KEY | `(symbol, fiscal_year_end)` | |

### `commodity_meta`
Reference table for tracked commodities.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT PRIMARY KEY` | e.g. `'GOLD'`, `'COPPER'` |
| `name` | `TEXT NOT NULL` | Display name |
| `unit` | `TEXT` | Price unit, e.g. `'USD/troy oz'` |
| `te_symbol` | `TEXT` | Trading Economics symbol |
| `yf_symbol` | `TEXT` | Yahoo Finance symbol, e.g. `'GC=F'` |
| `metals_dev_key` | `TEXT` | metals.dev API key name |
| `te_no_access` | `INTEGER NOT NULL DEFAULT 0` | 1 if TE blocks this commodity |

### `commodity_prices`
Daily commodity price history.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT NOT NULL` | FK → `commodity_meta.id` |
| `date` | `INTEGER NOT NULL` | Unix timestamp |
| `price` | `REAL NOT NULL` | In commodity-specific units (see `commodity_meta.unit`) |
| PRIMARY KEY | `(id, date)` | |

Populated by `scripts/fetch_commodities.py`, `fetch_trading_economics.py`, `fetch_metals_dev.py`, `fetch_manganese.py`.

### `crypto_meta`
Current metadata and price for tracked cryptocurrencies.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT PRIMARY KEY` | CoinGecko ID, e.g. `'bitcoin'` |
| `name` | `TEXT NOT NULL` | Display name |
| `cg_id` | `TEXT` | CoinGecko ID (same as `id`) |
| `yf_symbol` | `TEXT` | Yahoo Finance symbol, e.g. `'BTC-USD'` |
| `rank` | `INTEGER` | CoinGecko market cap rank |
| `price` | `REAL` | Current price (USD) |
| `change_pct_24h` | `REAL` | 24-hour price change % |
| `market_cap` | `REAL` | Market cap (USD) |
| `volume_24h` | `REAL` | 24-hour trading volume (USD) |
| `updated_at` | `TEXT` | ISO datetime of last update |

### `crypto_prices`
Daily OHLCV history for cryptocurrencies.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT NOT NULL` | FK → `crypto_meta.id` |
| `date` | `INTEGER NOT NULL` | Unix timestamp |
| `open` | `REAL` | |
| `high` | `REAL` | |
| `low` | `REAL` | |
| `close` | `REAL NOT NULL` | |
| `volume` | `REAL` | |
| PRIMARY KEY | `(id, date)` | |

Populated by `scripts/fetch_crypto.py` (daily). Top 100 by CoinGecko market cap rank.

### `currency_meta`
Current metadata and rate for tracked FX pairs.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT PRIMARY KEY` | e.g. `'AUDUSD'` |
| `base` | `TEXT NOT NULL` | Base currency, e.g. `'AUD'` |
| `quote` | `TEXT NOT NULL` | Quote currency, e.g. `'USD'` |
| `yf_symbol` | `TEXT NOT NULL` | Yahoo Finance ticker, e.g. `'AUDUSD=X'` |
| `group_name` | `TEXT` | `'AUD Pairs'` or `'Majors'` |
| `price` | `REAL` | Latest rate |
| `change_pct_24h` | `REAL` | 24-hour change % |
| `updated_at` | `TEXT` | ISO datetime of last update |

### `currency_prices`
Daily close history for FX pairs.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT NOT NULL` | FK → `currency_meta.id` |
| `date` | `INTEGER NOT NULL` | Unix timestamp |
| `close` | `REAL NOT NULL` | Daily close rate |
| PRIMARY KEY | `(id, date)` | |

Populated by `scripts/fetch_currencies.py` (daily 21:35 UTC). 14 pairs: 8 AUD-centric + 6 major crosses.
API: `GET /api/currencies`, `GET /api/currencies/<id>`.

### `kronos_predictions`
ML forward-return predictions from the Kronos fine-tuned model. Each run generates one row per symbol.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `generated_at` | `TEXT NOT NULL` | ISO datetime of prediction run |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `score` | `REAL NOT NULL` | Predicted 5-day forward return score |
| `date` | `INTEGER NOT NULL` | Unix timestamp of last data point used |
| `name` | `TEXT` | Company name at time of prediction |
| `industry` | `TEXT` | Industry at time of prediction |

Populated by GPU pipeline on realiti; results rsync'd to harri. Indexed on `generated_at` and `symbol`.
API: `GET /api/kronos/latest`, `GET /api/kronos/history`.

### `eod_fetch_failures`
Tracks consecutive end-of-day fetch failures per symbol (used to skip stale/delisted symbols).

| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT PRIMARY KEY` | ASX ticker |
| `consecutive_misses` | `INTEGER NOT NULL DEFAULT 0` | |
| `first_miss_date` | `TEXT NOT NULL` | YYYY-MM-DD |
| `last_miss_date` | `TEXT NOT NULL` | YYYY-MM-DD |

### Market cap
Computed live at query time: `symbols.shares × latest close from endofday`. No stale snapshot date.

### Data sources
- Symbols + shares: `fetch_symbols.py` → `symbols/asx-official.csv`; shares derived from `symbols/ASXListedCompanies-YYYYMMDD.csv`
- Shorts: `fetch_shorts.py` → `shorts/YYYY.csv` (ASIC public CSVs, 2010–present)
- OHLCV prices: purchased from eoddata.com; zip files in `asx-eod-data/zips/` (private submodule)
- Splits/consolidations: `fetch_splits.py` → Yahoo Finance (run periodically)
- Commodities: `fetch_commodities.py` (yfinance), `fetch_trading_economics.py`, `fetch_metals_dev.py`, `fetch_manganese.py`
- Crypto: `fetch_crypto.py` (CoinGecko + yfinance, daily)
- Currencies: `fetch_currencies.py` (yfinance, daily)
- Fundamentals: `fetch_fundamentals.py` (yfinance, weekly)
- Financials: `fetch_financials.py` (yfinance, as needed)
- Dividends: `fetch_dividends.py` (yfinance, monthly)
- Kronos predictions: GPU pipeline on realiti, rsync'd to harri

---

## 2. `asx-web/users.db` — Users, watchlists, portfolios & research (~45 KB)

Read-write from the web frontend. Contains all user auth and user-specific data.

### `users`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `email` | `TEXT NOT NULL UNIQUE COLLATE NOCASE` | Login identifier |
| `pw_hash` | `TEXT` | NULL for first-login users (triggers `/set-password` flow) |
| `enabled` | `INTEGER NOT NULL DEFAULT 1` | 0 = account disabled |
| `is_admin` | `INTEGER NOT NULL DEFAULT 0` | 1 = admin; can manage users and toggle features |
| `show_experimental` | `INTEGER NOT NULL DEFAULT 0` | 1 = Fermi analysis enabled for this user |
| `reset_hour` | `INTEGER NOT NULL DEFAULT 10` | Hour (AEST) at which yesterday's announcement icons reset |
| `default_page` | `TEXT NOT NULL DEFAULT '/'` | Landing page after login |
| `show_premarket_indicative` | `INTEGER NOT NULL DEFAULT 0` | Show pre-market indicative prices on stock page |
| `alert_limit` | `INTEGER NOT NULL DEFAULT 20` | Max alerts this user can create |
| `table_font_size` | `TEXT` | Font size for watchlist/portfolio tables |
| `table_font_dense` | `TEXT` | Dense font size for tables |

The admin user (configured via `ADMIN_EMAIL` env var) is seeded on startup, always enabled, non-deletable.

### `list_groups`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `type` | `TEXT NOT NULL` | `'watchlist'` or `'portfolio'` |
| `name` | `TEXT NOT NULL` | |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Sidebar sort order |
| `kind` | `TEXT NOT NULL DEFAULT 'standard'` | Group kind (e.g. `'standard'`, `'algorithm'`) |

### `lists`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `type` | `TEXT NOT NULL` | `'watchlist'` or `'portfolio'` |
| `group_id` | `INTEGER` | FK → `list_groups(id) ON DELETE SET NULL` |
| `name` | `TEXT NOT NULL` | |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Sidebar sort order within group |
| `algorithm_id` | `INTEGER` | FK → `algorithms(id) ON DELETE SET NULL` — links list to an algorithm |

### `watchlist_items`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `list_id` | `INTEGER NOT NULL` | FK → `lists(id) ON DELETE CASCADE` |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Display order |
| `notes` | `TEXT` | |
| UNIQUE | `(list_id, symbol)` | No duplicate symbols per list |

### `portfolio_items`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `list_id` | `INTEGER NOT NULL` | FK → `lists(id) ON DELETE CASCADE` |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `quantity` | `REAL NOT NULL` | Number of shares held |
| `purchase_price` | `REAL NOT NULL` | Cost per share |
| `purchase_date` | `TEXT` | YYYY-MM-DD |
| `notes` | `TEXT` | |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Display order |
| `attachment_data` | `BLOB` | Contract note PDF bytes |
| `attachment_name` | `TEXT` | Original filename of the attached contract note |

Multiple rows per symbol allowed (different tranches at different prices).

### `list_column_prefs`
| Column | Type | Notes |
|--------|------|-------|
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `type` | `TEXT NOT NULL` | `'watchlist'` or `'portfolio'` |
| `columns` | `TEXT NOT NULL` | JSON array of `{key, label, visible}` objects |
| PRIMARY KEY | `(user_id, type)` | One row per user per list type |

### `transactions`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `list_id` | `INTEGER` | FK → `lists(id) ON DELETE SET NULL` — portfolio the trade applies to |
| `portfolio_item_id` | `INTEGER` | FK to portfolio_items row (if applied) |
| `type` | `TEXT NOT NULL` | `'BUY'` or `'SELL'` |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `quantity` | `REAL NOT NULL` | |
| `price` | `REAL NOT NULL` | Per-share price |
| `trade_date` | `TEXT` | YYYY-MM-DD |
| `brokerage` | `REAL` | Brokerage fee |
| `broker` | `TEXT` | Broker name (e.g. `'CMC'`) |
| `pdf_data` | `BLOB` | Original contract note PDF bytes |
| `applied` | `INTEGER NOT NULL DEFAULT 0` | 1 = trade applied to portfolio |
| `created_at` | `INTEGER NOT NULL DEFAULT (strftime('%s','now'))` | Unix timestamp |

Contract notes are parsed by the CMC/generic parser in `asx.py` and stored here before being applied to a portfolio.

### `algorithms`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `name` | `TEXT NOT NULL` | |
| `description` | `TEXT` | |
| `shared` | `INTEGER NOT NULL DEFAULT 0` | 1 = visible to all users |
| `created_at` | `TEXT DEFAULT (datetime('now'))` | |
| `version` | `INTEGER NOT NULL DEFAULT 1` | Incremented on each edit |
| `source_id` | `INTEGER` | FK → `algorithms(id) ON DELETE SET NULL` — forked from |
| `source_version` | `INTEGER` | Version of source algorithm when forked |
| `code` | `TEXT` | Python source code of the algorithm |

### `recommendations`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `algorithm_id` | `INTEGER NOT NULL` | FK → `algorithms(id) ON DELETE CASCADE` |
| `symbol` | `TEXT NOT NULL` | ASX ticker |
| `signal` | `TEXT NOT NULL` | Signal label (e.g. `'BUY'`) |
| `signal_date` | `TEXT NOT NULL` | YYYY-MM-DD |
| `period_days` | `INTEGER NOT NULL DEFAULT 20` | Lookback/signal period |
| `expires_date` | `TEXT NOT NULL` | YYYY-MM-DD — when the recommendation expires |
| `score` | `REAL` | Confidence/strength score |
| `computed_at` | `TEXT NOT NULL DEFAULT (datetime('now'))` | |
| UNIQUE | `(algorithm_id, symbol, signal_date)` | |

### `alerts`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `name` | `TEXT NOT NULL` | Display name |
| `symbol` | `TEXT` | ASX ticker (NULL for portfolio-wide alerts) |
| `enabled` | `INTEGER NOT NULL DEFAULT 1` | 0 = paused |
| `portfolio_ids` | `TEXT` | JSON array of list IDs to monitor (alternative to symbol) |
| `baseline_price` | `REAL` | Reference price at time of creation |
| `notes` | `TEXT` | User notes |
| `created_at` | `TEXT` | |
| `updated_at` | `TEXT` | |

### `alert_conditions`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `alert_id` | `INTEGER NOT NULL` | FK → `alerts(id) ON DELETE CASCADE` |
| `condition_type` | `TEXT NOT NULL` | e.g. `'price_above'`, `'price_below'` |
| `threshold_value` | `REAL NOT NULL` | Trigger level |
| `direction` | `TEXT` | Direction qualifier if applicable |
| `last_triggered` | `TEXT` | ISO datetime of last trigger |
| `created_at` | `TEXT` | |

### `fermi_reports`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT PRIMARY KEY` | UUID |
| `research_report_id` | `INTEGER` | FK → `research_reports(id) ON DELETE CASCADE` (NULL for standalone) |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `symbol` | `TEXT` | ASX ticker the analysis targets |
| `model` | `TEXT NOT NULL` | Claude model used (e.g. `claude-haiku-4-5`) |
| `status` | `TEXT NOT NULL DEFAULT 'queued'` | `queued`, `running`, `completed`, `failed` |
| `progress_pct` | `INTEGER DEFAULT 0` | 0–100 |
| `error_msg` | `TEXT` | Error detail if status = failed |
| `output_path` | `TEXT` | Local path to completed report PDF |
| `api_cost_usd` | `REAL DEFAULT 0` | Total API spend for this report |
| `created_at` | `TEXT NOT NULL` | |
| `started_at` | `TEXT` | |
| `completed_at` | `TEXT` | |
| `research_data` | `BLOB` | Serialised research context used for generation |

### `fermi_api_calls`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `fermi_report_id` | `TEXT NOT NULL` | FK → `fermi_reports(id) ON DELETE CASCADE` |
| `call_type` | `TEXT` | Phase label (e.g. `'extraction'`, `'analysis'`) |
| `model` | `TEXT` | Model used for this call |
| `tokens_in` | `INTEGER` | Input tokens |
| `tokens_out` | `INTEGER` | Output tokens |
| `cost_usd` | `REAL` | Cost of this call |
| `timestamp` | `TEXT` | |

### `dashboard_preferences`
| Column | Type | Notes |
|--------|------|-------|
| `user_id` | `INTEGER NOT NULL PRIMARY KEY` | FK → `users(id) ON DELETE CASCADE` |
| `ann_price_sensitive_only` | `INTEGER NOT NULL DEFAULT 1` | Announcement feed: price-sensitive only |
| `ann_scope` | `TEXT NOT NULL DEFAULT 'both'` | `'watchlist'`, `'portfolio'`, or `'both'` |
| `events_scope` | `TEXT NOT NULL DEFAULT 'both'` | `'watchlist'`, `'portfolio'`, or `'both'` |
| `commodities` | `TEXT NOT NULL` | JSON array of pinned commodity IDs |
| `widget_order` | `TEXT NOT NULL` | JSON array of widget IDs in display order |
| `panels_config` | `TEXT NOT NULL DEFAULT '[]'` | JSON array of custom panel configs |
| `screener_saved` | `TEXT NOT NULL DEFAULT '[]'` | JSON array of saved screener filters |
| `options_saved` | `TEXT NOT NULL DEFAULT '[]'` | JSON array of saved warrant filter configs |

### `user_feature_changes`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `admin_user_id` | `INTEGER NOT NULL` | FK → `users(id)` — admin who made the change |
| `target_user_id` | `INTEGER NOT NULL` | FK → `users(id)` — user affected |
| `feature` | `TEXT NOT NULL` | Feature flag name (e.g. `'show_experimental'`) |
| `old_value` | `INTEGER` | Previous value |
| `new_value` | `INTEGER` | New value |
| `timestamp` | `TEXT NOT NULL` | |

Audit log for admin-toggled per-user feature flags.

### `asx_options`
| Column | Type | Notes |
|--------|------|-------|
| `option_symbol` | `TEXT PRIMARY KEY` | ASX warrant code |
| `expiry` | `TEXT NOT NULL` | Expiry date (YYYY-MM-DD) |
| `exercise` | `REAL NOT NULL` | Exercise/strike price |
| `share_symbol` | `TEXT NOT NULL` | Underlying share ticker |
| `share_name` | `TEXT NOT NULL` | Underlying company name |
| `note` | `TEXT` | Additional info (e.g. `'C'` for call) |
| `fetched_at` | `TEXT NOT NULL DEFAULT (datetime('now'))` | When last refreshed |

Populated by `asx-data/scripts/fetch_options_ib.py` (weekly from IB Gateway) and `fetch_options_eod.py` (daily EOD prices into `endofday`). EOD prices joined at query time from `endofday` on `option_symbol`.

### `symbol_changes`
| Column | Type | Notes |
|--------|------|-------|
| `old_symbol` | `TEXT NOT NULL` | Previous ASX ticker |
| `new_symbol` | `TEXT NOT NULL` | New ASX ticker |
| `effective_date` | `TEXT NOT NULL` | YYYY-MM-DD |
| PRIMARY KEY | `(old_symbol, new_symbol, effective_date)` | |

Populated by `asx-data/scripts/fetch_symbol_changes.py`. Displayed on the stock chart page.

### `research_reports`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id)` |
| `symbol` | `TEXT` | ASX ticker (NULL if not extracted) |
| `title` | `TEXT` | Display title (defaults to filename) |
| `file_name` | `TEXT` | Original uploaded filename |
| `file_type` | `TEXT` | `'pdf'` or `'docx'` |
| `file_data` | `BLOB` | Raw file bytes |
| `extracted_text` | `TEXT` | Full plain text extracted from the document |
| `bull_low` | `REAL` | Bull case lower bound price target |
| `bull_high` | `REAL` | Bull case upper bound price target |
| `base_low` | `REAL` | Base case lower bound price target |
| `base_high` | `REAL` | Base case upper bound price target |
| `bear_low` | `REAL` | Bear case lower bound price target |
| `bear_high` | `REAL` | Bear case upper bound price target |
| `bull_prob` | `REAL` | Bull case probability (0–1) |
| `base_prob` | `REAL` | Base case probability (0–1) |
| `bear_prob` | `REAL` | Bear case probability (0–1) |
| `report_date` | `TEXT` | Report publication date (YYYY-MM-DD), extracted from document header |
| `folder_id` | `INTEGER` | FK → `research_folders(id) ON DELETE SET NULL` |
| `notes` | `TEXT` | User notes |
| `is_public` | `INTEGER NOT NULL DEFAULT 0` | 0 = private, 1 = public (visible to all users) |
| `uploaded_at` | `INTEGER NOT NULL` | Unix timestamp of upload |

AI extraction uses Claude Haiku via `ANTHROPIC_API_KEY`. Expected Value = Σ(prob_i × midpoint_i).

### `research_folders`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `name` | `TEXT NOT NULL` | |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Sort order |
| `parent_id` | `INTEGER` | FK → `research_folders(id) ON DELETE SET NULL` — for nested folders |

---

## 3. `asx-announcements/announcements.db` — ASX announcements

Managed by the `asx-announcements` repo (private). Accessed via the announcements server.
The main web app proxies announcement requests to the announcements server via HTTP.

### `announcements`
| Column | Type | Notes |
|--------|------|-------|
| `ids_id` | `TEXT PRIMARY KEY` | ASX IDS (announcement) identifier |
| `ticker` | `TEXT NOT NULL` | ASX ticker |
| `headline` | `TEXT` | Announcement headline |
| `announced_at` | `TEXT` | ISO 8601 datetime |
| `price_sensitive` | `INTEGER DEFAULT 0` | 1 if price-sensitive |
| `page_count` | `INTEGER` | PDF page count |
| `file_size_kb` | `REAL` | |
| `pdf_url` | `TEXT` | Source URL on ASX website |
| `pdf_path` | `TEXT` | Local path to downloaded PDF |
| `file_size_bytes` | `INTEGER` | |
| `downloaded_at` | `TEXT` | ISO 8601 datetime of download |

PDFs stored at `asx-announcements/pdfs/YYYY/YYYY-MM/YYYY-MM-DD/<ids_id>.pdf`.

---

