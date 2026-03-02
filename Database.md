# Databases

The project uses three SQLite databases, each with a distinct concern.

---

## 1. `stockdb/stockdb.db` — Market data (934 MB)

Read-only from the web frontend. Built and maintained by the data pipeline on the backend server.

### `symbols`
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `TEXT PRIMARY KEY` | ASX ticker (e.g. `BHP`) |
| `name` | `TEXT` | Company name |
| `industry` | `TEXT` | Industry group |
| `shares` | `REAL` | Shares outstanding, derived from ListCorp snapshot: `mcap / last_trade_price` |

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

### Market cap
Computed live at query time: `symbols.shares × latest close from endofday`. No stale snapshot date.

### Data sources
- Symbols + shares: `fetch_symbols.py` → `symbols/asx-official.csv`; shares derived from `symbols/ASXListedCompanies-YYYYMMDD.csv`
- Shorts: `fetch_shorts.py` → `shorts/YYYY.csv` (ASIC public CSVs, 2010–present)
- OHLCV prices: purchased from eoddata.com; zip files in `asx-eod-data/zips/` (private submodule)

---

## 2. `stockdb/users.db` — Users, watchlists & portfolios (45 KB)

Read-write from the web frontend. Contains all user auth and list data.
Conceptually belongs to the frontend; will migrate to `userdata/users.db` as part of backend/frontend split.

### `users`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `email` | `TEXT NOT NULL UNIQUE COLLATE NOCASE` | Login identifier |
| `pw_hash` | `TEXT` | NULL for first-login users (triggers `/set-password` flow) |
| `enabled` | `INTEGER NOT NULL DEFAULT 1` | 0 = account disabled |

`admin@segall.net` is seeded on startup, always enabled, non-deletable.

### `list_groups`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `type` | `TEXT NOT NULL` | `'watchlist'` or `'portfolio'` |
| `name` | `TEXT NOT NULL` | |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Sidebar sort order |

### `lists`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `type` | `TEXT NOT NULL` | `'watchlist'` or `'portfolio'` |
| `group_id` | `INTEGER` | FK → `list_groups(id) ON DELETE SET NULL` |
| `name` | `TEXT NOT NULL` | |
| `position` | `INTEGER NOT NULL DEFAULT 0` | Sidebar sort order within group |

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

Multiple rows per symbol allowed (different tranches at different prices).

### `list_column_prefs`
| Column | Type | Notes |
|--------|------|-------|
| `user_id` | `INTEGER NOT NULL` | FK → `users(id) ON DELETE CASCADE` |
| `type` | `TEXT NOT NULL` | `'watchlist'` or `'portfolio'` |
| `columns` | `TEXT NOT NULL` | JSON array of `{key, label, visible}` objects |
| PRIMARY KEY | `(user_id, type)` | One row per user per list type |

---

## 3. `asx-announcements/announcements.db` — ASX announcements

Managed by the `asx-announcements` submodule (private repo). Lives on `harri.tailb1cff.ts.net`.
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
