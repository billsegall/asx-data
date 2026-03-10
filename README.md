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
