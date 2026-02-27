# asx-data
ASX data and processing

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

## The database

The database consists of three tables described below by example:

### symbols
symbol | name | industry | shares
------ | ---- | --- | ------
BHP    | BHP GROUP LIMITED | Materials | 5,102,905,054

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

# Web application

A Flask web app in `www/` visualises the data with a dark theme (Tailwind CSS)
and interactive charts (Plotly.js).

## Running

```bash
cd www && ./asx        # local
docker compose up      # Docker (mounts DB as read-only volume)
```

## Features

- **Stock page** (`/stock/<symbol>`) — interactive candlestick/line chart with
  XAO overlay, short interest on a secondary axis, volume subplot, and range
  selector buttons (1M / 3M / 6M / 1Y / 3Y / 5Y / 10Y / All). Defaults to a
  1Y viewport. Supports normalised comparison with a second symbol via the
  "vs." input.
- **Shorts** (`/shorts`) — latest short positions, sortable, filterable by symbol or company name
- **Market cap** — computed live from shares outstanding × latest close price
