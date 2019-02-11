# asx-shorts
ASX short lists processing

## Short data

The **raw** data files _data/shorts/RR*.csv_ were obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/), and
contains some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were processed by hand or consistency of name and character encoding to _data/shorts/20YY.csv_ using LibreOffice.

From there, _shorts2sqlite.py_ loads them into the database (by default at _data/stocks.db_).

## Stock price data

The **raw** historical ASX stock market data in _data/prices/raw/*.txt_ was obtained from the
[ASX Historical Data](https://www.asxhistoricaldata.com/archive/) and flattened and renamed for consistency. These
were then simply concatenated into _data/prices/prices.csv_.

From there, _prices2sqlite.py_ loads them into the database (by default at _data/stocks.db_).


## The database

The database consists of three tables described below by example:

### Symbols
ticker | name
------ | ----
BHP    | BHP BILLITON LIMITED ORDINARY

### Shorts
ticker | date | percent
------ | ---- | -------
BHP    | 1281657600.0 | 0.7
BHP    | 1281571200.0 | 0.66

### Prices
ticker | date | open | high | low | close | volume
------ | ---- | ---- | ---- | --- | ----- | ------
BHP    | 1281657600.0 | 42.75 | 43.26 | 42.71 | 43.08 | 3691070
