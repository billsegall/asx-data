# asx-shorts
ASX short lists processing

## Short data

The **raw** data files ('''data/shorts/RR*.csv''' were obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/), and
contains some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were processed by hand or consistency of name and character encoding to '''data/shorts/20YY.csv''' using LibreOffice.

From there, *csv2sqlite.py* loads them into a database (by default at *data/stocks.db).

## Stock price data

The historical ASX stock market data in **data/prices/0.raw** was obtained from the
[ASX Hitorical Data](https://www.asxhistoricaldata.com/archive/) and then flattened and renamed for consistency

