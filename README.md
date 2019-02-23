# asx-shorts
ASX short lists processing

## ASX Short data

The **raw** short data was obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/), and
contains some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were processed by hand for consistency of name and character encoding to _data/shorts/20YY.csv_ using LibreOffice.  See the Makefile for how that's put
into the database.

## ASX price and symbol data

The **raw** price and symbol data was obtained from
[ASX Historical Data](https://www.asxhistoricaldata.com/archive/) and flattened
and renamed for consistency. See the Makefile for how that's put into the
database.

Note that the raw data does **not** cater for splits and dividends.

## The database

The database consists of three tables described below by example:

### symbols
ticker | name | industry
------ | ---- | ---
BHP    | BHP BILLITON LIMITED ORDINARY | Materials

### shorts
ticker | date | short
------ | ---- | -------
BHP    | 1281657600.0 | 0.7
BHP    | 1281571200.0 | 0.66

### prices
ticker | date | open | high | low | close | volume
------ | ---- | ---- | ---- | --- | ----- | ------
BHP    | 1281657600.0 | 42.75 | 43.26 | 42.71 | 43.08 | 3691070
