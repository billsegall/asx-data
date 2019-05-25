# asx-shorts
ASX short lists processing

## ASX Short data

The **raw** short data was obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-position-reports-table/), and
contains some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were then massaged a little (ascii/latin, trailing comma on dates). See the Makefile for how they're then put
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
symbol | name | industry
------ | ---- | ---
BHP    | BHP BILLITON LIMITED ORDINARY | Materials

### shorts
symbol | date | short
------ | ---- | -------
BHP    | 1281571200.0 | 0.66

### prices
symbol | date | open | high | low | close | volume
------ | ---- | ---- | ---- | --- | ----- | ------
BHP    | 1281657600.0 | 42.75 | 43.26 | 42.71 | 43.08 | 3691070
