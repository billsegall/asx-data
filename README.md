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

The price data currently in use is purchased from eoddata.com and is proprietary. Should you purchase it you should place the zip files in a directory named asx-eod-data/zips/ and we should be able to work with that. Please contact me if that isn't the case.

The **raw** price and symbol data was previously obtained from [ASX Historical
Data](https://www.asxhistoricaldata.com/archive/) and flattened and
renamed for consistency. See the Makefile for how that's put into the
database. To add a new year you'll need to specify the date formats
that seem to change most years.

Note that the raw data does **not** cater for splits and dividends.

This data is still made available in the prices directory but is no longer used. Code for dealing with it is contained in an old version in the repository.

## ASX symbol data

Symbol data can be obtained from [listcorp](https://www.listcorp.com/asx) if you sign up for their newsletter.

## The database

The database consists of three tables described below by example:

### symbols
symbol | name | industry | mcap
------ | ---- | --- | ---
BHP    | BHP BILLITON LIMITED ORDINARY | Materials | 108,172,000,000 

### shorts
symbol | date | short
------ | ---- | -------
BHP    | 1281571200.0 | 0.66

### endofday
symbol | date | open | high | low | close | volume
------ | ---- | ---- | ---- | --- | ----- | ------
BHP    | 1281657600.0 | 42.75 | 43.26 | 42.71 | 43.08 | 3691070

### endofmonth
symbol | date | close
------ | ---- | -----
BHP    | 1281657600.0 | 43.08

