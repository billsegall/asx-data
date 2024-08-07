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

Symbol data can be obtained from [listcorp](https://www.listcorp.com/asx)
if you sign up for their newsletter (which is not bad).

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

# Charting

There is a pretty cheesy web server (using python/flask) used for
test purposes that will serve up some ugly charts in the www directory.
This is not the purpose of this dataset and it's not well maintained.
