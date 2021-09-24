# Copyright (c) 2018-2021, Bill Segall
# All rights reserved. See LICENSE for details.

DB = stockdb.db

all: $(DB)

SHORTS_CSV=shorts/2010.csv shorts/2011.csv shorts/2012.csv shorts/2013.csv shorts/2014.csv shorts/2015.csv shorts/2016.csv shorts/2017.csv shorts/2018.csv shorts/2019.csv shorts/2020.csv shorts/2021.csv

# What we have data for in asx-eod-data, final year is partial (see below)
YEARS=01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20
MONTHS=01 02 03 04 05 06 07 08 09 10 11 12
MONTHS21=01 02 03 04

asx-eod-data/eod.csv asx-eod-data/eom.csv:
	(cd asx-eod-data; \
	rm eod.csv eom.csv; \
	mkdir tmp; \
	cd tmp; \
	for zip in ../zips/*.zip; do unzip $$zip; done; \
	cd ..; \
	grep -vh ticker tmp/* > eod.csv; \
	(for year in $(YEARS); do for month in $(MONTHS); do \
		ls -1 tmp/ASX_20$$year$$month??.txt | tail -1; \
	done; done; for month in $(MONTHS21); do \
		ls -1 tmp/ASX_2021$$month??.txt | tail -1; \
	done) | while read eom; do grep -v ticker $$eom >> eom.csv; done; \
	rm -rf tmp; \
	dos2unix eod.csv; \
	dos2unix eom.csv)

$(DB): $(SHORTS_CSV) stockdb.py asx-eod-data/eod.csv asx-eod-data/eom.csv symbols/ASXListedCompanies.csv
	rm -f .$(DB)
	stockdb.py  --db .$@
	mv .$@ $@

all: asx-eod-data/eod.csv asx-eod-data/eom.csv

clean:
	@rm -f asx-eod-data/eod.csv asx-eod-data/eom.csv
	@rm -rf asx-eod-data/tmp


