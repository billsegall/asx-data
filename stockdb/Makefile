# Copyright (c) 2018-2024, Bill Segall
# All rights reserved. See LICENSE for details.

DB=stockdb.db
SHORTS_CSV=shorts/2010.csv shorts/2011.csv shorts/2012.csv shorts/2013.csv shorts/2014.csv shorts/2015.csv shorts/2016.csv shorts/2017.csv shorts/2018.csv shorts/2019.csv shorts/2020.csv shorts/2021.csv shorts/2022.csv shorts/2023.csv shorts/2024.csv

# What have data for, final year is partial (see below)
YEARS=01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 21 22 23
MONTHS=01 02 03 04 05 06 07 08 09 10 11 12
PART_YEAR=2024
PART_MONTHS=01 02 03 04 05 06 07

all: $(DB)

asx-eod-data/eod.csv asx-eod-data/eom.csv:
	(mkdir asx-eod-data/tmp; \
	cd asx-eod-data/tmp; \
	for zip in ../zips/*.zip; do unzip $$zip; done; \
	grep -vh ticker * > ../eod.csv; \
	(for year in $(YEARS); do for month in $(MONTHS); do \
		ls -1 ASX_20$$year$$month??.txt | tail -1; \
	done; done; for month in $(PART_MONTHS); do \
		ls -1 ASX_$(PART_YEAR)$$month??.txt | tail -1; \
	done) | while read eom; do grep -v ticker $$eom >> ../eom.csv; done; \
	cd ../..; \
	rm -rf asx-eod-data/tmp)
	dos2unix asx-eod-data/eod.csv
	dos2unix asx-eod-data/eom.csv


$(DB): $(SHORTS_CSV) stockdb.py asx-eod-data/eod.csv asx-eod-data/eom.csv symbols/ASXListedCompanies.csv
	rm -f .$(DB)
	stockdb.py  --db .$@
	mv .$@ $@

clean:
	@rm -f asx-eod-data/eod.csv asx-eod-data/eom.csv stockdb.db
	@rm -rf asx-eod-data/tmp
