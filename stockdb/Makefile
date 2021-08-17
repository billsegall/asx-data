# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

DB = stockdb.db

all: $(DB)

SHORTS_CSV=shorts/2010.csv shorts/2011.csv shorts/2012.csv shorts/2013.csv shorts/2014.csv shorts/2015.csv shorts/2016.csv shorts/2017.csv shorts/2018.csv shorts/2019.csv

.PHONY: prices/prices.csv
prices/prices.csv:
	cat prices/raw/*.txt > $@

YEARS=10 11 12 13 14 15 16 17 18 19
MONTHS=01 02 03 04 05 06 07 08 09 10 11 12
.PHONY: prices/eom.csv
prices/eom.csv:
	for year in $(YEARS); do for month in $(MONTHS); do \
		ls -1 prices/raw/20$$year$$month??.txt | tail -1; \
	done; done | while read eom; do \
		cat $$eom >> $@; \
	done

$(DB): $(SHORTS_CSV) stockdb.py prices/prices.csv symbols/ASXListedCompanies.csv
	rm -f $(DB)
	stockdb.py  --db $@ 

