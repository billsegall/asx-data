# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

DB = stockdb.db

all: $(DB)

clean:
	rm $(DB)


SHORTS_CSV=shorts/2010.csv shorts/2011.csv shorts/2012.csv shorts/2013.csv shorts/2014.csv shorts/2015.csv shorts/2016.csv shorts/2017.csv

prices/prices.csv:
	cat prices/raw/*.txt > $@

$(DB): $(SHORTS_CSV) stockdb.py prices/prices.csv symbols/ASXListedCompanies.csv
	stockdb.py  --db $@ 
