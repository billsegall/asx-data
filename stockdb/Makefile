# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

DB = stockdb.db

SHORTS = shorts
SHORTS_CSV=$(SHORTS)/2010.csv $(SHORTS)/2011.csv $(SHORTS)/2012.csv $(SHORTS)/2013.csv $(SHORTS)/2014.csv $(SHORTS)/2015.csv $(SHORTS)/2016.csv $(SHORTS)/2017.csv

PRICES = prices
PRICES_CSV = $(PRICES)/prices.csv

all: $(DB)

clean:
	rm $(DB)

$(PRICES_CSV):
	cat $(PRICES)/raw/*.txt > $(PRICES_CSV)

$(DB): $(SHORTS_CSV) $(PRICES_CSV) stockdb.py
	stockdb.py  --db $@ 
