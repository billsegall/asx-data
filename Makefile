# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

DB = data/stocks.db

SHORTS = data/shorts
SHORTS_CSV=$(SHORTS)/2010.csv $(SHORTS)/2011.csv $(SHORTS)/2012.csv $(SHORTS)/2013.csv $(SHORTS)/2014.csv $(SHORTS)/2015.csv $(SHORTS)/2016.csv $(SHORTS)/2017.csv

PRICES = data/prices
PRICES_CSV = $(PRICES)/prices.csv

all: $(DB)

clean:
	rm $(DB)

$(PRICES_CSV):
	cat $(PRICES)/raw/*.txt > $(PRICES_CSV)

$(DB): $(SHORTS_CSV) $(PRICES_CSV) shorts2sqlite.py prices2sqlite.py
	@# ASIC are inconsistent in their date formats (grumble) dd/mm/YYYY vs YYYY-mm-dd
	@# We do them in time order so any db updates makes current sense
	shorts2sqlite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2010.csv --db $@
	shorts2sqlite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2011.csv --db $@
	shorts2sqlite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2012.csv --db $@
	shorts2sqlite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2013.csv --db $@
	shorts2sqlite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2014.csv --db $@
	shorts2sqlite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2015.csv --db $@
	shorts2sqlite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2016.csv --db $@
	shorts2sqlite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2017.csv --db $@
	prices2sqlite.py --infile $(PRICES)/prices.csv --db $@
