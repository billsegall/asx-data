# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

DB = stockdb.db

all: $(DB)

SHORTS_CSV=shorts/2010.csv shorts/2011.csv shorts/2012.csv shorts/2013.csv shorts/2014.csv shorts/2015.csv shorts/2016.csv shorts/2017.csv shorts/2018.csv shorts/2019.csv

.PHONY: prices/prices.csv
prices/prices.csv:
	cat prices/raw/*.txt > $@

$(DB): $(SHORTS_CSV) stockdb.py prices/prices.csv symbols/ASXListedCompanies.csv
	rm -f .$(DB)
	stockdb.py  --db .$@
	mv .$@ $@

curve_inputs: curve/prices.csv curve/symbols.csv curve/shorts.csv

curve:
	mkdir -p curve

curve/prices.csv: prices/prices.csv curve
	echo "symbol,date,open,high,low,close,volume" > $@
	cat $< >> $@

curve/symbols.csv: symbols/ASXListedCompanies.csv curve
	echo "symbol,company,sector,mcap,weight,A,imcap,B" > $@
	tail +3 $< >> $@

curve/shorts.csv: shorts/shorts.csv
	echo "Code,Company,Sector,MarketCap,Weight,A,IndexMarketCap,B" > $@
	tail +3 $< >> $@
