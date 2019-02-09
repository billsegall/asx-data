DB = data/stocks.db
SHORTS = data/shorts
SHORTS_CSV=$(SHORTS)/2010.csv $(SHORTS)/2011.csv $(SHORTS)/2012.csv $(SHORTS)/2013.csv $(SHORTS)/2014.csv $(SHORTS)/2015.csv $(SHORTS)/2016.csv $(SHORTS)/2017.csv

all: $(DB)

$(SHORTS_MERGED): $(SHORTS_JSON) merge.py
	merge.py --infile $(SHORTS_JSON) --outfile $@

$(SHORTS_FILTERED): $(SHORTS_MERGED) filter.py
	filter.py --infile $(SHORTS_MERGED) --outfile $@ --top 20 --minpercent 15

$(DB): $(SHORTS_CSV) csv2sqllite.py
	@# ASIC are inconsistent in their date formats (grumble) dd/mm/YYYY vs YYYY-mm-dd
	@# We do them in time order so any db updates makes current sense
	csv2sqllite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2010.csv --db $@
	csv2sqllite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2011.csv --db $@
	csv2sqllite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2012.csv --db $@
	csv2sqllite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2013.csv --db $@
	csv2sqllite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2014.csv --db $@
	csv2sqllite.py --dateformat '%d/%m/%Y' --infile $(SHORTS)/2015.csv --db $@
	csv2sqllite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2016.csv --db $@
	csv2sqllite.py --dateformat '%Y-%m-%d' --infile $(SHORTS)/2017.csv --db $@
