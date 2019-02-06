SHORTS = data/shorts

SHORTS_JSON = $(SHORTS)/2010.json $(SHORTS)/2011.json $(SHORTS)/2012.json $(SHORTS)/2013.json $(SHORTS)/2014.json $(SHORTS)/2015.json $(SHORTS)/2016.json $(SHORTS)/2017.json
SHORTS_MERGED = $(SHORTS)/merged.json
SHORTS_FILTERED = $(SHORTS)/filtered.json

all: $(SHORTS_JSON) $(SHORTS_MERGED) $(SHORTS_FILTERED)

$(SHORTS_MERGED): $(SHORTS_JSON) merge.py
	merge.py --infile $(SHORTS_JSON) --outfile $@

$(SHORTS_FILTERED): $(SHORTS_MERGED) filter.py
	filter.py --infile $(SHORTS_MERGED) --outfile $@ --top 20 --minpercent 15

# ASIC are inconsistent in their date formats (grumble)

# dd/mm/YYYY
$(SHORTS)/2010.json: $(SHORTS)/2010.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(SHORTS)/2011.json: $(SHORTS)/2011.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(SHORTS)/2014.json: $(SHORTS)/2014.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(SHORTS)/2015.json: $(SHORTS)/2015.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

# YYYY-mm-dd
$(SHORTS)/2012.json: $(SHORTS)/2012.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(SHORTS)/2013.json: $(SHORTS)/2013.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(SHORTS)/2016.json: $(SHORTS)/2016.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(SHORTS)/2017.json: $(SHORTS)/2017.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@
