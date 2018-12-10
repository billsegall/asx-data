ASCIIDIR = data/1.ascii
JSONDIR = data/2.json
MERGEDDIR = data/3.merged

JSON = $(JSONDIR)/2010.json $(JSONDIR)/2011.json $(JSONDIR)/2012.json $(JSONDIR)/2013.json $(JSONDIR)/2014.json $(JSONDIR)/2015.json $(JSONDIR)/2016.json $(JSONDIR)/2017.json
MERGED = $(MERGEDDIR)/merged.json

all: $(JSON) $(MERGED)

$(MERGED): $(JSON)
	merge.py --infile $(JSON) --outfile $@

# ASIC are inconistent in their date formats (grumble)

# dd/mm/YYYY
$(JSONDIR)/2010.json: $(ASCIIDIR)/2010.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(JSONDIR)/2011.json: $(ASCIIDIR)/2011.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(JSONDIR)/2014.json: $(ASCIIDIR)/2014.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(JSONDIR)/2015.json: $(ASCIIDIR)/2015.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

# YYYY-mm-dd
$(JSONDIR)/2012.json: $(ASCIIDIR)/2012.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(JSONDIR)/2013.json: $(ASCIIDIR)/2013.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(JSONDIR)/2016.json: $(ASCIIDIR)/2016.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(JSONDIR)/2017.json: $(ASCIIDIR)/2017.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@
