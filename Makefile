ASCII = data/1.ascii
JSON = data/2.json

all: $(JSON)/2010.json $(JSON)/2011.json $(JSON)/2012.json $(JSON)/2013.json $(JSON)/2014.json $(JSON)/2015.json $(JSON)/2016.json $(JSON)/2017.json

# ASIC are inconistent in their date formats (grumble)

# dd/mm/YYYY
$(JSON)/2010.json: $(ASCII)/2010.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(JSON)/2011.json: $(ASCII)/2011.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(JSON)/2014.json: $(ASCII)/2014.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

$(JSON)/2015.json: $(ASCII)/2015.csv csv2json.py
	csv2json.py --dateformat '%d/%m/%Y' --infile $< --outfile $@

# YYYY-mm-dd
$(JSON)/2012.json: $(ASCII)/2012.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(JSON)/2013.json: $(ASCII)/2013.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(JSON)/2016.json: $(ASCII)/2016.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@

$(JSON)/2017.json: $(ASCII)/2017.csv csv2json.py
	csv2json.py --dateformat '%Y-%m-%d' --infile $< --outfile $@
