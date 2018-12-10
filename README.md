# asx-shorts
ASX short lists processing

The **data/0.raw** data files were obtained from [ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/), and contain some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were processed modified for consistency to **data/1.ascii** using LibreOffice.

Then *cvs2jon.py* was used to load that and convert it to useful json on a per year basis. See the *Makefile*
for how that's done and the differences per year.
