# asx-shorts
ASX short lists processing

The **data/0.raw** data files were obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/), and
contains some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were processed modified for consistency to **data/1.ascii** using LibreOffice.

Then, *cvs2json.py* was used to load that and convert it to json on a per year basis in **data/2.json**. See
the *Makefile* for how that's done and the differences per year. It also strips out the days which ASIC
reported as having bad data.
