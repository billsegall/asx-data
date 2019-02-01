# asx-shorts
ASX short lists processing

## Short data

The **data/shorts/0.raw** data files were obtained from
[ASIC](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/), and
contains some [inaccuracies](https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/).

These were processed for consistency by hand to **data/shorts/1.ascii** using LibreOffice.

Then, *cvs2json.py* was used to load that and convert it to json on a per year basis in **data/shorts/2.json**. See
the *Makefile* for how that's done and the differences per year. It also strips out the days which ASIC
reported as having bad data.

Next, *merge.py* was used to join those files together so we have a complete data set in **data/shorts/3.json**.

*filter.py* is then used to extract a subset of that data for analysis. See the *Makefile* and
**data/shorts/4.json** for an example.

## Stock price data
