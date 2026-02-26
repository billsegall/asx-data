#!/usr/bin/env python3
# Download ASIC short selling YTD CSV files.
#
# The ASIC JSON index covers 2010-present but the CSV format changed in 2022,
# so this script only targets 2022 onward (matching filedateformats_2022 in
# stockdb.py).  Files for 2010-2021 were downloaded manually and are left alone.
#
# Usage:
#   python3 fetch_shorts.py          # refresh current + previous year, fill gaps
#   python3 fetch_shorts.py --force  # re-download all 2022+ years

import json, os, sys, urllib.request
from datetime import datetime

INDEX_URL  = 'https://download.asic.gov.au/short-selling/short-selling-data.json'
BASE_URL   = 'https://download.asic.gov.au/short-selling/'
SHORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shorts')
FIRST_YEAR = 2022   # first year using the 2022+ CSV format

def fetch_index():
    print('Fetching ASIC index...')
    with urllib.request.urlopen(INDEX_URL) as r:
        return json.load(r)

def last_entry_per_year(entries):
    '''Return {year_str: entry} keeping the highest date per year.'''
    by_year = {}
    for e in entries:
        year = str(e['date'])[:4]
        if year not in by_year or e['date'] > by_year[year]['date']:
            by_year[year] = e
    return by_year

def download(year, entry):
    date    = str(entry['date'])
    version = entry['version']
    url     = f'{BASE_URL}RR{date}-{version}-SSDailyYTD.csv'
    outfile = os.path.join(SHORTS_DIR, f'{year}.csv')
    print(f'{year}: downloading {url}')
    try:
        urllib.request.urlretrieve(url, outfile)
        size = os.path.getsize(outfile)
        print(f'{year}: saved {size:,} bytes → {outfile}')
        return True
    except Exception as e:
        print(f'{year}: ERROR — {e}', file=sys.stderr)
        return False

def main():
    force = '--force' in sys.argv
    current_year = datetime.now().year

    entries  = fetch_index()
    by_year  = last_entry_per_year(entries)

    fetched = []
    skipped = []

    for year_str in sorted(by_year):
        year = int(year_str)
        if year < FIRST_YEAR:
            continue

        outfile = os.path.join(SHORTS_DIR, f'{year}.csv')
        exists  = os.path.exists(outfile)

        # Always refresh current and previous year; skip older complete files
        needs_download = force or (year >= current_year - 1) or not exists

        if not needs_download:
            skipped.append(year_str)
            continue

        if download(year_str, by_year[year_str]):
            fetched.append(year_str)

    print()
    if fetched:
        print(f'Downloaded: {", ".join(fetched)}')
    if skipped:
        print(f'Skipped (already complete): {", ".join(skipped)}')

    # Remind user to update filedateformats_2022 in stockdb.py for any new years
    known_years = {2022, 2023, 2024, 2025, 2026}
    new_years   = [y for y in fetched if int(y) not in known_years]
    if new_years:
        print()
        print(f'NOTE: add {new_years} to filedateformats_2022 in stockdb.py')

if __name__ == '__main__':
    main()
