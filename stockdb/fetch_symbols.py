#!/usr/bin/env python3
"""Fetch the ASX official listed companies CSV (updated nightly by ASX).

A dated snapshot (asx-official-YYYYMMDD.csv) is saved alongside the live file
whenever it has been 6 or more months since the last snapshot was taken.
"""
import glob, os, re, shutil, urllib.request
from datetime import date, datetime

SYMBOLS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'symbols')
ASX_URL = 'https://www.asx.com.au/asx/research/ASXListedCompanies.csv'

def fetch():
    os.makedirs(SYMBOLS_DIR, exist_ok=True)
    dest = os.path.join(SYMBOLS_DIR, 'asx-official.csv')
    tmp = dest + '.tmp'
    print(f"Fetching {ASX_URL}...")
    urllib.request.urlretrieve(ASX_URL, tmp)
    os.replace(tmp, dest)
    print(f"Saved {dest}")

    # Save a dated snapshot if the last one is 6+ months old (or none exists)
    today = date.today()
    dated = sorted(glob.glob(os.path.join(SYMBOLS_DIR, 'asx-official-????????.csv')))
    save_snapshot = True
    if dated:
        m = re.search(r'asx-official-(\d{8})\.csv$', dated[-1])
        if m:
            last = datetime.strptime(m.group(1), '%Y%m%d').date()
            months_since = (today.year - last.year) * 12 + (today.month - last.month)
            save_snapshot = months_since >= 6
    if save_snapshot:
        snapshot = os.path.join(SYMBOLS_DIR, f'asx-official-{today.strftime("%Y%m%d")}.csv')
        shutil.copy(dest, snapshot)
        print(f"Saved snapshot {snapshot}")

if __name__ == '__main__':
    fetch()
