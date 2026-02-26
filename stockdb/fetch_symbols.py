#!/usr/bin/env python3
"""Fetch the ASX official listed companies CSV (updated nightly by ASX)."""
import os, urllib.request

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

if __name__ == '__main__':
    fetch()
