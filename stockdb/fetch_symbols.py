#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Fetch the ASX listed companies directory and write it out in the legacy
`asx-official.csv` layout that stockdb.py's symbol-import step expects.

The original source (www.asx.com.au/asx/research/ASXListedCompanies.csv) has
been silently WAF-rejected (Incapsula "Request Rejected") since ~2026-07-28,
even with a full browser UA/Referer and real session cookies harvested from
the site itself — this isn't a spoofable header issue, ASX has locked the
endpoint down. The site's own directory page now pulls from a different,
undocumented API (asx.api.markitdigital.com) instead, so this fetches from
there and reshapes it to match the old 3-column format so stockdb.py needs
no changes.

A dated snapshot (asx-official-YYYYMMDD.csv) is saved alongside the live file
whenever it has been 6 or more months since the last snapshot was taken.
"""
import csv
import glob
import io
import os
import re
import shutil
import sys
from datetime import date, datetime

import requests

SYMBOLS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'symbols')
DIRECTORY_URL = 'https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file'
USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')


def fetch_directory_csv():
    """Fetch the live directory. Returns rows as (code, name, industry) tuples.
    Raises RuntimeError if the response doesn't look like a real directory —
    loudly, so a renewed block (or another API change) shows up in cron logs
    instead of silently no-op'ing the way the old endpoint did for 25 days."""
    resp = requests.get(DIRECTORY_URL, headers={'User-Agent': USER_AGENT}, timeout=30)
    resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.text))
    rows = []
    for row in reader:
        if len(row) < 3 or row[0].strip() == 'ASX code':
            continue  # header or blank row
        code, name, industry = row[0].strip(), row[1].strip(), row[2].strip()
        if code:
            rows.append((code, name, industry))
    if len(rows) < 1000:
        raise RuntimeError(
            f'Directory fetch returned only {len(rows)} companies (expected ~1800+) — '
            f'source may be blocked or changed shape again. First 200 chars: {resp.text[:200]!r}'
        )
    return rows


def write_legacy_csv(rows, dest):
    """Write out in the historical 'Company name,ASX code,GICS industry group'
    layout (3 columns, unquoted-unless-needed) that stockdb.py already parses."""
    with open(dest, 'w', newline='') as f:
        f.write(f'ASX listed companies as at {datetime.now().strftime("%a %b %d %H:%M:%S %Y")}\n\n')
        w = csv.writer(f)
        w.writerow(['Company name', 'ASX code', 'GICS industry group'])
        for code, name, industry in rows:
            w.writerow([name, code, industry])


def fetch():
    os.makedirs(SYMBOLS_DIR, exist_ok=True)
    dest = os.path.join(SYMBOLS_DIR, 'asx-official.csv')
    tmp = dest + '.tmp'

    print(f"Fetching {DIRECTORY_URL}...")
    rows = fetch_directory_csv()
    print(f"Got {len(rows)} companies")

    write_legacy_csv(rows, tmp)
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
    try:
        fetch()
    except Exception as e:
        print(f"fetch_symbols failed: {e}", file=sys.stderr)
        sys.exit(1)
