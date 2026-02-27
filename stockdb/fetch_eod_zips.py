#!/usr/bin/env python3
"""Download ASX EOD zip files from eoddata.com.

Downloads the current and previous year zips into asx-eod-data/zips/,
skipping any file that hasn't changed since the last download.

Credentials are read from stockdb/.env:
    EODDATA_USER=bill@segall.net
    EODDATA_PASS=yourpassword

Usage:
    python3 fetch_eod_zips.py
    python3 fetch_eod_zips.py --year 2025
"""

import argparse, os, sys
from datetime import date
from bs4 import BeautifulSoup

# Load .env from the same directory as this script
_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_ENV_FILE):
    with open(_ENV_FILE) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

try:
    import requests
except ImportError:
    print('Error: requests library required (pip install requests)')
    sys.exit(1)

BASE_URL = 'https://eoddata.com'
STOCKDB  = os.path.dirname(os.path.abspath(__file__))
ZIPS_DIR = os.path.join(STOCKDB, 'asx-eod-data', 'zips')

ZIP_URL  = BASE_URL + '/data/filedownload.aspx?sf=1&k=5rtvhbph52&e=ASX&d=9&y={year}&o=w'


def credentials():
    user = os.environ.get('EODDATA_USER', '').strip()
    pwd  = os.environ.get('EODDATA_PASS', '').strip()
    if not user or not pwd:
        print('Error: set EODDATA_USER and EODDATA_PASS environment variables')
        sys.exit(1)
    return user, pwd


def login(user, pwd):
    session = requests.Session()
    session.headers['User-Agent'] = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    )
    r = session.get(f'{BASE_URL}/login.aspx')
    soup = BeautifulSoup(r.text, 'html.parser')

    def hidden(name):
        el = soup.find('input', {'name': name})
        return el['value'] if el else ''

    payload = {
        '__VIEWSTATE':          hidden('__VIEWSTATE'),
        '__VIEWSTATEGENERATOR': hidden('__VIEWSTATEGENERATOR'),
        '__EVENTVALIDATION':    hidden('__EVENTVALIDATION'),
        '__EVENTTARGET':        'ctl00$cph1$Login1$btnLogin',
        '__EVENTARGUMENT':      '',
        'ctl00$cph1$Login1$txtEmail':    user,
        'ctl00$cph1$Login1$txtPassword': pwd,
    }
    session.post(f'{BASE_URL}/login.aspx', data=payload)
    return session


def download_year(session, year):
    dest = os.path.join(ZIPS_DIR, f'ASX_{year}.zip')
    tmp  = dest + '.tmp'
    url  = ZIP_URL.format(year=year)

    print(f'  {year}.zip  downloading ...', end=' ', flush=True)
    r = session.get(url, stream=True)
    r.raise_for_status()
    size = 0
    with open(tmp, 'wb') as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
            size += len(chunk)
    print(f'{size/1024/1024:.1f} MB', end='', flush=True)

    # Only replace if the file actually changed
    local_size = os.path.getsize(dest) if os.path.exists(dest) else 0
    if size == local_size:
        os.unlink(tmp)
        print('  (unchanged)')
        return False

    os.replace(tmp, dest)
    print()
    return True


def main():
    parser = argparse.ArgumentParser(description='Download ASX EOD zip files from eoddata.com')
    parser.add_argument('--year', type=int, action='append',
                        help='Year to download (default: current and previous year)')
    args = parser.parse_args()

    years = args.year or [date.today().year - 1, date.today().year]

    user, pwd = credentials()
    print('Logging in ...', end=' ', flush=True)
    session = login(user, pwd)
    print('OK')

    updated = []
    for year in years:
        changed = download_year(session, year)
        if changed:
            updated.append(year)

    if updated:
        print(f'\nUpdated: {", ".join(str(y) for y in updated)}')
        print('Run "make clean && make" to rebuild the database.')
    else:
        print('\nNo updates.')


if __name__ == '__main__':
    main()
