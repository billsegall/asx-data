#!/usr/bin/env python3
"""Fetch ASX end-of-day data from the EODData SOAP API.

Appends new trading days to asx-eod-data/eod.csv and asx-eod-data/eom.csv,
starting from the day after the last date already in eod.csv.

Credentials are read from stockdb/.env:
    EODDATA_USER=bill@segall.net
    EODDATA_PASS=yourpassword

Usage:
    python3 fetch_eod_soap.py
    python3 fetch_eod_soap.py --from 20250906
    python3 fetch_eod_soap.py --from 20250906 --to 20250912
"""

import argparse, csv, os, sys, time
from datetime import date, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
import xml.etree.ElementTree as ET

# Load .env from the same directory as this script
_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_ENV_FILE):
    with open(_ENV_FILE) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

SOAP_URL  = 'https://ws.eoddata.com/data.asmx'
NS        = 'https://ws.eoddata.com/Data'
STOCKDB   = os.path.dirname(os.path.abspath(__file__))
EOD_CSV   = os.path.join(STOCKDB, 'asx-eod-data', 'eod.csv')
EOM_CSV   = os.path.join(STOCKDB, 'asx-eod-data', 'eom.csv')
DELAY     = 1.0   # seconds between requests


def credentials():
    user = os.environ.get('EODDATA_USER', '').strip()
    pwd  = os.environ.get('EODDATA_PASS', '').strip()
    if not user or not pwd:
        print('Error: set EODDATA_USER and EODDATA_PASS environment variables')
        sys.exit(1)
    return user, pwd


def soap_request(action, body_xml):
    """Send a SOAP request and return the parsed response XML root."""
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    {body_xml}
  </soap:Body>
</soap:Envelope>"""
    data = envelope.encode('utf-8')
    req = Request(
        SOAP_URL,
        data=data,
        headers={
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction':   f'"{NS}/{action}"',
        },
    )
    with urlopen(req, timeout=30) as resp:
        return ET.fromstring(resp.read())


def login(user, pwd):
    """Authenticate and return a session token."""
    body = f"""<Login xmlns="{NS}">
      <username>{user}</username>
      <password>{pwd}</password>
    </Login>"""
    root = soap_request('Login', body)
    result = root.find('.//{%s}LoginResult' % NS)
    if result is None:
        print('Error: unexpected Login response')
        print(ET.tostring(root, encoding='unicode'))
        sys.exit(1)
    token = result.get('Token', '')
    message = result.get('Message', '')
    if not token:
        print(f'Login failed: {message}')
        sys.exit(1)
    return token


def fetch_day(token, exchange, quote_date):
    """Fetch all quotes for exchange on quote_date (YYYYMMDD).
    Returns list of (symbol, date, open, high, low, close, volume) tuples,
    or None if no market data for that day."""
    body = f"""<QuoteListByDate xmlns="{NS}">
      <Token>{token}</Token>
      <Exchange>{exchange}</Exchange>
      <QuoteDate>{quote_date}</QuoteDate>
    </QuoteListByDate>"""
    try:
        root = soap_request('QuoteListByDate', body)
    except HTTPError as e:
        print(f' [HTTP {e.code}]')
        return None
    except URLError as e:
        print(f' [network error: {e.reason}]')
        return None

    result = root.find('.//{%s}QuoteListByDateResult' % NS)
    if result is None:
        return None

    message = result.get('Message', '')
    if message and message.lower() != 'success':
        # e.g. "Market Closed", "No Data"
        return None

    rows = []
    for q in result.findall('.//{%s}QUOTE' % NS):
        symbol = (q.get('Symbol') or '').strip()
        if not symbol:
            continue
        try:
            rows.append((
                symbol,
                quote_date,
                float(q.get('Open',   0) or 0),
                float(q.get('High',   0) or 0),
                float(q.get('Low',    0) or 0),
                float(q.get('Close',  0) or 0),
                int(float(q.get('Volume', 0) or 0)),
            ))
        except (ValueError, TypeError):
            continue

    rows.sort(key=lambda r: r[0])
    return rows if rows else None


def last_eod_date():
    """Return the last date in eod.csv as a date object."""
    last = None
    with open(EOD_CSV) as f:
        for line in f:
            line = line.strip()
            if line:
                last = line.split(',')[1]
    if not last:
        print('Error: eod.csv is empty or missing')
        sys.exit(1)
    return date(int(last[:4]), int(last[4:6]), int(last[6:8]))


def main():
    parser = argparse.ArgumentParser(description='Fetch ASX EOD data from EODData SOAP API')
    parser.add_argument('--from', dest='from_date',
                        help='Start date YYYYMMDD (default: day after last in eod.csv)')
    parser.add_argument('--to', dest='to_date',
                        help='End date YYYYMMDD (default: today)')
    args = parser.parse_args()

    user, pwd = credentials()

    print('Logging in...', end=' ', flush=True)
    token = login(user, pwd)
    print('OK')

    if args.from_date:
        d = args.from_date
        start = date(int(d[:4]), int(d[4:6]), int(d[6:8]))
    else:
        start = last_eod_date() + timedelta(days=1)

    if args.to_date:
        d = args.to_date
        end = date(int(d[:4]), int(d[4:6]), int(d[6:8]))
    else:
        end = date.today()

    if start > end:
        print(f'Already up to date (last date: {start - timedelta(days=1)})')
        return

    print(f'Fetching ASX data from {start} to {end}')

    all_rows   = []   # all new EOD rows
    month_last = {}   # (year, month) -> last day's rows (for EOM)

    current = start
    while current <= end:
        if current.weekday() >= 5:   # skip weekends
            current += timedelta(days=1)
            continue

        date_compact = current.strftime('%Y%m%d')
        print(f'  {current} ...', end=' ', flush=True)

        rows = fetch_day(token, 'ASX', date_compact)
        if not rows:
            print('no data')
            current += timedelta(days=1)
            time.sleep(DELAY)
            continue

        print(f'{len(rows)} symbols')
        all_rows.extend(rows)
        month_last[(current.year, current.month)] = rows

        current += timedelta(days=1)
        time.sleep(DELAY)

    if not all_rows:
        print('No new data fetched.')
        return

    # Append to eod.csv
    print(f'\nAppending {len(all_rows)} rows to eod.csv')
    with open(EOD_CSV, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(all_rows)

    # Append last trading day of each completed month to eom.csv
    eom_rows = []
    for (year, month), rows in sorted(month_last.items()):
        if (year, month) < (end.year, end.month):
            eom_rows.extend(rows)

    if eom_rows:
        print(f'Appending {len(eom_rows)} rows to eom.csv')
        with open(EOM_CSV, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(eom_rows)

    print('Done.')


if __name__ == '__main__':
    main()
