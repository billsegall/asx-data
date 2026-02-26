#!/usr/bin/env python3
"""Fetch ASX end-of-day data from the EODData REST API.

Appends new trading days to asx-eod-data/eod.csv and asx-eod-data/eom.csv,
starting from the day after the last date already in eod.csv.

NOTE: API access requires a premium EODData account subscription.
A basic/free account returns HTTP 401. Get your API key from:
  https://eoddata.com/myaccount/api.aspx

Usage:
    EODDATA_API_KEY=yourkey python3 fetch_eod.py
    EODDATA_API_KEY=yourkey python3 fetch_eod.py --from 20250906
    EODDATA_API_KEY=yourkey python3 fetch_eod.py --from 20250906 --to 20250912
"""

import argparse, csv, json, os, sys, time
from datetime import date, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

BASE_URL  = 'https://api.eoddata.com'
STOCKDB   = os.path.dirname(os.path.abspath(__file__))
EOD_CSV   = os.path.join(STOCKDB, 'asx-eod-data', 'eod.csv')
EOM_CSV   = os.path.join(STOCKDB, 'asx-eod-data', 'eom.csv')
DELAY     = 1.0   # seconds between requests
RETRY_MAX = 3


def api_key():
    key = os.environ.get('EODDATA_API_KEY', '').strip()
    if not key:
        print('Error: set EODDATA_API_KEY environment variable')
        sys.exit(1)
    return key


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


def fetch_day(exchange, datestamp, key):
    """Fetch all quotes for exchange on datestamp (YYYY-MM-DD).
    Returns list of quote dicts, or None if no market data for that day."""
    url = f'{BASE_URL}/Quote/List/{exchange}?DateStamp={datestamp}&ApiKey={key}'
    for attempt in range(RETRY_MAX):
        try:
            req = Request(url, headers={'Accept': 'application/json'})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                return data if data else None
        except HTTPError as e:
            if e.code == 404:
                return None     # market closed / holiday
            if e.code == 401:
                print(f' [HTTP 401 Unauthorized — check EODDATA_API_KEY]')
                sys.exit(1)
            if e.code == 429:
                wait = 60 * (attempt + 1)
                print(f' [rate limited, waiting {wait}s]', end='', flush=True)
                time.sleep(wait)
            else:
                print(f' [HTTP {e.code}]')
                return None
        except URLError as e:
            print(f' [network error: {e.reason}]')
            return None
    return None


def quotes_to_rows(quotes, date_yyyymmdd):
    """Convert API quote dicts to CSV rows, sorted by symbol."""
    rows = []
    for q in quotes:
        symbol = (q.get('symbolCode') or '').strip()
        if not symbol:
            continue
        rows.append((
            symbol,
            date_yyyymmdd,
            q.get('open',   0),
            q.get('high',   0),
            q.get('low',    0),
            q.get('close',  0),
            q.get('volume', 0),
        ))
    rows.sort(key=lambda r: r[0])
    return rows


def main():
    parser = argparse.ArgumentParser(description='Fetch ASX EOD data from EODData API')
    parser.add_argument('--from', dest='from_date',
                        help='Start date YYYYMMDD (default: day after last in eod.csv)')
    parser.add_argument('--to', dest='to_date',
                        help='End date YYYYMMDD (default: today)')
    args = parser.parse_args()

    key = api_key()

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

    all_rows    = []          # all new EOD rows
    month_last  = {}          # (year, month) -> last day's rows (for EOM)

    current = start
    while current <= end:
        if current.weekday() >= 5:   # skip weekends
            current += timedelta(days=1)
            continue

        datestamp    = current.strftime('%Y-%m-%d')
        date_compact = current.strftime('%Y%m%d')
        print(f'  {datestamp} ...', end=' ', flush=True)

        quotes = fetch_day('ASX', datestamp, key)
        if not quotes:
            print('no data')
            current += timedelta(days=1)
            time.sleep(DELAY)
            continue

        rows = quotes_to_rows(quotes, date_compact)
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
