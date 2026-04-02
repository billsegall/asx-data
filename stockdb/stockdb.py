#! /usr/bin/env python3
# Copyright (c) 2019-2024, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, glob, locale, sqlite3, time, sys, cProfile, pstats, re
PROFILE=False

class StockDB:
    '''The ASX Stock Database'''

    def __init__(self, dbfile, check_same_thread):
        self.dbfile = dbfile
        self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=check_same_thread, timeout=30)

    def __del__(self):
        self.db.close()

    def close(self):
        self.db.close()

    def cursor(self):
        return self.db.cursor()

    def commit(self):
        self.db.commit()

    def db(self):
        return self.db

    def CreateTableSymbols(self, drop):
        '''Create the symbols table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists symbols')
        c.execute('create table if not exists symbols (symbol text primary key, name text, industry text, shares real, current integer not null default 1)')
        c.close()

    def CreateTableShorts(self, drop):
        '''Create the shorts table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists shorts')
        c.execute('create table if not exists shorts (symbol text, date datetime, short real)')
        c.close()

    def CreateTableEndOfDay(self, drop):
        '''Create the endofday table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists endofday')
        c.execute('create table if not exists endofday (symbol text, date datetime, open real, high real, low real, close real, volume int)')
        c.close()

    def CreateTableEndOfMonth(self, drop):
        '''Create the endofmonth table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists endofmonth')
        c.execute('create table if not exists endofmonth (symbol text, date datetime, close real)')
        c.close()

    def CreateTableCorporateEvents(self, drop=False):
        '''Create the corporate_events table (splits/consolidations)'''
        c = self.db.cursor()
        if drop:
            c.execute('DROP TABLE IF EXISTS corporate_events')
        c.execute('''CREATE TABLE IF NOT EXISTS corporate_events (
            symbol      TEXT    NOT NULL,
            date        INTEGER NOT NULL,
            event_type  TEXT    NOT NULL,
            ratio       REAL    NOT NULL,
            description TEXT,
            PRIMARY KEY (symbol, date)
        )''')
        self.db.commit()
        c.close()

    def CreateTableDividends(self, drop=False):
        '''Create the dividends table (historical per-share dividend amounts)'''
        c = self.db.cursor()
        if drop:
            c.execute('DROP TABLE IF EXISTS dividends')
        c.execute('''CREATE TABLE IF NOT EXISTS dividends (
            symbol   TEXT    NOT NULL,
            ex_date  INTEGER NOT NULL,
            amount   REAL    NOT NULL,
            currency TEXT    NOT NULL DEFAULT 'AUD',
            PRIMARY KEY (symbol, ex_date)
        )''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_dividends_symbol ON dividends(symbol)')
        self.db.commit()
        c.close()

    def CreateIndexesShorts(self):
        c = self.db.cursor()
        print("Creating shorts indexes...")
        c.execute('drop index if exists idx_shorts_symbol_date')
        c.execute('drop index if exists idx_shorts_3char_peak')
        c.execute('create index idx_shorts_symbol_date on shorts(symbol, date)')
        c.execute('create index idx_shorts_3char_peak on shorts(symbol, short desc) where length(symbol) = 3')
        c.close()

    def CreateIndexesEOD(self):
        c = self.db.cursor()
        print("Creating EOD indexes...")
        c.execute('drop index if exists idx_endofday_symbol_date')
        c.execute('create index idx_endofday_symbol_date on endofday(symbol, date)')
        c.close()

    def CreateIndexes(self):
        '''Create indexes on endofday and shorts after population for query performance'''
        self.CreateIndexesShorts()
        self.CreateIndexesEOD()

    def LookupSymbol(self, symbol):
        c = self.db.cursor()
        try:
            name, industry, shares = c.execute('select name,industry,shares from symbols where symbol = ?', (symbol,)).fetchone()
        except Exception as e:
            print(e)
            return (None, None, None)
        return (name, industry, shares)


# When run we populate our database which requires some
# knowledge of our collected raw data
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the StockDB')
    parser.add_argument('--db', default='stockdb.db', help='sqlite3 database to store into')
    parser.add_argument('--drop', dest='drop', action='store_true', help='Drop existing tables')
    parser.add_argument('--shorts',  dest='shorts',  action='store_true', help='Rebuild shorts tables only')
    parser.add_argument('--symbols', dest='symbols', action='store_true', help='Rebuild symbols table only')
    parser.add_argument('--eod',     dest='eod',     action='store_true', help='Rebuild EOD tables only')
    parser.set_defaults(drop=False)
    args = parser.parse_args()

    partial = args.shorts or args.symbols or args.eod
    build_symbols = args.symbols or not partial
    build_shorts  = args.shorts  or not partial
    build_eod     = args.eod     or not partial
    drop_symbols  = args.drop
    drop_shorts   = args.drop
    drop_eod      = args.drop

    if PROFILE:
        cProfile.run('re.compile("foo|bar")')

    stockdb = StockDB(args.db, False)
    c = stockdb.cursor()

    # A. Bulk-load PRAGMAs — only for full rebuilds; partial updates leave journal_mode alone
    #    (journal_mode=OFF requires exclusive access; incompatible with live Flask connection)
    if not partial:
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
    c.execute('PRAGMA cache_size=-65536')   # 64 MB page cache

    # C. Date cache — only ~6000 unique trading dates across 6.9M EOD rows
    _date_cache = {}
    def parse_date(s):
        if s not in _date_cache:
            _date_cache[s] = time.mktime(time.strptime(s, '%Y%m%d'))
        return _date_cache[s]

    if build_symbols:
        stockdb.CreateTableSymbols(drop_symbols)

        # Symbol names and industry from ASX official CSV (fetched by fetch_symbols.py)
        # Format: Company name, ASX code, GICS industry group
        symbols_official = 'symbols/asx-official.csv'
        print("Processing:", symbols_official)
        d_symbols = {}
        reader = csv.reader(open(symbols_official, 'r'))
        for row in reader:
            if len(row) < 3 or not row[1].strip() or row[1].strip() == 'ASX code':
                continue  # skip title, blank, and column header rows
            try:
                d_symbols[row[1].strip()] = (row[0].strip(), row[2].strip())
            except Exception as error:
                print("Parse symbols failed", error, row)
                sys.exit(1)

        # Shares outstanding derived from most recent ListCorp snapshot
        # (ASXListedCompanies-YYYYMMDD.csv): shares = mcap / last_trade_price
        # Format: Code (ASX:XXX), Company, Link, Market Cap, Last trade, Change, %Change, Sector
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        listcorp_files = sorted(glob.glob('symbols/ASXListedCompanies-????????.csv'))
        d_shares = {}
        if listcorp_files:
            latest_listcorp = listcorp_files[-1]
            print("Deriving shares outstanding from:", latest_listcorp)
            reader = csv.reader(open(latest_listcorp, 'r'))
            for row in reader:
                if reader.line_num == 1:
                    continue  # header row
                try:
                    symbol = row[0][4:].strip()  # strip "ASX:" prefix
                    mcap = locale.atof(row[3].strip())
                    price = float(row[4].strip())
                    if price:
                        d_shares[symbol] = mcap / price
                except Exception:
                    pass  # skip rows with missing/unparseable data

        # Insert all symbols — preserve current flag for existing rows
        for symbol, (name, industry) in d_symbols.items():
            try:
                c.execute('''INSERT INTO symbols (symbol, name, industry, shares, current)
                             VALUES (?, ?, ?, ?, 1)
                             ON CONFLICT(symbol) DO UPDATE SET
                                 name     = excluded.name,
                                 industry = excluded.industry,
                                 shares   = excluded.shares''',
                    (symbol, name, industry, d_shares.get(symbol, 0)))
            except Exception as error:
                print("Insert into symbols failed", error, symbol)
                sys.exit(1)


    if build_shorts:
        # Short data - see README.md for how that's obtained
        # The input CSV is in the form:
        # '', '', 'Trade Data', ('',)*              - Header
        # '', '', ('dd/mm/yy', '',)*                - The dates we need
        # '', '', ('#short', %short,)*              - Header
        # 'Name', ...                               - Header
        # Name, Code, (#short, %short,)*            - The short data we want

        # The ASX are inconsistent in their date formats
        filedateformats_2014 = {
            'shorts/2010.csv' : '%d/%m/%Y',
            'shorts/2011.csv' : '%d/%m/%Y',
            'shorts/2012.csv' : '%Y-%m-%d',
            'shorts/2013.csv' : '%Y-%m-%d',
            'shorts/2014.csv' : '%d/%m/%Y',
            'shorts/2015.csv' : '%d/%m/%Y',
            'shorts/2016.csv' : '%Y-%m-%d',
            'shorts/2017.csv' : '%Y-%m-%d',
            'shorts/2018.csv' : '%Y-%m-%d',
            'shorts/2019.csv' : '%Y-%m-%d',
            'shorts/2020.csv' : '%d/%m/%Y',
            'shorts/2021.csv' : '%Y-%m-%d'
        }

        # And they're inconsistent in their file formats as well as of 2022
        filedateformats_2022 = {
            'shorts/2022.csv' : '%d/%m/%Y',
            'shorts/2023.csv' : '%d/%m/%Y',
            'shorts/2024.csv' : '%d/%m/%Y',
            'shorts/2025.csv' : '%d/%m/%Y',
            'shorts/2026.csv' : '%d/%m/%Y',
        }

        # The ASX have some days with bad data
        # https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/
        bad = [
        "22 December 2022", "1 February 2022", "31 January 2022", "15 September 2020",
        "2 September 2020", "1 September 2020", "25 May 2020", "19 June 2017",
        "16 June 2017", "15 June 2017", "1 November 2016", "3 October 2016",
        "6 October 2014", "2 September 2014", "1 September 2014", "29 August 2014", "15 November 2013",
        "7 October 2013", "28 June 2013", "27 June 2013", "26 June 2013", "25 June 2013", "24 June 2013",
        "21 June 2013", "20 June 2013", "19 June 2013", "18 June 2013", "17 June 2013", "14 June 2013",
        "13 June 2013", "7 June 2013", "22 October 2012", "19 October 2012", "18 October 2012", "17 October 2012",
        "16 October 2012", "15 October 2012", "12 October 2012", "11 October 2012", "12 March 2012"]
        baddates = []
        for date in bad:
            baddates.append(time.mktime(time.strptime(date, "%d %B %Y")))

        # table shorts: symbol -> (date, short) mappings
        stockdb.CreateTableShorts(drop_shorts)

        # For incremental updates, only insert rows newer than what's already in the DB
        max_existing_shorts = c.execute('SELECT MAX(date) FROM shorts').fetchone()[0] or 0

        d_shorts = {}

        for f, fmt in filedateformats_2014.items():
            print("Processing:", f, fmt)
            reader = csv.reader(open(f, 'r'))
            dates = []
            for row in reader:

                # There is no row zero

                # Header 1: Basically a descriptor but check it
                if reader.line_num == 1:
                    if row[2] != 'Trade Date':
                        print("oops: row 1", row)
                        sys.exit(1)

                # Header 2: The dates
                if reader.line_num == 2:
                    for date in row[2::2]: # Every second
                        try:
                            dt = time.mktime(time.strptime(date, fmt))

                        except Exception as e:
                            print("Failed on:", date, fmt)
                            print(e)
                        if not dt in baddates:
                            dates.append(dt)
                        else:
                            dates.append(0)

                # Header 3, Another descriptor but check it
                elif reader.line_num == 3:
                    if row[2] != 'Reported Short Positions':
                        print("oops: row 3", row)
                        sys.exit(1)

                # Header 4, Another descriptor but check it
                elif reader.line_num == 4:
                        if row[0] != 'Product':
                            print("oops: row 4", row)
                            sys.exit(1)

                # short data to add to our dictionary
                else:
                    name = row[0].strip()
                    symbol = row[1].strip()
                    if symbol not in d_shorts:
                        d_shorts[symbol] = (name, [])
                    date_index = 0
                    for percent in row[3::2]: # Every second
                        if percent != '': # Lots of empty days
                            if dates[date_index] != 0: # Don't add days ASIC said had bad data
                                d_shorts[symbol][1].append((dates[date_index], float(percent.replace(',',''))))
                                #print("dates", dates[date_index])

                        date_index += 1

        for f, fmt in filedateformats_2022.items():
            print("Processing:", f, fmt)
            reader = csv.reader(open(f, 'r'))
            dates = []
            for row in reader:

                # There is no row zero

                # Header 1: Basically a descriptor but check it
                if reader.line_num == 1:
                    if row[1] != 'Trade Date':
                        print("oops: row 1", "rows[0]", row[0], "rows[1]", row[1], row)
                        sys.exit(1)

                # Header 2: Dates so build the date list
                # elif reader.line_num == 2:
                    for date in row[2::2]: # Every second
                        try:
                            dt = time.mktime(time.strptime(date, fmt))
                            #print("date:", date)
                        except Exception as e:
                            print("Failed on:", date, fmt)
                            print(e)
                        if not dt in baddates:
                            dates.append(dt)
                        else:
                            dates.append(0)

                # Header 3, Another descriptor but check it
                elif reader.line_num == 2:
                    if row[2] != 'Reported Short Positions':
                        print("oops: row 2", row)
                        sys.exit(1)

                # short data to add to our dictionary
                else:
                    name = row[0].strip()
                    symbol = row[1].strip()
                    if symbol not in d_shorts:
                        d_shorts[symbol] = (name, [])
                    date_index = 0
                    for percent in row[3::2]: # Every second
                        if percent != '' and percent != '-': # Lots of empty days
                            if dates[date_index] != 0: # Don't add days ASIC said had bad data
                                #print("percent", percent)
                                d_shorts[symbol][1].append((dates[date_index], float(percent.replace(',',''))))
                                #print("dates", dates[date_index])

                        date_index += 1

        # Now add them all to the shorts table — B. executemany
        # Skip rows already in DB (incremental update: only insert dates > max existing)
        shorts_rows = [(k, date, pct) for k, v in d_shorts.items() for date, pct in v[1]
                       if date > max_existing_shorts]
        print(f"Inserting {len(shorts_rows)} new shorts rows (max existing date: {max_existing_shorts})")
        try:
            c.executemany('insert into shorts values (?, ?, ?)', shorts_rows)
        except Exception as e:
            print("Insert shorts failed:", e)
            sys.exit(1)

        # Some symbols will be delisted and not in our symbol list so add
        # what we can ignoring errors
        for k, v in d_shorts.items():
            try:
                c.execute('insert into symbols values (?, ?, "Delisted", 0, 0)', (k, v[0]))
            except Exception as e:
                pass

        stockdb.CreateIndexesShorts()

    if build_eod:
        # EndOfDay
        stockdb.CreateTableEndOfDay(drop_eod)

        # Price data - see README.md for how that's obtained
        # The input CSV is in the form:
        # symbol | date | open | high | low | close | volume
        eod = 'asx-eod-data/eod.csv'
        print("Processing:", eod)
        def _eod_rows():
            for row in csv.reader(open(eod, 'r')):
                try:
                    yield (row[0].strip(), parse_date(row[1].strip()),
                           float(row[2]), float(row[3]), float(row[4]), float(row[5]), int(row[6]))
                except Exception as error:
                    print("Insert into endofday failed", error, row)
                    sys.exit(1)

        if not drop_eod:
            # Date-bounded merge: delete rows up to the max date in the CSV, then
            # reinsert — this preserves any Yahoo Finance rows for newer dates.
            max_eod_date = 0
            for row in csv.reader(open(eod, 'r')):
                try:
                    d = parse_date(row[1].strip())
                    if d > max_eod_date:
                        max_eod_date = d
                except Exception:
                    pass
            print(f"  Deleting endofday rows up to {max_eod_date} (date-bounded merge)")
            c.execute('DELETE FROM endofday WHERE date <= ?', (max_eod_date,))

        c.executemany('insert into endofday values (?, ?, ?, ?, ?, ?, ?)', _eod_rows())

        # EndOfMonth
        stockdb.CreateTableEndOfMonth(drop_eod)

        # EOM data - The Makefile generates the subset of eod into eom.csv
        eom = 'asx-eod-data/eom.csv'
        print("Processing:", eom)
        def _eom_rows():
            for row in csv.reader(open(eom, 'r')):
                try:
                    yield (row[0].strip(), parse_date(row[1].strip()), float(row[5]))
                except Exception as error:
                    print("Insert into endofmonth failed", error, row)
                    sys.exit(1)

        if not drop_eod:
            max_eom_date = 0
            for row in csv.reader(open(eom, 'r')):
                try:
                    d = parse_date(row[1].strip())
                    if d > max_eom_date:
                        max_eom_date = d
                except Exception:
                    pass
            c.execute('DELETE FROM endofmonth WHERE date <= ?', (max_eom_date,))

        c.executemany('insert into endofmonth values (?, ?, ?)', _eom_rows())

        # Fix known eoddata.com decimal-point artifacts in XAO index data.
        # Legitimate XAO range is ~2500-10000; values outside 1500-20000 are 10x errors.
        c.execute('''UPDATE endofday SET open=open/10, high=high/10, low=low/10, close=close/10
                     WHERE symbol='XAO' AND close > 20000''')
        c.execute('''UPDATE endofday SET open=open*10, high=high*10, low=low*10, close=close*10
                     WHERE symbol='XAO' AND close < 1500''')

        stockdb.CreateIndexesEOD()

    # Make it so
    stockdb.commit()
    stockdb.close()

    if PROFILE:
        p = pstats.Stats()
        p.strip_dirs().sort_stats(-1).print_stats()
