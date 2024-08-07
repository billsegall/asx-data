#! /usr/bin/env python3
# Copyright (c) 2019-2024, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, locale, sqlite3, time, sys, cProfile, pstats, re
PROFILE=False

class StockDB:
    '''The ASX Stock Database'''

    def __init__(self, dbfile, check_same_thread):
        self.dbfile = dbfile
        self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=check_same_thread)

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
        c.execute('create table symbols (symbol text primary key, name text, industry text, mcap real)')
        c.close()

    def CreateTableShorts(self, drop):
        '''Create the shorts table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists shorts')
        c.execute('create table shorts (symbol text, date datetime, short real)')
        c.close()

    def CreateTableEndOfDay(self, drop):
        '''Create the endofday table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists endofday')
        c.execute('create table endofday (symbol text, date datetime, open real, high real, low real, close real, volume int)')
        c.close()

    def CreateTableEndOfMonth(self, drop):
        '''Create the endofmonth table, dropping any existing if asked'''
        c = self.db.cursor()
        if drop:
            c.execute('drop table if exists endofmonth')
        c.execute('create table endofmonth (symbol text, date datetime, close real)')
        c.close()

    def LookupSymbol(self, symbol):
        c = self.db.cursor()
        try:
            name, industry, mcap = c.execute('select name,industry,mcap from symbols where symbol = ?', (symbol,)).fetchone()
        except Exception as e:
            print(e)
            return (None, None, None)
        return (name, industry, mcap)


# When run we populate our database which requires some
# knowledge of our collected raw data
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the StockDB')
    parser.add_argument('--db', default='stockdb.db', help='sqlite3 database to store into')
    parser.add_argument('--drop', dest='drop', action='store_true', help='Drop existing tables')
    parser.set_defaults(drop=False)
    args = parser.parse_args()

    if PROFILE:
        cProfile.run('re.compile("foo|bar")')

    stockdb = StockDB(args.db, False)
    c = stockdb.cursor()

    # Symbols
    try:
        stockdb.CreateTableSymbols(args.drop)
    except sqlite3.OperationalError as error:
        print("Database symbols already exists, Use --drop to recreate")
        sys.exit(1)

    # Symbol data - see README.md for how that's obtained
    # The input has CSV has one header rows and is then in the form:
    # Code,Company,Listcorp-link,Market Cap,Last trade,Change, %Change", Sector
    # Market Cap is strings of the form 123,456 so we need get ints from that
    locale.setlocale( locale.LC_ALL, 'en_US.UTF-8')
    symbols = 'symbols/ASXListedCompanies.csv'
    print("Processing:", symbols)
    reader = csv.reader(open(symbols, 'r'))
    for row in reader:
        if reader.line_num >= 2: # There is no row 0, row 1 is header
            try:
                c.execute('insert into symbols values (?, ?, ?, ?)',
                    (row[0][4:].strip(), row[1].strip(), row[7].strip(), locale.atof(row[3].strip())))
                
            except Exception as error:
                print("Insert into symbols failed", error, row)
                sys.exit(1)


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
        'shorts/2024.csv' : '%d/%m/%Y'
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
    try:
        stockdb.CreateTableShorts(args.drop)
    except sqlite3.OperationalError as error:
        print("Database shorts already exists, Use --drop to recreate")
        sys.exit(1)

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


    # Now add them all to the shorts table
    for k, v in d_shorts.items():
        try:
            for date, percent in v[1]:
                #print("try", date, percent)
                c.execute('insert into shorts values (?, ?, ?)', (k, date, percent))
        except:
            print("Insert shorts", k, date, percent, "failed")
            sys.exit(1)

        # Some symbols will be delisted and not in our symbol list so add
        # what we can ignoring errors
        try:
            c.execute('insert into symbols values (?, ?, "Delisted", 0)', (k, v[0]))
        except Exception as e:
            pass

    # EndOfDay
    try:
        stockdb.CreateTableEndOfDay(args.drop)
    except sqlite3.OperationalError as error:
        # table already exists
        print("Database %s already exists, Use --drop to recreate" %(args.db,))
        sys.exit(1)

    # Price data - see README.md for how that's obtained
    # The input CSV is in the form:
    # symbol | date | open | high | low | close | volume
    eod = 'asx-eod-data/eod.csv'
    print("Processing:", eod)
    for row in csv.reader(open(eod, 'r')):
        try:
            c.execute('insert into endofday values (?, ?, ?, ?, ?, ?, ?)',
                (row[0].strip(),
                time.mktime(time.strptime(row[1].strip(), '%Y%m%d')),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
                int(row[6])))
        except Exception as error:
            print("Insert into endofday failed", error, row)
            sys.exit(1)

    # EndOfMonth
    try:
        stockdb.CreateTableEndOfMonth(args.drop)
    except sqlite3.OperationalError as error:
        # table already exists
        print("Database %s already exists, Use --drop to recreate" %(args.db,))
        sys.exit(1)

    # EOM data - The Makefile generates the subset of eod into eom.csv
    eom = 'asx-eod-data/eom.csv'
    print("Processing:", eom)
    for row in csv.reader(open(eom, 'r')):
        try:
            c.execute('insert into endofmonth values (?, ?, ?)',
                (row[0].strip(),
                time.mktime(time.strptime(row[1].strip(), '%Y%m%d')),
                float(row[5])))
        except Exception as error:
            print("Insert into endofmonth failed", error, row)
            sys.exit(1)

    # Make it so
    stockdb.commit()
    stockdb.close()

    if PROFILE:
        p = pstats.Stats()
        p.strip_dirs().sort_stats(-1).print_stats()
