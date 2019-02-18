#! /usr/bin/env python3
# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, sqlite3, time, sys
import stockdb

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add the ASX short positions to stockdb')
    parser.add_argument('--infile', type=argparse.FileType('r', encoding='ascii'), default=sys.stdin, help='input csv')
    parser.add_argument('--db', default='stocks.db', help='sqlite3 database to store into')
    parser.add_argument('--dateformat', default='%d/%m/%Y', help='Format of the data (strptime)')
    args = parser.parse_args()
    # print(args)

    # The CSV is in the form:
    # '', '', 'Trade Data', ('',)*              - Header
    # '', '', ('dd/mm/yy', '',)*                - The dates we need
    # '', '', ('#short', %short,)*              - Header
    # 'Name', ...                               - Header
    # Name, Code, (#short, %short,)*            - The short data we want
    #
    # We end up with dict[code] -> (Name, [date, %])

    # Days with bad data
    # https://asic.gov.au/regulatory-resources/markets/short-selling/short-selling-reports-notice/
    bad = [ "19 June 2017", "16 June 2017", "15 June 2017", "1 November 2016", "3 October 2016",
    "6 October 2014", "2 September 2014", "1 September 2014", "29 August 2014", "15 November 2013",
    "7 October 2013", "28 June 2013", "27 June 2013", "26 June 2013", "25 June 2013", "24 June 2013",
    "21 June 2013", "20 June 2013", "19 June 2013", "18 June 2013", "17 June 2013", "14 June 2013",
    "13 June 2013", "7 June 2013", "22 October 2012", "19 October 2012", "18 October 2012", "17 October 2012",
    "16 October 2012", "15 October 2012", "12 October 2012", "11 October 2012", "12 March 2012"]
    baddates = []
    for date in bad:
        baddates.append(time.mktime(time.strptime(date, "%d %B %Y")))

    dates = []
    reader = csv.reader(args.infile)
    d_shorts = {}
    for row in reader:

        # There is no row zero
        if reader.line_num == 0:
            print("oops: row 0", row)
            sys.exit(1)

        # Header 1: Basically a descriptor but check it
        elif reader.line_num == 1:
            if row[2] != 'Trade Date':
                print("oops: row 1", row)
                sys.exit(1)

        # Header 2: Dates so build the date list
        elif reader.line_num == 2:
            for date in row[2::2]: # Every second
                dt = time.mktime(time.strptime(date, args.dateformat))
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
            ticker = row[1].strip()
            d_shorts[ticker] = (name, [])
            date_index = 0
            for percent in row[3::2]: # Every second
                if percent != '': # Lots of empty days
                    if dates[date_index] != 0: # Don't add days ASIC said had bad data
                        d_shorts[ticker][1].append((dates[date_index], float(percent)))
                        #print("dates", dates[date_index])

                date_index += 1


    stockdb = stockdb.StockDB(args.db)
    c = stockdb.cursor()

    # Output to database the symbols -> name mappings
    try:
        stockdb.CreateTableSymbols(False)
    except sqlite3.OperationalError as error:
        pass # Table already exists

    for k, v in d_shorts.items():
        try:
            c.execute('''INSERT OR REPLACE INTO symbols values (?, ?)''', (k, v[0]))
        except:
            print("Insert symbols", k, v[0], "failed")

    # Output to database the symbols -> (date, short) mappings
    try:
        stockdb.CreateTableShorts(False)
    except sqlite3.OperationalError as error:
        pass # Table already exists

    for k, v in d_shorts.items():
        try:
            for date, percent in v[1]:
                #print("try", date, percent)
                c.execute('''INSERT INTO shorts values (?, ?, ?)''', (k, date, percent))
        except:
            print("Insert shorts", k, date, percent, "failed")

    stockdb.commit()
    stockdb.close()
