#! /usr/bin/env python
# Copyright (c) 2018, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, json, time, sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Filter the ASX short positions lists')
    parser.add_argument('--infile', type=argparse.FileType('r', encoding='ascii'), default=sys.stdin, help='input CSV')
    parser.add_argument('--outfile', type=argparse.FileType('w'), default=sys.stdout, help='output JSON')
    parser.add_argument('--dateformat', default='%d/%m/%Y', help='Format of the data (strptime)')
    args = parser.parse_args()
    # print(args)

    # The CSV is in the form:
    # '', '', 'Trade Data', ('',)*              - Header
    # '', '', ('dd/mm/yy', '',)*                - The dates we need
    # '', '', ('#short', %short,)*              - Header
    # 'Product', ...                            - Header
    # Name, Code, (#short, %short,)*            - The short data we want
    #
    # We end up with dict[code] -> [date, %]

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
            ticker = row[1].strip()
            d_shorts[ticker] = []
            date_index = 0
            for percent in row[3::2]: # Every second
                if percent != '': # Lots of empty days
                    if dates[date_index] != 0: # Don't add days ASIC said had bad data
                        d_shorts[ticker].append((dates[date_index], float(percent)))
                        
                date_index += 1

    json.dump(d_shorts, args.outfile)
