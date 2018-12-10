#! /usr/bin/env python
# Copyright (c) 2018, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, json, time, sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Filter the ASX short positions lists')
    parser.add_argument('--infile', type=argparse.FileType('r', encoding='ascii'), default=sys.stdin, help='input CSV')
    parser.add_argument('--outfile', type=argparse.FileType('w'), default=sys.stdout, help='output CSV')
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

    reader = csv.reader(args.infile)
    dates = []
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
                dates.append(dt)

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
            d_shorts[row[1]] = []
            date_index = 0
            for percent in row[3::2]: # Every second
                if percent != '': # Lots of empty days
                    d_shorts[row[1]].append((dates[date_index], percent))
                date_index += 1

    json.dump(d_shorts, args.outfile)
