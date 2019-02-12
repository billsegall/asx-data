#! /usr/bin/env python
# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, sqlite3, time, sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate database with historical data')
    parser.add_argument('--infile', type=argparse.FileType('r', encoding='ascii'), default=sys.stdin, help='input csv')
    parser.add_argument('--db', default='stocks.db', help='sqlite3 database to store into')
    parser.add_argument('--dateformat', default='%Y%m%d', help='Format of the data (strptime)')
    args = parser.parse_args()

    # Create the prices table
    conn = sqlite3.connect(args.db)
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE prices (ticker text, date date, open real, high real, low real, close real, volume
int)''')
    except sqlite3.OperationalError as error:
        # table symbols already exists
        pass


    # The CSV is in the form:
    # ticker | date | open | high | low | close | volume
    reader = csv.reader(args.infile)
    for row in reader:
        try:
            c.execute('''INSERT INTO prices values (?, ?, ?, ?, ?, ?, ?)''',
                (row[0].strip(), time.mktime(time.strptime(row[1].strip(), args.dateformat)), float(row[2]),
                float(row[3]), float(row[4]), float(row[5]), int(row[6])))
        except e as error:
            print("Insert shorts failed", e)

    conn.commit()
    conn.close()
