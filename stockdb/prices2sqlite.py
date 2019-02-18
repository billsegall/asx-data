#! /usr/bin/env python3
# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, sqlite3, time, sys
import stockdb

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate database with historical data')
    parser.add_argument('--infile', type=argparse.FileType('r', encoding='ascii'), default=sys.stdin, help='input csv')
    parser.add_argument('--db', default='stocks.db', help='sqlite3 database to store into')
    parser.add_argument('--dateformat', default='%Y%m%d', help='Format of the data (strptime)')
    args = parser.parse_args()

    # Create the prices table
    stockdb = stockdb.StockDB(args.db)  
    c = stockdb.cursor()
    try:
        stockdb.CreateTablePrices(False)
    except sqlite3.OperationalError as error:
        # table already exists
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

    stockdb.commit()
    stockdb.close()
