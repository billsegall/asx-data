#! /usr/bin/env python3
# Copyright (c) 2019, Bill Segall
# All rights reserved. See LICENSE for details.

import sqlite3

class StockDB:
    '''The ASX Stock Database'''

    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES)

    def __del__(self):
        self.db.close()

    def db(self):
        return self.db

    def ticker2name(self, ticker):
        c = self.db.cursor()
        try:
            name = c.execute('select name from symbols where ticker = ?', (ticker,)).fetchone()[0]
        except Exception as e:
            name = "Unknown"
        return name
