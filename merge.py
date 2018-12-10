#! /usr/bin/env python
# Copyright (c) 2018, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, itertools, json, time, sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Merge ASX short positions files')
    parser.add_argument('--infiles', type=argparse.FileType('r'), nargs='+', help='input JSON')
    parser.add_argument('--outfile', type=argparse.FileType('w'), default=sys.stdout, help='output JSON')
    args = parser.parse_args()

    # The input json is on a per year basis and is simply a dictionary of:
    # code -> [date, %]

    d_shorts = {}
    for f in args.infiles:
        d_year = json.load(f)
        for k, v in d_year.items():
          if k in d_shorts:
            d_shorts[k].append(v)
          else:
            d_shorts[k] = v
    json.dump(d_shorts, args.outfile)
