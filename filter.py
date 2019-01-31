#! /usr/bin/env python
# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, itertools, json, time, sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Filter ASX short positions files')
    parser.add_argument('--infile', type=argparse.FileType('r'), default=sys.stdin, help='input JSON')
    parser.add_argument('--outfile', type=argparse.FileType('w'), default=sys.stdout, help='output JSON')
    parser.add_argument('--mindates', type=int, default=100, help='Minimum number of daily entries')
    parser.add_argument('--verbose', type=bool, default=False, help='Make verbose')
    parser.add_argument('--minpercent', type=int, default=10, help='Stocks wthout a short percentage greater than this will be ignored')
    args = parser.parse_args()

    # The input json is a dictionary of:
    # code -> [date, %]

    d_shorts = json.load(args.infile)
    for k, v in d_shorts.items():
        if len(v) < args.mindates:
            if args.verbose:
                print(k, "has too few entries")
        else:
            biggest = 0
            for entry in v:
                percent = entry[1]
                if percent > biggest:
                    biggest = percent
            if biggest < args.minpercent:
                if args.verbose:
                    print(k, "ignoring as biggest of", biggest, "is less than minpercent of", args.minpercent)
            else: 
                print(k, v)
