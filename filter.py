#! /usr/bin/env python
# Copyright (c) 2018-2019, Bill Segall
# All rights reserved. See LICENSE for details.

import argparse, csv, itertools, json, time, sys
 
# Sort key for a tuple
def getKey(tpl):
    return tpl[0]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Filter ASX short positions files')
    parser.add_argument('--infile', type=argparse.FileType('r'), default=sys.stdin, help='input JSON')
    parser.add_argument('--outfile', type=argparse.FileType('w'), default=sys.stdout, help='output JSON')
    parser.add_argument('--verbose', type=bool, default=False, help='Make verbose')
    parser.add_argument('--mindates', type=int, default=100, help='Minimum number of daily entries')
    parser.add_argument('--minpercent', type=int, default=0, help='Stocks wthout a short percentage greater than this will be ignored')
    parser.add_argument('--top', type=int, default=0, help="Filter to the top 'n'")
    args = parser.parse_args()

    # The input json is a dictionary of:
    # code -> [date, %]

    d_shorts = json.load(args.infile)
    d_outputs = {}
    d_final = {}
    percents = []

    for k, v in d_shorts.items():
        # For this purpose we are only interested in simple equities
        if len(k) > 3:
            if args.verbose:
                print(k, 'is not a simple equity')
        elif len(v) < args.mindates:
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
            elif biggest > 100:
                if args.verbose:
                    print(k, "ignoring as biggest of", biggest, "is greater than 100")
            else:
                # Track the percentages for top n filtering
                percents.append((biggest, k))
                d_outputs[k] = v

    # Do we want only the top n?
    if args.top != 0:
        s = sorted(percents, key=getKey)[-args.top:]
        for k, v in d_outputs.items():
            if k in [i[1] for i in s]:
                d_final[k] = v
    else:
        d_final = d_outputs

    print(len(d_final), "matching symbols")
    json.dump(d_final, args.outfile)
