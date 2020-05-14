
"""
Tick source: Netfonds-Tick-Capture. Check this: https://github.com/FEC-UIUC/Netfonds-Tick-Capture

Converts tick data semicolon delimited - TIME;PRICE;VOLUME - format...
... to second bars - TIME,OPEN,HIGH,LOW,CLOSE,VWAP-PRICE,VOLUME

Source: https://github.com/FEC-UIUC/Ticks-To-Second-Bars/tree/master/src

"""

import os
import sys
import csv
import numpy as np
from datetime import datetime, timedelta

DATA_DIR =  # TODO - insert data directory path here

LAST_DATA_DIR = os.path.join(DATA_DIR, 'last')
BARS_DATA_DIR = os.path.join(DATA_DIR, 'bars')

if not os.path.isdir(BARS_DATA_DIR):
    os.mkdir(BARS_DATA_DIR)

def ticks_to_bars(sym):

    in_file = open(os.path.join(LAST_DATA_DIR, '.'.join([sym, 'Last', 'txt'])))
    csv_f = csv.reader(in_file, delimiter=';')
    out_file = open(os.path.join(BARS_DATA_DIR, '.'.join([sym, 'Bars', 'csv'])), 'w')
    print(sym)
    next_timestamp = None
    last_timestamp = None

    cur_p = []
    cur_v = 0
    vwap_sum = 0

    for row in csv_f:
        timestamp = datetime.strptime(row[0], '%Y%m%d %H%M%S')
        last = float(row[1])
        volume = int(row[2])

        if not next_timestamp:  # first day
            next_timestamp = timestamp

        if timestamp != last_timestamp and last_timestamp:  # time to load a bar
            price_ = np.round(float(vwap_sum) / cur_v, 2)
            open_ = cur_p[0]
            high_ = np.max(cur_p)
            low_ = np.min(cur_p)
            close_ = cur_p[-1]
            out_file.write(",".join(map(str,[last_timestamp,open_, high_, low_, close_, price_, cur_v])) + "\n")
            last_data = [open_, high_, low_, close_, price_, 0]
            next_timestamp = last_timestamp + timedelta(0, 1)
            cur_p = []
            cur_v = 0
            vwap_sum = 0

        if next_timestamp.day != timestamp.day:  # new day, fill in remainder
            if next_timestamp < datetime(next_timestamp.year, next_timestamp.month, next_timestamp.day, 22, 0, 0):
                s = ((datetime(next_timestamp.year, next_timestamp.month, next_timestamp.day, 22, 0,
                          0)) - next_timestamp).seconds
                values = s * [last_data]
                indexes = [timestamp + timedelta(0, ds) for ds in xrange(0, s)]
                for i in xrange(0, s):
                    out_file.write(",".join(map(str,[indexes[i]]+values[i])) + "\n")
            next_timestamp = timestamp

        if timestamp > next_timestamp:  # we are too far ahead, add some copy bars
            s = (timestamp - next_timestamp).seconds
            values = s * [last_data]
            indexes = [next_timestamp + timedelta(0, ds) for ds in xrange(0, s)]
            for i in xrange(0, s):
                out_file.write(",".join(map(str,[indexes[i]]+values[i])) + "\n")
            next_timestamp = timestamp

        cur_p += [last]
        cur_v += volume
        vwap_sum += last*volume

        last_timestamp = timestamp

    in_file.close()
    out_file.close()

if __name__ == "__main__":
    syms = sys.argv[1:]
    for sym in syms:
        ticks_to_bars(sym)



#################################################################################################

