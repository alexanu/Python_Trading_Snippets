
# Source: https://github.com/FEC-UIUC/Netfonds-Tick-Capture

import datetime
import requests
import os
import json

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DATA_DIR = os.path.join(BASE_DIR, "data")
LAST_DIR = os.path.join(BASE_DATA_DIR, "last")
BID_DIR = os.path.join(BASE_DATA_DIR, "bid")
ASK_DIR = os.path.join(BASE_DATA_DIR, "ask")
SYMBOLS_FILE = os.path.join(BASE_DIR, "symbols.txt")
META_FILE = os.path.join(BASE_DATA_DIR, "meta.json")

TICK_BASE_URL = "http://hopey.netfonds.no/tradedump.php"
QUOTE_BASE_URL = "http://hopey.netfonds.no/posdump.php"

EXCHANGE_CODES = ['O', 'N', 'A']

start_date = (datetime.datetime.now() - datetime.timedelta(20)).date()
latest_date = (datetime.datetime.now()- datetime.timedelta(1)).date()


def make_dirs():
    if not os.path.isdir(os.path.join(BASE_DATA_DIR)):
        os.mkdir(BASE_DATA_DIR)
    if not os.path.isdir(LAST_DIR):
        os.mkdir(LAST_DIR)
    if not os.path.isdir(ASK_DIR):
        os.mkdir(ASK_DIR)
    if not os.path.isdir(BID_DIR):
        os.mkdir(BID_DIR)
    if not os.path.isfile(META_FILE):
        with open(META_FILE, 'w') as f:
            f.write("{}")


def get_exchange_code(sym):

    def prev_weekday():
        adate = datetime.datetime.now().date() - datetime.timedelta(days=1)
        while adate.weekday() > 4: # Mon-Fri are 0-4
            adate -= datetime.timedelta(days=1)
        return adate

    def send_netfonds_request(exchange_code):
        request_args = {
            'date': prev_weekday().strftime("%Y%m%d"),
            'paper': sym + "." + exchange_code,
            'csv_format': 'txt'
        }
        return requests.get(TICK_BASE_URL, params=request_args)

    def valid_netfonds_response(r):
        return r.ok and r.text[:15] != u'<!DOCTYPE HTML>' and len(r.text.split('\n')) > 2

    for c in EXCHANGE_CODES:
        r = send_netfonds_request(c)
        if valid_netfonds_response(r):
            return c
    return "X"


def send_tick_request(sym, request_args):
    r = requests.get(TICK_BASE_URL, params=request_args)
    flast = open(os.path.join(LAST_DIR, ".".join([sym, "Last", "txt"])))
    for line in r.text.split('\n')[1:-1]:
        data = line.split("\t")
        if len(data) >= 3:
            flast.write(";".join([data[0].replace("T", " "), data[1], data[2]]) + "\n")
    flast.close()


def send_quote_request(sym, request_args):
    r = requests.get(QUOTE_BASE_URL, params= request_args)
    fask = open(os.path.join(ASK_DIR, ".".join([sym, "Ask", "txt"])), 'a')
    fbid = open(os.path.join(BID_DIR, ".".join([sym, "Bid", "txt"])), 'a')
    for line in r.text.split('\n')[1:-1]:
        data = line.split("\t")
        if len(data) >= 6:
            fask.write(";".join([data[0].replace("T", " "), data[1], data[2]]) + "\n")
            fbid.write(";".join([data[0].replace("T", " "), data[4], data[5]]) + "\n")
    fask.close()
    fbid.close()


def capture_day(sym, meta, _datestr):
    request_args = {
        'date': _datestr,
        'paper': sym + "." + meta[sym]['code'],
        'csv_format': 'txt'
    }
    send_tick_request(sym, request_args)
    send_quote_request(sym, request_args)
    meta[sym]['date'] = _datestr
    

def get_start_date(sym, meta):
    global start_date, latest_date
    if sym in meta:
        sym_start_date = datetime.datetime.strptime(meta[sym].get('date', '20000101'), "%Y%m%d").date() + \
                         datetime.timedelta(1)
        if sym_start_date > latest_date:
            return False
        if sym_start_date < start_date:
            sym_start_date = start_date
    else:
        meta[sym] = {}
        sym_start_date = start_date
    return sym_start_date


def capture_sym(sym, meta):
    global start_date, latest_date
    sym_start_date = get_start_date(sym, meta)
    if not sym_start_date:
        return
    days = (latest_date - sym_start_date).days
    if not 'code' in meta[sym]:
        meta[sym]['code'] = get_exchange_code(sym)
    if meta[sym]['code'] == "X":
        return
    for n in xrange(0, days):
        _datestr = (sym_start_date + datetime.timedelta(n)).strftime("%Y%m%d")
        capture_day(sym, meta, _datestr)
        print sym + "." + meta[sym]['code'] + " - " + _datestr


def main():
    make_dirs()
    with open(SYMBOLS_FILE) as f:
        syms = f.read().split('\n')
    with open(META_FILE) as f:
        meta = json.loads(f.read())
    for sym in syms:
        capture_sym(sym, meta)
        with open(META_FILE, 'w') as f:
            f.write(json.dumps(meta))


if __name__ == "__main__":
    main()