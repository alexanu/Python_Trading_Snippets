import csv
import datetime
import sys
import functools
import os
import os.path
import json
import types
import logging
# 3rd pty mods
import requests
import requests.exceptions

# Global logging setup
logging.basicConfig( stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s %(message)s')
logr = logging.getLogger( )

ROOT_DIR = os.path.join( os.path.split( __file__)[0], '..', '..')
DAT_DIR = os.path.join( ROOT_DIR, 'dat')
TICKERS_FILE_NAME = os.path.join( DAT_DIR, 'tickers.txt')
TIINGO_URL = 'https://api.tiingo.com/tiingo/daily/%s/prices?startDate=%s&endDate=%s'

AUTH_KEY = '48777192920ae042c07a1ba8cbf5b8bfd0c17c64'

START_DATE = '2018-01-31'
END_DATE = '2018-02-28'

# START_DATE = '2018-02-01'
# END_DATE = '2018-03-01'

HTTP_HEADERS = {
    'Content-Type': 'application/json',
    'Authorization' : 'Token %s' % AUTH_KEY
}


def download( ):
    with open( TICKERS_FILE_NAME, 'rt') as tickf:
        ln = tickf.readline( )
        while ln:
            ticker = ln.strip( )
            url = TIINGO_URL % ( ticker, START_DATE, END_DATE)
            try:
                resp = requests.get( url, headers=HTTP_HEADERS)
                rfname = '%s_%s_%s.json' % ( ticker, START_DATE, END_DATE)
                rfpath = os.path.join( ROOT_DIR, 'dat', rfname)
                logr.info( rfpath)
                with open( rfpath, 'wt') as rf:
                    rf.write( resp.text)
                ln = tickf.readline( )
            except requests.exceptions.ConnectionError, ex:
                logr.error( 'download: %s' % str( ex))


def normalize( ):
    keys = set( )
    dates = set( )
    results = dict( )
    start_date = end_date = '#'
    for fn in os.listdir( DAT_DIR):
        jpath = os.path.join( DAT_DIR, fn)
        if os.path.isfile( jpath):
            extup = os.path.splitext( fn)
            if extup[1] == '.json':
                with open( jpath, 'rt') as jfile:
                    tickr, start_date, end_date = extup[0].split('_')
                    jtick = json.loads( jfile.read( ))
                    # jtick should be a list of dictionaries, each dict having date, open close etc.
                    # But is the download failed it may be a dict like {'detail':'Error: unknown ticker'}
                    if type( jtick) == type( results):
                        logr.error( 'normalize: %s' % str( jtick))
                    else:
                        for jday in jtick:
                            for k in jday.iterkeys( ):
                                keys.add( k)
                            dt = jday.get('date')
                            if dt:
                                # There is a date element. We only want the first 10 chars (2018-03-10), so
                                # discard the trailing T00:00:00 timestamp stuff.
                                sdt = dt[:10]
                                dates.add( sdt)
                                tdict = results.setdefault( tickr, {})
                                tdict[sdt] = jday
                            else:
                                logr.error( 'Normalize:bad date(%s)' % str( dt))
    # Do some fix ups on the data: remove date from keys as date is the X axis, and there's no need for a
    # grid of date vs date. Also order the dates returned, by converting to date objects, sorting, then
    # converting back to a list of strings that write_csvs( ) can use as keys.
    keys.remove('date')
    dlist = [ datetime.date( int( sdt[0:4]), int( sdt[5:7]), int( sdt[8:10])) for sdt in dates]
    sdlist = sorted( dlist)
    ssdlist = [ dt.strftime( '%Y-%m-%d') for dt in sdlist]
    return results, keys, ssdlist, start_date, end_date


def write_csvs( results, keys, dates, start_date, end_date):
    for key in keys:
        ofname = os.path.join( DAT_DIR, '%s_%s_%s.csv' % ( key, start_date, end_date))
        with open( ofname, 'wt') as ofile:
            # Write out column headers: the dates
            w = csv.DictWriter( ofile, fieldnames=['ticker'] + list( dates))
            w.writeheader( )
            for tickr in sorted( results.iterkeys( )):
                row = dict( ticker=tickr)
                ticks = results.get( tickr)
                for sdt in dates:
                    tick = ticks.get( sdt, {})
                    row[sdt] = tick.get( key)
                w.writerow( row)


if __name__ == '__main__':
    download( )
    tup = normalize( )
    write_csvs( *tup)