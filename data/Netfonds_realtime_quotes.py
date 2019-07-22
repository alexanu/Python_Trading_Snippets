'''
Netfonds import 5 days of intraday data
'''

import numpy as np
import pandas as p
from pandas.tseries.offsets import *
import datetime as dt

from pprint import pprint as pp
import time
import itertools

# ================================================================== #
# timer start #
t0 = time.clock() 

# ================================================ #
# functions
# ~~~~~~~~~~~~~~~~~~

now   = dt.date.today()
year  = str(now.year)
m     = str(now.month)
month = '0'+m

day_5 = now - 5 * BDay()
day_4 = now - 4 * BDay()
day_3 = now - 3 * BDay()
day_2 = now - 2 * BDay()
day_1 = now - 1 * BDay()
# Add current day_0
day_0 = now - 0 * BDay()

days  = [ day_5.day, day_4.day, day_3.day, day_2.day, day_1.day, day_0.day ]
months = [ day_5.month, day_4.month, day_3.month, day_2.month, day_1.month, day_0.month ]
years = [ day_5.year, day_4.year, day_3.year, day_2.year, day_1.year, day_0.year ]
days  = [ str(d) for d in days ]
months  = [ str(ms) for ms in months ]
years  = [ str(ys) for ys in years ]

def netfonds_p( symbol ):
    url_posdump  = r'http://www.netfonds.no/quotes/posdump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'

    sym_posdump  = p.DataFrame()
    cols_posdump = [ 'bid', 'bdepth', 'bdeptht', 'offer', 'odepth', 'odeptht' ]

    # ~~~~~~~~~~~~~~~~~~
    #for day in days:
    for (day, month, year) in itertools.izip(days, months, years):
        try:
            #  Append '0' to day and month if single digit
            if len(day) == 1:
                day = '0' + day
            if len(month) == 1:
                month = '0' + month

            full_url = url_posdump % ( year, month, day, symbol, exchange_sym )
            #sym_posdump = sym_posdump.append( p.read_csv( url_posdump % ( year, month, day, symbol, exchange_sym ), index_col=0, header=0, parse_dates=True ) ) 
            sym_posdump = sym_posdump.append( p.read_csv( full_url, index_col=0, header=0, parse_dates=True ) )   
        except Exception as e:
            print( "{} posdump not found".format( symbol ) )
    sym_posdump.columns = cols_posdump
    # ~~~~~~~~~~~~~~~~~~
    return sym_posdump

def netfonds_t( symbol ):
    url_tdump = r'http://www.netfonds.no/quotes/tradedump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'
    sym_tdump = p.DataFrame()

    # ~~~~~~~~~~~~~~~~~~
    #for day in days:
    for (day, month, year) in itertools.izip(days, months, years):
        try:
            #  Append '0' to day and month if single digit
            if len(day) == 1:
                day = '0' + day
            if len(month) == 1:
                month = '0' + month

            full_url = url_tdump % ( year, month, day, symbol, exchange_sym )
            
            #sym_tdump = sym_tdump.append( p.read_csv( url_tdump % ( year, month, day, symbol, exchange_sym ),
            sym_tdump = sym_tdump.append( p.read_csv( full_url,
                        index_col=0, header=0, parse_dates=True ) )
        except Exception as e:
            print( "{} tdump not found".format( symbol ) )
    # ~~~~~~~~~~~~~~~~~~
    return sym_tdump

def resample( data ):
    dat       = data.resample( rule='1min', how='mean').dropna()
    dat.index = dat.index.tz_localize('UTC').tz_convert('US/Eastern')
    dat       = dat.fillna(method='ffill')
    return dat

def trading_start(d):
    mkt_open = dt.datetime( int(year), int(month), int(d), 9, 30 )
    return mkt_open

def trading_end(d):
    mkt_close = dt.datetime( int(year), int(month), int(d), 16, 00 )
    return mkt_close

def trading_hours(data):
    test = []
    for d in days:
        dat = data[ ( data.index > trading_start(d) ) & ( data.index < trading_end(d) ) ]
        test.append( dat )
    return test
    
# ================================================ #
# ticker/data #
# need to know exchange symbol
# N = NYSE
# O = Nasdaq
# A = Amex # common for ETFs
# ~~~~~~~~~~~~~~~~~~
ticker       = 'IWM'
exchange_sym = 'A'
str_status = 'Read {} real-time tick data from Netfonds.no; exchange {}'
             .format( ticker, exchange_sym)
print str_status

# ~~~~~~~~~~~~~~~~~~
# resample irregular tick data
raw_data = netfonds_p( ticker )
#pos = resample( netfonds_p( ticker ) )
pos = resample( raw_data )
t   = resample( netfonds_t( ticker ).dropna(axis=1) )

# ~~~~~~~~~~~~~~~~~~
# trading hours only
pos_rth = trading_hours( pos )
t_rth   = trading_hours( t )

pos_trading_days = [ pos_rth[0],pos_rth[1],pos_rth[2],pos_rth[3],pos_rth[4],pos_rth[5] ]
t_trading_days   = [ t_rth[0],t_rth[1],t_rth[2],t_rth[3],t_rth[4],t_rth[5] ]

pos_rth = p.concat( pos_trading_days,ignore_index=True )
t_rth   = p.concat( t_trading_days, ignore_index=True )

# ================================================================== #
# timer looking clean #
secs      = np.round( ( time.clock()  - t0 ), 4 )
time_secs = "{timeSecs} seconds to run".format(timeSecs = secs)
mins      = np.round( ( (  time.clock() ) -  t0 )  / 60, 4 ) 
time_mins = "| {timeMins} minutes to run".format(timeMins = mins)
hours     = np.round( (  time.clock()  -  t0 )  / 60 / 60, 4 ) 
time_hrs  = "| {timeHrs} hours to run".format(timeHrs = hours)
print( time_secs, time_mins, time_hrs )