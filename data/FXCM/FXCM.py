# Source: https://github.com/Zoldin/fxcm_api

import fxcmpy
import pandas as pd
import datetime as dt
import time
import numpy as np
import decimal
import mysql.connector

#token provided from fxcm 
token =''
con = fxcmpy.fxcmpy(access_token = token, log_level = 'error')
instruments = con.get_instruments()

#initialize start date and end date
start = dt.datetime(2018, 7,1)
stop = dt.datetime(2018,7 15)
#use get_candles methos for accessing data from fxcm
prices = con.get_candles('EUR/USD', period='m1',start=start, stop=stop)

prices.rename(columns = {'askopen':'open', 'askhigh':'high', 'asklow':'low','askclose':'close'}, inplace = True) 
ctm = prices.index.astype(np.int64)//10**9
prices = prices.assign(ctm=ctm)
prices = prices.assign(ctm_dtm=cijene.index)

#use pymysql package for data load (into mysql aws rds database)

import pymysql
#connection credentials
user=''
password=''
ip=''
from sqlalchemy import create_engine

engine = create_engine( 'mysql+pymysql://' + user + ':' + password + '@' + ip + ':3306/shema_name')

#load the prices into the database
prices[['ctm_dtm','ctm','open','low','high','close']][:-1].to_sql(name='OHLC_GBPUSD_HIST', con=engine, if_exists = 'append', index=False)



# ----------------------------------------------------------------------------------------------------

# Source: https://github.com/Abreto/fxcm-market-data-graber

import sys, os, time
from multiprocessing import Pool
import os.path
from requests import get


app_dir = os.getcwd()
data_dir = app_dir + '/data'
url_pattern = ('https://candledata.fxcorporate.com'
               '/{periodicity}/{instrument}/{year}/{week}.csv.gz')
periodicity = ['m1']
instruments = [
    'AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
    'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
    'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
    'USDCAD', 'USDCHF', 'USDJPY'
]
years = ['2012', '2013', '2014', '2015', '2016', '2017', '2018']
weeks = list(range(1, 54))
workers = 8


def guarantee(path):
    os.system('mkdir -p "{}"'.format(path))
    # try:
    #     os.mkdir(path)
        
    # except FileExistsError:
    #     pass
    # except:
    #     print('Unexpected error.')
    #     raise

def compose_url(periodicity, instrument, year, week):
    return config.url_pattern.format(
        periodicity=periodicity,
        instrument=instrument,
        year=year,
        week=week
    )


def pull(periodicity, instrument, year):
    taskname = '{}/{}/{}'.format(periodicity, instrument, year)
    basedir = os.path.join(config.data_dir, taskname)
    helpers.guarantee(basedir)
    
    for w in config.weeks:
        prefix = '/{}/{}'.format(taskname, w)
        url = helpers.compose_url(periodicity, instrument, year, w)
        savepath = os.path.join(basedir, '{}.csv.gz'.format(w))
        print('[{}] Downloading {}'.format(prefix, url))

        tries = 0
        while tries < 5:
            r = get(url, allow_redirects=True)
            if r.status_code == 404:
                print('[{}] {} not found.'.format(prefix, url))
                break
            if r.status_code != 200:
                tries = tries + 1
                continue
            print('[{}] Saving to {}'.format(prefix, savepath))
            open(savepath, 'wb').write(r.content)
            break

        if tries == 5:
            print('[{}] Failed to download {}'.format(prefix, url))


def handler(periodicity, instrument, year):
    taskname = '/{}/{}/{}'.format(periodicity, instrument, year)
    print('Handle task {} ({})'.format(taskname, os.getpid()))
    
    start_time = time.time()
    try:
        pull(periodicity, instrument, year)
    except:
        print('Unexpected error: ', sys.exc_info())
        raise
    end_time = time.time()
    print('Task {} runs {:.2f} seconds.'.format(taskname, end_time - start_time))

if __name__ == '__main__':
    print('Pulling begins')
    p = Pool(config.workers)
    for pd in config.periodicity:
        for i in config.instruments:
            for y in config.years:
                p.apply_async(handler, args=(pd, i, y))
    
    print('Tasks distributed, waiting for all tasks done...')
    p.close()
    p.join()
    print('All tasks done.')

# ----------------------------------------------------------------------------------------------------

# Source: https://github.com/ZornitsaDineva/FXCMconnectAPI


import pandas as pd
import datetime
 
 
from forexconnect import fxcorepy, ForexConnect
 
 
def session_status_changed(session: fxcorepy.O2GSession,
                           status: fxcorepy.AO2GSessionStatus.O2GSessionStatus):
    print("Trading session status: " + str(status))
 
 
def main():
 
    with ForexConnect() as fx:
        try:
            fx.login("D261153925", "384", "fxcorporate.com/Hosts.jsp",
                     "Demo", session_status_callback=session_status_changed)
 
            history = fx.get_history("EUR/USD", "H1",
                                    datetime.datetime.strptime("08.01.2019 18:55:55", '%m.%d.%Y %H:%M:%S').replace(tzinfo=datetime.timezone.utc),
                                    datetime.datetime.strptime("08.02.2019 17:45:45", '%m.%d.%Y %H:%M:%S').replace(tzinfo=datetime.timezone.utc))
 
            print("Date, BidOpen, BidHigh, BidLow, BidClose, Volume")
            for row in history:
                print("{0:s}, {1:,.5f}, {2:,.5f}, {3:,.5f}, {4:,.5f}, {5:d}".format(
                    pd.to_datetime(str(row['Date'])).strftime('%m.%d.%Y %H:%M:%S'), row['BidOpen'], row['BidHigh'],
                    row['BidLow'], row['BidClose'], row['Volume']))
 
        except Exception as e:
            print("Exception: " + str(e))
 
        try:
            fx.logout()
        except Exception as e:
            print("Exception: " + str(e))
 
 
if __name__ == "__main__":
    main()



 
def session_status_changed(session: fxcorepy.O2GSession,
                           status: fxcorepy.AO2GSessionStatus.O2GSessionStatus):
    print("Trading session status: " + str(status))
 
 
def main():
    with ForexConnect() as fx:
        try:
            fx.login("D261153925", "384", "fxcorporate.com/Hosts.jsp",
                     "Demo", session_status_callback=session_status_changed)
 
            history = fx.get_history("EUR/USD", "H1",
                                    datetime.datetime.strptime("07.01.2019 21:21:21", '%m.%d.%Y %H:%M:%S').replace(tzinfo=datetime.timezone.utc),
                                    datetime.datetime.strptime("07.12.2019 21:21:21", '%m.%d.%Y %H:%M:%S').replace(tzinfo=datetime.timezone.utc))
 
            print("Date, BidOpen, BidHigh, BidLow, BidClose, Volume")
            for row in history:
                print("{0:s}, {1:,.5f}, {2:,.5f}, {3:,.5f}, {4:,.5f}, {5:d}".format(
                    pd.to_datetime(str(row['Date'])).strftime('%m.%d.%Y %H:%M:%S'), row['BidOpen'], row['BidHigh'],
                    row['BidLow'], row['BidClose'], row['Volume']))
 
        except Exception as e:
            print("Exception: " + str(e))
 
        try:
            fx.logout()
        except Exception as e:
            print("Exception: " + str(e))
 
 
if __name__ == "__main__":
    main()


#######################################################################################################

# Source: https://github.com/grananqvist/FXCM-Forex-Data-Downloader


import os
import sys
import fxcmpy
from tqdm import tqdm
import math
import argparse
from datetime import datetime, timedelta

LARGE_TFS = ['D1', 'W1', 'M1']
MEDIUM_TFS = ['H1', 'H2', 'H3', 'H4', 'H8']
SMALL_TFS = ['m1', 'm5', 'm15', 'm30']
STEPS = { 
    **{ k: timedelta(weeks=52*10) for k in LARGE_TFS },
    **{ k: timedelta(weeks=52) for k in MEDIUM_TFS },
    **{ k: timedelta(weeks=1) for k in SMALL_TFS }
}

def download(period, symbol, token, path='./'):
    """ Downloads forex and index cfd historical data from FXCM API 
    during the period 2000-01-01 to todays date
    Arguments:
    period - the timeframe to use
    symbol - a list of symbols to download
    token - required FXCM API token
    path - path for saving the data on disk
    """

    con = fxcmpy.fxcmpy(access_token=token, log_level='error')
    
    all_symbols = con.get_instruments()
    symbols = symbol if symbol is not None else all_symbols
    print('All symbols: ',all_symbols)
    print('Symbols to download: ',symbols)

    # download for all symbols
    for symbol in symbols:

        start_date = datetime(2000,1,1) 
        end_date = start_date + STEPS[period]

        header = True
        with open(os.path.join(path, symbol.replace('/','') + '_' + period + '.csv'), 'a') as f:

            # split download up in chunks of size `STEPS`

            num_steps = math.ceil( (datetime.now() - start_date) / STEPS[period] )
            for _ in tqdm(range(num_steps)):

                # fetch data 
                df = con.get_candles(symbol, period=period,
                        start=start_date, end=end_date)

                # append to csv
                df.to_csv(f, header=header)

                start_date = end_date 
                if period not in LARGE_TFS:
                    start_date += timedelta(minutes=1)

                end_date += STEPS[period]
                header = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='FXCM Historical data downloader')
    parser.add_argument('-t', '--token', help='Your FXCM API token to authorize for access. Works with a demo account key', default=None)
    parser.add_argument('-s', '--symbol', nargs='+', help='symbols to download. Flag can be used to download multiple symbols. Example: -s EUR/USD AUD/USD. Downloads all symbols by default', default=None)
    parser.add_argument('-pe', '--period', help='Time frame for data to download. m1, m5, m15, m30, H1, H2, H3, H4, H6, H8, D1, W1, M1', default='m1')
    parser.add_argument('-p', '--path', help='Path to store downloaded data in', default='./')
    params = parser.parse_args(sys.argv[1:])

    assert params.token is not None, "you must provide an FXCM API token. They are free to get using a demo account"

    download(**vars(params))

#########################################################################################################

# Source: https://github.com/MoritzGoeckel/FXCMTickData

##from StringIO import StringIO
from io import BytesIO
import gzip
import urllib
import datetime 


url = 'https://tickdata.fxcorporate.com/'##This is the base url 
url_suffix = '.csv.gz' ##Extension of the file name
symbol = 'EURUSD' ##symbol we want to get tick data for
##Available Currencies 
##AUDCAD,AUDCHF,AUDJPY, AUDNZD,CADCHF,EURAUD,EURCHF,EURGBP
##EURJPY,EURUSD,GBPCHF,GBPJPY,GBPNZD,GBPUSD,GBPCHF,GBPJPY
##GBPNZD,NZDCAD,NZDCHF.NZDJPY,NZDUSD,USDCAD,USDCHF,USDJPY


##The tick files are stored a compressed csv.  The storage structure comes as {symbol}/{year}/{week_of_year}.csv.gz  
##The first week of the year will be 1.csv.gz where the 
##last week might be 52 or 53.  That will depend on the year.
##Once we have the week of the year we will be able to pull the correct file with the data that is needed.
start_dt =  datetime.date(2015,7,16)##random start date
end_dt = datetime.date(2015,8,16)##random end date 


start_wk = start_dt.isocalendar()[1]##find the week of the year for the start  
end_wk = end_dt.isocalendar()[1] ##find the week of the year for the end 
year = str(start_dt.isocalendar()[0]) ##pull out the year of the start

###The URL is a combination of the currency, year, and week of the year.
###Example URL https://tickdata.fxcorporate.com/EURUSD/2015/29.csv.gz
###The example URL should be the first URL of this example

##This will loop through the weeks needed, create the correct URL and print out the lenght of the file.
for i in range(start_wk, end_wk ):
    url_data = url + symbol+'/'+year+'/'+str(i)+url_suffix
    print(url_data)
    requests = urllib.request.urlopen(url_data)
    buf = BytesIO(requests.read())
    f = gzip.GzipFile(fileobj=buf)
    data = f.read()
    print(len(data))



######################################################################################

import fxcmpy
from fxcmpy import fxcmpy_tick_data_reader as tdr
from fxcmpy import fxcmpy_candles_data_reader as cdr

from configparser import ConfigParser

config = ConfigParser()
config.read(r'..\API_Connections\connections.cfg')



    def rtv_tick_data_fxcm(self, symbol, start, end):
        '''

        :param symbol: 'EURUSD' and others. see method get_avaiable_symbols()
        :param start: YYYY-MM-DD
        :param end: YYYY-MM-DD
        :return: Return a DataFrame
        '''

        try:
            api = fxcmpy.fxcmpy(access_token=config['FXCM']['access_token'], log_file=config['FXCM']['log_file'])
            td = tdr(symbol= symbol, start= start, end = end)
            df = td.get_raw_data()
            return(df)
        except:
            print('retieve error, try again!')


    def rtv_candle_data_fxcm(self, symbol, start, end, period):

        '''

        :param symbol: see get_avaiable_symbols()
        :param start: YYYY-MM-DD
        :param end: YYY-MM-DD
        :param period: 'm1','H1','D1'
        :return: Return a DataFrame
        '''

        try:
            api = fxcmpy.fxcmpy(access_token=config['FXCM']['access_token'], log_file=config['FXCM']['log_file'])
            candles = cdr(symbol= symbol, start= start, end = end, period= period)
            df = candles.get_data()
            return(df)
        except:
            print('retrieve error, try again!')
