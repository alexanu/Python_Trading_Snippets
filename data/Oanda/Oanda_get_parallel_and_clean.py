import auth_info 
import json
import requests
import threading
import pickle
import pprint
import time
import pandas as pd 
import numpy as np
from pandas.io.json import json_normalize

#constants
hostname = "https://api-fxpractice.oanda.com"
streaming_hostname = "https://stream-fxpractice.oanda.com"
accountID = auth_info.account_id
header  ={'Authorization': auth_info.token}
# header  ={'Authorization': auth_info.token,'Content-type': 'application/json', 'Accept': 'text/plain'}
majors = ['EUR_USD', 'GBP_USD', 'USD_CAD', 'USD_CHF', 'USD_JPY']
# leverage = 10
majors_str = ''
for major in majors:
    majors_str += major
    majors_str += ','
candle_settings = {'granularity':'H12', 'count':500}
raw_data = {'candles':{}} #need to specify the data type of nested dictionaries
clean_data = {'candles':{}}

'''
Endpoints list:
/v3/accounts/{accountID}
/v3/accounts/{accountID}/transactions
/v3/accounts/{accountID}/pricing
/v3/instruments/{instrument}/candles

/v3/accounts/{accountID}/orders #POST; fills an order 
/v3/accounts/{accountID}/trades/{tradeSpecifier}/close #PUT
/v3/accounts/{accountID}/trades/{tradeSpecifier}/orders #PUT; modify trade properties
'''


def get_account():
    try:
        url = hostname + "/v3/accounts/{accountID}".format(accountID = accountID)
        r = requests.get(url, headers = header, timeout = 5)
        raw_data['account'] = r.json()['account']

    except Exception as e:
        print(str(e))

def get_transactions():
    try:
        url = hostname + "/v3/accounts/{accountID}/transactions/sinceid?id=5".format(accountID = accountID)
        r = requests.get(url, headers = header, timeout = 5)
        raw_data['transactions'] = r.json()

    except Exception as e:
        print(str(e))

def get_candles(pair):
    try:
        url = hostname + "/v3/instruments/{instrument}/candles".format(instrument = pair)
        r = requests.get(url, headers = header, params = candle_settings, timeout = 5)
        raw_data['candles'][pair] = r.json()['candles']

    except Exception as e:
        print(str(e))

def get_prices():
    try:
        url = hostname + "/v3/accounts/{accountID}/pricing".format(accountID = accountID)
        r = requests.get(url, headers = header, params = {'instruments': majors_str}, timeout = 5)
        raw_data['prices'] = r.json()['prices']
        # pprint.PrettyPrinter(indent=4).pprint(raw_data['prices'])

    except Exception as e:
        print(str(e))        

def download():
    '''
    Pulls the data from OANDA and uses threading to make it faster 
    '''
    threads = []
    threads.append(threading.Thread(target = get_account, args=()))
    threads.append(threading.Thread(target = get_transactions, args=()))
    threads.append(threading.Thread(target = get_prices, args=()))
    for pair in majors:
        threads.append(threading.Thread(target=get_candles, args= (pair, )) )

    #begin threading
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    #pickle the raw data
    if debug_mode == True:
        pickle.dump(raw_data, open('raw_data.pkl', 'wb'))
    print('Raw data pulled')


def clean():
    '''
    sorts the raw data into easily accessible dataframes and dictionaries
    '''
    #unpickle it and normalize it
    if debug_mode == True:
        pkl_raw_data = pickle.load(open('raw_data.pkl', 'rb'))
    else:
        pkl_raw_data = raw_data

    #puts the main candlestick data in dataframes
    for pair in majors:
        pair_norm = json_normalize(pkl_raw_data['candles'][pair])
        clean_data['candles'][pair] = pd.DataFrame.from_dict(pair_norm, orient = 'columns')
        clean_data['candles'][pair] = clean_data['candles'][pair].apply(pd.to_numeric, errors='ignore')
        clean_data['candles'][pair]['time'] = pd.to_datetime(clean_data['candles'][pair]['time'])

    orders_norm = json_normalize(pkl_raw_data['account']['orders'])
    clean_data['orders'] = pd.DataFrame.from_dict(orders_norm, orient = 'columns')
    clean_data['orders'] = clean_data['orders'].apply(pd.to_numeric, errors='ignore')

    positions_norm = json_normalize(pkl_raw_data['account']['positions'])
    clean_data['positions'] = pd.DataFrame.from_dict(positions_norm, orient = 'columns')
    clean_data['positions'] = clean_data['positions'].apply(pd.to_numeric, errors='ignore')

    trades_norm = json_normalize(pkl_raw_data['account']['trades'])
    clean_data['trades'] = pd.DataFrame.from_dict(trades_norm, orient = 'columns')
    clean_data['trades'] = clean_data['trades'].apply(pd.to_numeric, errors='ignore')

    #account and prices are the 2 non-dataframe entries in the clean data dictionary
    for item in ['orders', 'positions', 'trades']:
        pkl_raw_data['account'].pop(item, None)
    clean_data['account'] = pkl_raw_data['account']

    clean_data['prices'] = {}
    for dic in pkl_raw_data['prices']:
        clean_data['prices'][dic['instrument']] = {'ask': float(dic['closeoutAsk']), 
    'bid':float(dic['closeoutBid']),
    'spread': float(dic['closeoutAsk'])-float(dic['closeoutBid']),  
    'mid': (float(dic['closeoutAsk'])+float(dic['closeoutBid']))/2 }
    # pprint.PrettyPrinter(indent=4).pprint(clean_data['prices'])

    # ensures the data types in the account are numbers
    for key, value in clean_data['account'].items():
        try:
            clean_data['account'][key] = float(value)
        except:
            pass

    #pickle the clean data for analysis
    if debug_mode == True:
        pickle.dump(clean_data, open('clean_data.pkl', 'wb'))
    print('Raw data cleaned')



def process():
    '''
    Calculates the exponential moving averages and the volatility for the other models
    '''
    if debug_mode == True:
        pkl_clean_data = pickle.load(open('clean_data.pkl', 'rb'))
    else:
        pkl_clean_data = clean_data
    
    #process candle data
    for pair in majors:
        df = pkl_clean_data['candles'][pair]
        df['EMA5'] = pd.Series.ewm(df['mid.c'], span=5).mean()
        df['EMA20'] = pd.Series.ewm(df['mid.c'], span=20).mean()
        df['EMA50'] = pd.Series.ewm(df['mid.c'], span=50).mean()
        df['EMA200'] = pd.Series.ewm(df['mid.c'], span=200).mean()
        df['DEMA1'] = df['EMA20'] - df['EMA50']
        df['DEMA2'] = df['EMA50'] - df['EMA200']
        df['TR1'] = df['mid.h'] - df['mid.l'] #Current High less the current Low
        df['TR2'] = abs(df['mid.h'] - df['mid.c'].shift(1)) # Current High less the previous Close (absolute value)
        df['TR3'] = abs(df['mid.l'] - df['mid.c'].shift(1)) # Current Low less the previous Close (absolute value)
        df['TR'] = df[['TR1', 'TR2', 'TR3']].max(axis=1) #calculates the true range 
        df['ATR_cal'] = pd.Series.rolling(df['TR'], 10).mean() #average of past 10 true range prices
        df['ATR10'] = (df['ATR_cal'].shift(1)* 9 + df['ATR_cal']) / 10 #smooths it using a 10 candle span
        df['wick'] = (df['mid.c'] - df['mid.h']) + (df['mid.o'] - df['mid.l'])
        df['body'] = (df['mid.c'] - df['mid.o'])
        # df['cmetric'] = df[['wick', 'body']].max(axis=1) #need fix
        pkl_clean_data['candles'][pair] = df.drop(columns=['TR1', 'TR2', 'TR3', 'ATR_cal', 'TR', 'wick', 'body', 'complete', 'volume'])
        # print(df.head(20))

    #process prices data
    nav = pkl_clean_data['account']['NAV']
    nav_pair = nav/len(majors)
    leverage = 1/pkl_clean_data['account']['marginRate'] #leverage is inverse of margin rate
    for pair, value in pkl_clean_data['prices'].items():
        # ensures that the yen pairs don't have maxunits of 5
        if 'JPY' in pair:
            value['multiplier'] = 100
        else:
            value['multiplier'] = 1
        value['maxunits'] = int(nav_pair / value['mid'] * leverage * value['multiplier']) #10 for leverage

    pickle.dump(pkl_clean_data, open('processed_data.pkl', 'wb'))
    print('Raw data processed and pickled')

#functions to run code
debug_mode = False

if __name__ == '__main__':
    download()
    clean()
    process()