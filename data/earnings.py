"""
Copyright: Copyright (C) 2015 Baruch College
Description: automatic process to get stock before/after close price around surprise dates
"""

# python data structure imports
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import bisect

# Data source imports
import pandas.io.data as web
import Quandl

# Parser helper imports
import urllib
import urllib2
from lxml import etree
import xml.etree.ElementTree as ET

# Multiprocessing
from multiprocessing import Pool

# Logging
import logging


quandl_token='xxxx'

def make_dir(date):
    s_dir = os.getcwd() + '/' + date.strftime('%Y-%m-%d')
    if not os.path.exists(s_dir):
        os.makedirs(s_dir)
    return s_dir


def get_index_symbols(date, index='spr', use_cache=True):
    # database path for SPR symbol list
    s_symbol_path = make_dir(date)+'/SPR_symbols.csv'
    if os.path.isfile(s_symbol_path) and use_cache:
        return pd.read_csv(s_symbol_path, index_col=0)
    
    if index in ['sp400', 'sp500', 'sp600']:
        s_url = 'http://www.barchart.com/stocks/' + index + '.php?_dtp1=0'
        ls_lines = urllib2.urlopen(s_url).readlines()
        for s_line in ls_lines:
            if s_line.startswith('<table class="datatable ajax"'):
                ls_symbols = s_line.split('symbols=')[1].split(';')[0].split(',')
                df_symbols = pd.DataFrame(ls_symbols, columns = ['Symbol'])
                df_symbols['Source'] = index
                return df_symbols

    # combine sp400, sp500, sp600 into spr symbol list
    elif index == 'spr':
        df_sp400 = get_index_symbols(date, 'sp400', use_cache)
        df_sp500 = get_index_symbols(date, 'sp500', use_cache)
        df_sp600 = get_index_symbols(date, 'sp600', use_cache)
        df_spr = df_sp400.append(df_sp500).append(df_sp600)
        df_spr = df_spr.sort('Symbol').set_index('Symbol')
        df_spr.to_csv(s_symbol_path)
        logging.info('\t' + index.upper() + '\tsymbols downloaded successfully')
        return df_spr

    else:
        logging.debug('\t' + index.upper() + '\tfailed to download')

def get_quandl_earnings(date, use_cache=True):
    ls_spr = get_index_symbols(date, 'spr', use_cache).index.values
    
    # Derive surprise dates for each equity
    for i, s_equity in enumerate(ls_spr):
        s_equity_path = make_dir(date) + '/' + s_equity + '.csv'
        if os.path.isfile(s_equity_path):
            continue
        
        
        df_earnings = Quandl.get("ZES/"+s_equity.replace('.','_'), authtoken=quandl_token) # get surprise date from Quandl
        ls_earnings_dates = df_earnings.index.values
        
        # derive close price from Yahoo
        while True:
            try:
                dt_start = ls_earnings_dates[0] - np.timedelta64(1, 'W')
                break
            except IndexError:
                pass
        dt_end   = ls_earnings_dates[-1] + np.timedelta64(1, 'W')
        df_close = web.DataReader(s_equity.replace('.','/'), 'yahoo', dt_start, dt_end)['Adj Close']
        ls_close_dates = list(df_close.index)
        
        # append before/after close into df_earnings
        ls_annouce_hour = []
        ls_prev_close = []
        ls_next_close  = []
        
        for dt_date in ls_earnings_dates:
            i_annouce_hour = int(str(dt_date)[11:13])
            ls_annouce_hour.append(i_annouce_hour)
            
            while True:
                try:
                    i_date = ls_close_dates.index(dt_date)
                    break
                except ValueError:
                    if i_annouce_hour > 16:
                        dt_date -= np.timedelta64(1,'D')
                    elif i_annouce_hour < 9:
                        dt_date += np.timedelta64(1,'D')
                    else:
                        raise ValueError('Earning Annouce Hour between 9am and 4pm')
        
            if i_annouce_hour > 16:
                ls_prev_close.append(df_close[i_date])
                ls_next_close.append(df_close[i_date+1])
            elif i_annouce_hour < 9:
                ls_prev_close.append(df_close[i_date-1])
                ls_next_close.append(df_close[i_date])
            else:
                raise ValueError('Earning Annouce Hour between 9am and 4pm')

        df_earnings['ANNOUCE_HOUR'] = ls_annouce_hour
        df_earnings['BEFORE_CLOSE'] = ls_prev_close
        df_earnings['AFTER_CLOSE']  = ls_next_close
        
        # save into csv
        df_earnings.to_csv(s_equity_path, date_format='%Y%m%d')

def get_stock_earnings(s_equity):
    ''' Derive earnings history for each equity ''' 
    try:
        # database for equity, if exists then skip
        date = dt.date.today()
        s_equity_path = make_dir(date) + '/' + s_equity + '.csv'
        if os.path.isfile(s_equity_path):
            return True

        # get earnings history raw data from busystock
        s_params = urllib.urlencode({'s': s_equity.replace('.','-')})
        s_url = 'http://busystock.com/i.php?%s' % s_params
        bt_raw = urllib2.urlopen(s_url).read()
        
        # parse raw data into a dataframe
        treeElem_root = ET.fromstring('<table>'+bt_raw.split('<table>')[1].split('</table>')[0]+'</table>',parser=etree.XMLParser(recover=True))
        ls_earning_date   = []
        ls_time           = []
        ls_eps_est        = []
        ls_eps_act        = []
        for treeElem_child1 in treeElem_root[1:]:
            ls_earning_date.append(dt.datetime.strptime(treeElem_child1[0][0].text, '%Y-%m-%d').date())
            ls_time.append(treeElem_child1[1].text)
            try:
                ls_eps_est.append(float(treeElem_child1[3].text))
            except TypeError:
                ls_eps_est.append(None)
            try:
                ls_eps_act.append(float(treeElem_child1[4].text))
            except TypeError:
                ls_eps_act.append(None)
        df_earnings = pd.DataFrame({'EarningDate': ls_earning_date, 'Time': ls_time, 'EPS-E': ls_eps_est, 'EPS-A': ls_eps_act})

        # construct Prev Close and Next Close column in df_earnings
        ls_reference_date = []
        ls_prev_close     = []
        ls_next_close     = []
        ls_pprev_close    = []
        ls_nnext_close    = []

        if len(df_earnings) != 0:
            # get adjusted close data from Yahoo Finance
            dt_start = ls_earning_date[-1] - dt.timedelta(days=10)
            df_close = web.DataReader(s_equity.replace('.','/'), 'yahoo', dt_start, date)['Adj Close']
            ls_close_date = [dt.date(ts_date.year, ts_date.month, ts_date.day) for ts_date in df_close.index]
            
            for dt_date, s_time in zip(ls_earning_date, ls_time):
                i_date = bisect.bisect(ls_close_date, dt_date) 

                # yahoo finance does not have data as early as earning date
                if i_date == 0:
                    ls_reference_date.append(None)
                    ls_pprev_close.append(None)
                    ls_prev_close.append(None)
                    ls_next_close.append(None)
                    ls_nnext_close.append(None)
                elif s_time == 'AMC' or s_time == None:
                    ls_pprev_close.append(df_close[i_date-2])
                    ls_prev_close.append(df_close[i_date-1])
                    try:
                        ls_next_close.append(df_close[i_date])
                        ls_reference_date.append(ls_close_date[i_date])
                    except IndexError:
                        ls_next_close.append(None)
                        ls_reference_date.append(None)
                    try:
                        ls_nnext_close.append(df_close[i_date+1])
                    except IndexError:
                        ls_nnext_close.append(None)

                elif s_time == 'BMO':
                    ls_reference_date.append(dt_date)
                    ls_pprev_close.append(df_close[i_date-3])
                    ls_prev_close.append(df_close[i_date-2])
                    ls_next_close.append(df_close[i_date-1])
                    try:
                        ls_nnext_close.append(df_close[i_date])
                    except IndexError:
                        ls_nnext_close.append(None)
                else:
                    raise TypeError('Earning Annouce Time Missing.')

        df_earnings['ReferenceDate'] = ls_reference_date
        df_earnings['PPrevClose']    = ls_pprev_close
        df_earnings['PrevClose']     = ls_prev_close
        df_earnings['NextClose']     = ls_next_close
        df_earnings['NNextClose']    = ls_nnext_close

        # convert date format
        df_earnings['EarningDate']   = [date.strftime('%Y%m%d') for date in df_earnings['EarningDate']]
        df_earnings['ReferenceDate'] = [date.strftime('%Y%m%d') if date != None else None for date in df_earnings['ReferenceDate']]
        
        # save into csv
        df_earnings = df_earnings[['EarningDate', 'Time', 'ReferenceDate', 'PPrevClose', 'PrevClose', 'NextClose', 'NNextClose']]
        df_earnings = df_earnings.set_index('EarningDate').fillna('NaN')
        df_earnings.to_csv(s_equity_path,date_format='%Y%m%d')

        # Logging
        if len(df_earnings) != 0:
            logging.info('\t' + s_equity + '\tdownloaded successfully')
        else:
            logging.info('\t' + s_equity + '\tmissing history')
        
        return True

    except:
        logging.debug('\t' + s_equity + '\tfailed to download')
        print s_equity, '\t', sys.exc_info()[0]
        return False

def get_busystock_earnings(date=None, use_cache=True):
    '''
    @summary: create earnings history for each equity in SPR index according to given date
    @datasource: www.busystock.com
    @param date: date to run on
    @param use_cache: True if using previous run data, False to re-run all the stuffs
    @return: None, save all earnings file in database
    '''
    # Initialization
    if date == None:
        date = dt.date.today()

    # Logging
    logging.basicConfig(filename=make_dir(date)+'/DEBUG.log',level=logging.DEBUG)

    # Derive list of SPR symbols
    ls_spr = get_index_symbols(date, 'spr', use_cache).index.values
    
    # Multiprocessing 
    p = Pool(100)
    ls_signals = p.map(get_stock_earnings, ls_spr)

    #ls_signals = [get_stock_earnings(s_equity) for s_equity in ls_spr]

    # Logging
    ls_failed = [ls_spr[i] for i, b_success in enumerate(ls_signals) if b_success==False]
    logging.debug('\tFailed List:\t' + '\t'.join(ls_failed))


if __name__ == '__main__':
    #get_busystock_earnings()
    get_stock_earnings('NWSA')
