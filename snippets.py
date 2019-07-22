




def resample( data ):
    dat       = data.resample( rule='1min', how='mean').dropna()
    dat.index = dat.index.tz_localize('UTC').tz_convert('US/Eastern')
    dat       = dat.fillna(method='ffill')
    return dat
#--------------------------------------------------------------------------------------------------------




dome_sign = 'cu'
dome_expire_year = ['17', '18']
dome_expire_month = ['01', '02', '03', '04', '05','06', '07', '08', '09', '10', '11', '12']
dome_all_symbols = [dome_sign + x + y for x in dome_expire_year for y in dome_expire_month]

fore_sign = 'HG'
fore_expire_year = ['7', '8']
fore_expire_month = ['F', 'G', 'H', 'J', 'K', 'M','N', 'Q', 'U', 'V', 'X', 'Z']
fore_all_symbols = [fore_sign + x + y for x in fore_expire_year for y in fore_expire_month]

symbol_pairs = zip(dome_all_symbols, fore_all_symbols)

#--------------------------------------------------------------------------------------------------------

future_types = ['m']
expiry_years = ['17', '18']
expiry_months = ['01', '03', '05', '07', '08', '09', '11', '12']
strike_prices = range(2000, 3500, 50)
option_types = ['C', 'P']

future_symbols = [(x + y + z) for x in future_types for y in expiry_years for z in expiry_months]
option_symbols = [(x + '-' + y + '-' + str(z)) for x in future_symbols for y in option_types for z in strike_prices]
call_symbols = [x for x in option_symbols if 'C' in x]
put_symbols = [x for x in option_symbols if 'P' in x]

all_symbols = future_symbols + option_symbols
all_symbols.sort()

#--------------------------------------------------------------------------------------------------------

CURRENCIES = ['eur', 'gbp', 'aud', 'nzd', 'usd', 'cad', 'chf', 'jpy']
def pairs():
    return list(itertools.combinations(CURRENCIES, 2))

#--------------------------------------------------------------------------------------------------------
import re
from ftplib import FTP
from io import StringIO

def get_nasdaq_stocks(filename, column):
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('SymbolDirectory')
    lines = StringIO()
    ftp.retrlines('RETR '+filename, lambda x: lines.write(str(x)+'\n'))
    ftp.quit()
    lines.seek(0)
    result = [l.split('|')[column] for l in lines.readlines()]
    return [l for l in result if re.match(r'^[A-Z]+$', l)]

#--------------------------------------------------------------------------------------------------------

import quandl
import os

quandl.ApiConfig.api_key = os.environ["QUANDL_API_KEY"]
nse = quandl.Database('NSE')

# retrieve first page of 100 stocks in NSE

nse_stocks_page = nse.datasets()
pageCount = 1

# restricting the pageCount not to exceed daily call limit

while nse_stocks_page.has_more_results() and pageCount < 7:
    for nse_stock in nse_stocks_page:
        print("{0}\t\t{1}".format(nse_stock.code, nse_stock.name))

    pageCount = pageCount + 1
    nse_stocks_page = nse.datasets(params = {"page":pageCount})

#------------------------------------------------------------------------------------------------------------------------------

# uses Yahoo Finance's API to get minute-by-minute ticks

import requests
import pandas as pd
import arrow
import datetime
import pandas as pd
import numpy as np


def get_quote_data(symbol, data_range, data_interval):
    res = requests.get(
        'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={data_range}&interval={data_interval}'.format(
            **locals()))
    data = res.json()
    body = data['chart']['result'][0]
    dt = datetime.datetime
    dt = pd.Series(map(lambda x: arrow.get(x).to('EST').datetime.replace(tzinfo=None), body['timestamp']), name='dt')
    df = pd.DataFrame(body['indicators']['quote'][0], index=dt)
    dg = pd.DataFrame(body['timestamp'])
    df = df.loc[:, ('close', 'volume')]
    df.dropna(inplace=True)  # removing NaN rows
    df.columns = ['CLOSE', 'VOLUME']  # Renaming columns in pandas
    df.to_csv('out.csv')

    return df


data = get_quote_data('F', '5d', '1m')
print(data)
    
    

#------------------------------------------------------------------------------------------------------------------------------

from io import StringIO
import os
import requests
import pandas as pd
from util import download_from_url

income_statement_url = 'https://stockrow.com/api/companies/{}/financials.xlsx?dimension=MRY&section=Income%20Statement&sort=desc'
balance_sheet_url = 'https://stockrow.com/api/companies/{}/financials.xlsx?dimension=MRY&section=Balance%20Sheet&sort=desc'
cash_flow_url = 'https://stockrow.com/api/companies/{}/financials.xlsx?dimension=MRY&section=Cash%20Flow&sort=desc'

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
data_dir = os.path.join(parent_dir, 'data', 'sr')
if not os.path.isdir(data_dir):
    os.makedirs(data_dir)
    

def download_from_url(url, filename, overwrite=False):
    if not overwrite and os.path.isfile(filename):
        return filename
    request = requests.get(url)
    with open(filename, 'wb') as fp:
        for chunk in request.iter_content(chunk_size=1024):
            if chunk:
                fp.write(chunk)
return filename
    

def download_income_stmt(symbol, force=True):
    url = income_statement_url.format(symbol)
    filename = os.path.join(data_dir, symbol+'.income_stmt.xlsx')
    return download_from_url(url, filename, overwrite=force)

def download_balance_sheet(symbol, force=True):
    url = balance_sheet_url.format(symbol)
    filename = os.path.join(data_dir, symbol+'.balance_sheet.xlsx')
    return download_from_url(url, filename, overwrite=force)

def download_cash_flow(symbol, force=True):
    url = cash_flow_url.format(symbol)
    filename = os.path.join(data_dir, symbol+'.cash_flow.xlsx')
    return download_from_url(url, filename, overwrite=force)

class Stockrow(object):
    def __init__(self, symbol, force=False):
        self.income_stmt = pd.read_excel(download_income_stmt(symbol, force))
        self.balance_sheet = pd.read_excel(download_balance_sheet(symbol, force))
        self.cash_flow = pd.read_excel(download_cash_flow(symbol, force))

    def dump(self):
        print(self.income_stmt.head())

if __name__ == '__main__':
company = Stockrow('AAPL').dump()

#--------------------------------------------------------------------------------------------------------
import re

def resolve_value(value):
    """
    Convert "1k" to 1 000, "1m" to 1 000 000, etc.
    """
    if value is None:
        return None
    tens = dict(k=10e3, m=10e6, b=10e9, t=10e12)
    value = value.replace(',', '')
    match = re.match(r'(-?\d+\.?\d*)([kmbt]?)$', value, re.I)
    if not match:
        return None
    factor, exp = match.groups()
    if not exp:
        return float(factor)
     return int(float(factor)*tens[exp.lower()])
    
#--------------------------------------------------------------------------------------------------------

import pandas as pd
import pandas_datareader.data as web
import os

DATE_FORMAT = "%Y-%m-%d"
#symbols_list = ["FB","AAPL","NFLX","GOOG","BA","GS","BABA","TSLA"]
symbols_list = ["AAPL"]

def file_exists(fn):
    exists = os.path.isfile(fn)
    if exists:
        return 1
    else:
        return 0

def write_to_file(exists, fn, f):
    if exists:
        f1 = open(fn, "r")
        last_line = f1.readlines()[-1]
        f1.close()
        last = last_line.split(",")
        date = (datetime.datetime.strptime(last[0], DATE_FORMAT)).strftime(DATE_FORMAT)
        today = datetime.datetime.now().strftime(DATE_FORMAT)
        if date != today:
            with open(fn, 'a') as outFile:
                f.tail(1).to_csv(outFile, header=False)
    else:
        print("new file")
        f.to_csv(fn)

def get_daily_quote(ticker):
    today = datetime.datetime.now().strftime(DATE_FORMAT)
    f = web.DataReader([ticker], "yahoo", start=today)
    return f

def get_history_quotes(ticker):
    today = datetime.now().strftime(DATE_FORMAT)
    f = web.DataReader([ticker], "yahoo", start='2018-08-01', end=today)
    return f


    for ticker in symbols_list:
        fn = "./quotes/" + ticker + "_day.csv";
        if file_exists(fn):
            f = get_daily_quote(ticker)
            write_to_file(OLD, fn, f)
        else:
            f = get_history_quotes(ticker)
            write_to_file(NEW, fn, f)

#--------------------------------------------------------------------------------------------------------

def read_line_from_file(filename):
    """Load lines from csv file"""
    lines = []
    with open(filename, 'r') as f:
        for line in f:
            lines.append(line.rstrip())
    if len(lines) > 0:
        lines = lines[1:]
    return lines


#--------------------------------------------------------------------------------------------------------

"""
Momentum
Source: https://en.wikipedia.org/wiki/Momentum_(technical_analysis)
Params: 
    data: pandas DataFrame
	periods: period for calculating momentum
	close_col: the name of the CLOSE values column
    
Returns:
    copy of 'data' DataFrame with 'momentum' column added
"""
def momentum(data, periods=14, close_col='<CLOSE>'):
    data['momentum'] = 0.
    
    for index,row in data.iterrows():
        if index >= periods:
            prev_close = data.at[index-periods, close_col]
            val_perc = (row[close_col] - prev_close)/prev_close

            data.set_value(index, 'momentum', val_perc)

   return data

#--------------------------------------------------------------------------------------------------------

"""chained comparison with all kind of operators"""
a = 10
print(1 < a < 50)
print(10 == a < 20)


"""Concatenate long strings elegantly 
across line breaks in code"""

my_long_text = ("We are no longer the knights who say Ni! "
                "We are now the knights who say ekki-ekki-"
                "ekki-p'tang-zoom-boing-z'nourrwringmm!")


"""calling different functions with same arguments based on condition"""
def product(a, b):
    return a * b

def subtract(a, b):
    return a - b

b = True
print((product if b else subtract)(1, 1))


"""else gets called when for loop does not reach break statement"""
a = [1, 2, 3, 4, 5]
for el in a:
    if el == 0:
        break
else:
     print('did not break out of for loop')

d1 = {'a': 1}
d2 = {'b': 2}

d1.update(d2)
print(d1)


"""
Find Index of Min/Max Element.
"""

lst = [40, 10, 20, 30]


def minIndex(lst):
    return min(range(len(lst)), key=lst.__getitem__)  # use xrange if < 2.7


def maxIndex(lst):
    return max(range(len(lst)), key=lst.__getitem__)  # use xrange if < 2.7

print(minIndex(lst))
print(maxIndex(lst))


"""
Convert raw string integer inputs to integers
"""

str_input = "1 2 3 4 5 6"

print("### Input ###")
print(str_input)

int_input = map(int, str_input.split())

print("### Output ###")
print(list(int_input))


""" You can have an 'else' clause with try/except. 
    It gets excecuted if no exception is raised.
    This allows you to put less happy-path code in the 'try' block so you can be 
    more sure of where a caught exception came from."""

try:
    1 + 1
except TypeError:
    print("Oh no! An exception was raised.")
else:
    print("Oh good, no exceptions were raised.")



#--------------------------------------------------------------------------------------------------------


dctA = {'a': 1, 'b': 2, 'c': 3}
dctB = {'b': 4, 'c': 3, 'd': 6}

"""loop over dicts that share (some) keys in Python3"""
for ky in dctA.keys() & dctB.keys():
    print(ky)

"""loop over dicts that share (some) keys and values in Python3"""
for item in dctA.items() & dctB.items():
print(item)

#--------------------------------------------------------------------------------------------------------

