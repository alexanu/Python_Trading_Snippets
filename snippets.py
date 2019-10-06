mask = df_results[pnl_col_name] > 0
all_winning_trades = df_results[pnl_col_name].loc[mask] 
   
#--------------------------------------------------------------------------------------------------------
	
for ticker in stocks: # for each ticker in our pair          
	mask = (stock_data['Date'] > start_date) & (stock_data['Date'] <= end_date) # filter our column based on a date range   
	stock_data = stock_data.loc[mask] # rebuild our dataframe
	stock_data = stock_data.reset_index(drop=True) # re-index the data        
	array_pd_dfs.append(stock_data) # append our df to our array


#--------------------------------------------------------------------------------------------------------

def fetch_last_day_mth(year_, conn):
    """
    return date of the last day of data we have for a given year in our Postgres DB. 
    conn: a Postgres DB connection object
    """  
    cur = conn.cursor()
    SQL =   """
            SELECT MAX(date_part('day', date_price)) FROM daily_data
            WHERE date_price BETWEEN '%s-12-01' AND '%s-12-31'
            """
    cur.execute(SQL, [year_,year_])        
    data = cur.fetchall()
    cur.close()
    last_day = int(data[0][0])
    return last_day

#--------------------------------------------------------------------------------------------------------

def pair_data_verifier(array_df_data, pair_tickers, threshold=10):
    """
    merge two dataframes, verify if we still have the same number of data we originally had.
    use an inputted threshold that tells us whether we've lost too much data in our merge or not.
    threshold: max number of days of data we can be missing after merging eshold
    """
    stock_1 = pair_tickers[0]
    stock_2 = pair_tickers[1]
    df_merged = pd.merge(array_df_data[0], array_df_data[1], left_on=['Date'], right_on=['Date'], how='inner')
    
    new_col_names = ['Date', stock_1, stock_2] 
    df_merged.columns = new_col_names
    # round columns
    df_merged[stock_1] = df_merged[stock_1].round(decimals = 2)
    df_merged[stock_2] = df_merged[stock_2].round(decimals = 2)
    
    new_size = len(df_merged.index)
    old_size_1 = len(array_df_data[0].index)
    old_size_2 = len(array_df_data[1].index)

    print("Pairs: {0} and {1}".format(stock_1, stock_2))
    print("New merged df size: {0}".format(new_size))
    print("{0} old size: {1}".format(stock_1, old_size_1))
    print("{0} old size: {1}".format(stock_2, old_size_2))

    if (old_size_1 - new_size) > threshold or (old_size_2 - new_size) > threshold:
        print("This pair {0} and {1} were missing data.".format(stock_1, stock_2))
        return False
    else:
        return df_merged


#--------------------------------------------------------------------------------------------------------


def resample( data ):
    dat       = data.resample( rule='1min', how='mean').dropna()
    dat.index = dat.index.tz_localize('UTC').tz_convert('US/Eastern')
    dat       = dat.fillna(method='ffill')
    return dat



#--------------------------------------------------------------------------------------------------------------------------
def PickRandomPair(self, pair_type):
       pairs = {
            'major': ['EUR_USD', 'USD_JPY', 'GBP_USD', 'USD_CAD', 'USD_CHF', 'AUD_USD', 'NZD_USD'],
            'minor': ['EUR_GBP', 'EUR_CHF', 'EUR_CAD', 'EUR_AUD', 'EUR_NZD', 'EUR_JPY', 'GBP_JPY', 'CHF_JPY', 'CAD_JPY', 'AUD_JPY', 'NZD_JPY', 'GBP_CHF', 'GBP_AUD', 'GBP_CAD'],
            'exotic': ['EUR_TRY', 'USD_SEK', 'USD_NOK', 'USD_DKK', 'USD_ZAR', 'USD_HKD', 'USD_SGD'],
            'all': ['EUR_USD', 'USD_JPY', 'GBP_USD', 'USD_CAD', 'USD_CHF', 'AUD_USD', 'NZD_USD', 'EUR_GBP', 'EUR_CHF', 'EUR_CAD', 'EUR_AUD', 'EUR_NZD', 'EUR_JPY', 'GBP_JPY', 'CHF_JPY', 'CAD_JPY', 'AUD_JPY', 'NZD_JPY', 'GBP_CHF', 'GBP_AUD', 'GBP_CAD', 'EUR_TRY', 'USD_SEK', 'USD_NOK', 'USD_DKK', 'USD_ZAR', 'USD_HKD', 'USD_SGD']
        }
       return pairs[pair_type][randint(0, len(pairs[pair_type]) - 1)]

#--------------------------------------------------------------------------------------------------------------------------

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

#----------------------------------------------------------------------------------------------------------------------------

CURRENCIES = ['eur', 'gbp', 'aud', 'nzd', 'usd', 'cad', 'chf', 'jpy']
def pairs():
    return list(itertools.combinations(CURRENCIES, 2))


#----------------------------------------------------------------------------------------------------------------------------

def make_instrument_triangles(self, instruments):

        # Making a list of all instrument pairs (based on quoted currency)
        first_instruments = []
        for instrument in itertools.combinations(instruments, 2):
            quote1 = instrument[0][4:]
            if quote1 in instrument[1]:
                first_instruments.append((instrument[0], instrument[1]))

        # Adding the final instrument to the pairs to convert currency back to starting ...
	# ... (based on the currency only in one pair and the starting currency)
        for instrument in first_instruments:
            currency1 = instrument[0][:3]
            currency2 = instrument[0][4:]
            currency3 = instrument[1][:3]
            currency4 = instrument[1][4:]
            currencies = [currency1, currency2]
            if currency3 not in currencies:
                currencies.append(currency3)
            elif currency4 not in currencies:
                currencies.append(currency4)
            for combo in itertools.combinations(currencies, 2):
                combo_str = combo[0] + '_' + combo[1]
                if combo_str not in instrument and combo_str in instruments:
                    instruments.append(
                        (instrument[0], instrument[1], combo_str))

     return instruments

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
# List of ETFs

import urllib2
import pandas

def ETF_from_YH():
    response = urllib2.urlopen('http://finance.yahoo.com/etf/lists/?mod_id=mediaquotesetf&tab=tab3&rcnt=50')
    the_page = response.read()
    splits = the_page.split('<a href=\\"\/q?s=')
    etf_symbols = [split.split('\\')[0] for split in splits[1:]]
    return etf_symbols

def get_ETFSymbols(source):
    if source.lower() == 'yahoo':
        return ETF_from_YH()
    elif source.lower() == 'nasdaq':
        return pandas.read_csv('http://www.nasdaq.com/investing/etfs/etf-finder-results.aspx?download=Yes')['Symbol'].values

#--------------------------------------------------------------------------------------------------------

import quandl
import os

quandl.ApiConfig.api_key = os.environ["QUANDL_API_KEY"]
nse = quandl.Database('NSE')

nse_stocks_page = nse.datasets()
pageCount = 1
while nse_stocks_page.has_more_results() and pageCount < 7: # restricting the pageCount not to exceed daily call limit
    for nse_stock in nse_stocks_page:
        print("{0}\t\t{1}".format(nse_stock.code, nse_stock.name))
    pageCount = pageCount + 1
    nse_stocks_page = nse.datasets(params = {"page":pageCount})

	
#------------------------------------------------------------------------------------------------------------------------------
	

import os	
import pandas.io.data as web
import datetime as dt
from apscheduler.schedulers.blocking import BlockingScheduler

def download_SPX(date=None):
    if date == None:
        date = dt.date.today()

    s_dir = os.getcwd() + '/' + date.strftime('%Y-%m-%d')
    if not os.path.exists(s_dir):
        os.makedirs(s_dir)

    start = dt.datetime(1990,1,1)
    end = date
    f = web.DataReader("^GSPC", 'yahoo', start, end).fillna('NaN')
    f.to_csv(s_dir+'/SPX.csv', date_format='%Y%m%d')	
	
def main():

    sched = BlockingScheduler()

    @sched.scheduled_job('cron', day_of_week='mon,tue,wed,thu,fri,sat', hour=23)
    def scheduled_job():
    	print('[INFO] Job started.')
        download_SPX()
        print('[INFO] Job ended.')

    sched.start()    

if __name__ == '__main__':
	main()
	
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

def momentum(data, periods=14, close_col='<CLOSE>'):
    data['momentum'] = 0.
    
    for index,row in data.iterrows():
        if index >= periods:
            prev_close = data.at[index-periods, close_col]
            val_perc = (row[close_col] - prev_close)/prev_close

            data.set_value(index, 'momentum', val_perc)

   return data

#--------------------------------------------------------------------------------------------------------