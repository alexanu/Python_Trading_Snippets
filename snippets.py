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


def data_array_merge(data_array): # merge all dfs into one dfs    
    merged_df = functools.reduce(lambda left,right: pd.merge(left,right,on='Date'), data_array)
    merged_df.set_index('Date', inplace=True)
    return merged_df

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


def create_directories(self):
        main_directory = "PairsResults"+self.params
        
        if not os.path.exists(main_directory):
            os.makedirs(main_directory)
        if not os.path.exists(self.directory_pair):
            os.makedirs(self.directory_pair)		
	
	
#-----------------------------------------------------------------------------------------------------------------------	

if os.path.exists('{}'.format(path)):
	response = input('A database with that path already exists. Are you sure you want to proceed? [Y/N] ')
	if response == 'Y':
		for item in os.listdir('{}/trades/'.format(path)):
			os.remove('{}/trades/{}'.format(path, item))
		os.rmdir('{}/trades/'.format(path))
		for item in os.listdir('{}'.format(path)):
			os.remove('{}/{}'.format(path, item))
		os.rmdir('{}'.format(path))
print('Creating a new database in directory: {}/'.format(path))
self.trades_path = '{}/trades/'.format(path)
os.makedirs(path)
os.makedirs(self.trades_path)
for name in names:
	with open(self.trades_path + 'trades_{}.txt'.format(name), 'w') as trades_file:
		trades_file.write('sec,nano,name,side,shares,price\n')
			
			
#-----------------------------------------------------------------------------------------------------------------------

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

'''divide text (csv or ...) to small files with defined number of lines'''
import os

def splitter(name, parts = 100000):
    # make dir for files
    if not os.path.exists(name.split('.')[0]):
        os.makedirs(name.split('.')[0])
    f = open(name, 'r', errors = 'ignore')
    lines = f.readlines()
    f.close()
    i = 0
    while i < len(lines):
        for item in lines[i:i+parts]:
            f2 = open(name.split('.')[0]+ '/'name.split('.')[0]+ str(i)+'.txt', 'a+', errors = 'ignore') 
            f2.write(item)
            f2.close()
    i += parts

#--------------------------------------------------------------------------------------------------------
''' Seperates dataframe into multiple by treatment
E.g. if treatment is 'gender' with possible values 1 (male) or 2 (female) 
the function returns a list of two frames (one with all males the other with all females) '''

def seperated_dataframes(df, treatment):
    treat_col = data[treatment] # col with the treatment
    dframes_sep = [] # list to hold seperated dataframes 
    for cat in categories(treat_col): # Go through all categories of the treatment
         for the treatmet into a new dataframe
        df = data[treat_col == cat] # select all rows that match the category        
        dframes_sep.append(df) # append the selected dataframe
    return dframes_sep


#--------------------------------------------------------------------------------------------------------
'''
Short function using Pandas to export data from MongoDB to excel
'''
import pandas as pd
from pymongo import MongoClient

# Connectio URI can be in shape mongodb://<username>:<password>@<ip>:<port>/<authenticationDatabase>')
client = MongoClient('mongodb://localhost')

def export_to_excel(name, collection, database):
    '''
    save collection from MongoDB as .xlsx file, name of file is argument of function 
    collection <string> is name of collection 
    database <string> is name of database
    '''
    data = list(client[database][collection].find({},{'_id':0}))
    df =  pd.DataFrame(data)
    #writer = pd.ExcelWriter('{}.xlsx'.format(name), engine='xlsxwriter')
    df.to_excel('{}.xlsx'.format(name)') #writer, sheet_name='Sheet1')
    #writer.save()


#--------------------------------------------------------------------------------------------------------

# download the zip file and saved it to our computer
import requests
url = "https://www.ssa.gov/oact/babynames/names.zip"
with requests.get(url) as response:
    with open("names.zip", "wb") as temp_file:
        temp_file.write(response.content)

data_list = [["year", "name", "gender", "count"]] # 2-dimensional Array (list of lists)

with ZipFile("names.zip") as temp_zip: # open the zip file into memory
    for file_name in temp_zip.namelist(): # Then we read the file list.
        if ".txt" in file_name: # We will only process .txt files.
            with temp_zip.open(file_name) as temp_file: # read the current file from the zip file.
                # The file is opened as binary, we decode it using utf-8 so it can be manipulated as a string.
                for line in temp_file.read().decode("utf-8").splitlines():
                    line_chunks = line.split(",")
                    year = file_name[3:7]
                    name = line_chunks[0]
                    gender = line_chunks[1]
                    count = line_chunks[2]

                    data_list.append([year, name, gender, count])

csv.writer(open("data.csv", "w", newline="", # We save the data list into a csv file.
                encoding="utf-8")).writerows(data_list)
                # I prefer to use writerows() instead of writerow() ...
                # ...since it is faster as it does it in bulk instead of one row at a time.



#--------------------------------------------------------------------------------------------------------
'''
Short function using Pandas to export data from MongoDB to excel
'''
import pandas as pd
from pymongo import MongoClient

# Connectio URI can be in shape mongodb://<username>:<password>@<ip>:<port>/<authenticationDatabase>')
client = MongoClient('mongodb://localhost')

def export_to_excel(name, collection, database):
    '''
    save collection from MongoDB as .xlsx file, name of file is argument of function 
    collection <string> is name of collection 
    database <string> is name of database
    '''
    data = list(client[database][collection].find({},{'_id':0}))
    df =  pd.DataFrame(data)
    #writer = pd.ExcelWriter('{}.xlsx'.format(name), engine='xlsxwriter')
    df.to_excel('{}.xlsx'.format(name)') #writer, sheet_name='Sheet1')
    #writer.save()


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

# chained comparison with all kind of operators
a = 10
print(1 < a < 50)
print(10 == a < 20)


#--------------------------------------------------------------------------------------------------------
# Concatenate long strings elegantly across line breaks in code

my_long_text = ("We are no longer the knights who say Ni! "
                "We are now the knights who say ekki-ekki-"
                "ekki-p'tang-zoom-boing-z'nourrwringmm!")


#--------------------------------------------------------------------------------------------------------		
# extract US numbers from text
import re
numbers = ' 123.456.7889 (123)-456-7888 (425) 465-7523 456 123-7891 111 111.1111 (222)333-4444 666 777 8888 987-654-4321'
res = re.findall(r'\d{3}\)*?\-*?\s*?\.*?\d{3}\-*?\s*?\.*?\d{3}', numbers)		


#--------------------------------------------------------------------------------------------------------
# clean spaces in strings
		
import re
def clean(string): # cleans string from empty spaces on the start and on the end
    # input is : "   Hello World    " and output is "Hello World"
		
    first = 0
    for item in string:
        if item != ' ':
            first = string.index(item)
            break
    string = string[::-1]
    last = 0
    for item in string:
        if item != ' ':
            last = string.index(item)
            break
    return string[::-1][first:len(string)-last]
    
def clean2(string): # the same purpose as above
    nonempty = [string.index(item) for item in string if item != ' ']
    nonempty2 = [string[::-1].index(item) for item in string[::-1] if item != ' ']
    return string[nonempty[0]:len(string) -nonempty2[0]]

def clean3(string): # clean string from rebundant spaces
    try:
        for item in re.findall('[" "]{2,}',string):
            string = string.replace(item, ' ')
            if string[0] == ' ':
                string = string[1:]
            if string[-1] == ' ':
                string = string[:-1]
        return string
    except:
return string
		
		
		
#--------------------------------------------------------------------------------------------------------
# calling different functions with same arguments based on condition

def product(a, b):
    return a * b

def subtract(a, b):
    return a - b

b = True
print((product if b else subtract)(1, 1))



#--------------------------------------------------------------------------------------------------------
# else gets called when for loop does not reach break statement
a = [1, 2, 3, 4, 5]
for el in a:
    if el == 0:
        break
else:
     print('did not break out of for loop')


#--------------------------------------------------------------------------------------------------------
d1 = {'a': 1}
d2 = {'b': 2}

d1.update(d2)
print(d1)


#--------------------------------------------------------------------------------------------------------
# Find Index of Min Element
lst = [40, 10, 20, 30]
min(range(len(lst)), key=lst.__getitem__)


#--------------------------------------------------------------------------------------------------------
# Convert raw string integer inputs to integers

str_input = "1 2 3 4 5 6"
int_input = map(int, str_input.split())
print(list(int_input))

#--------------------------------------------------------------------------------------------------------

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

''' use threading module for paralel running of some function '''

import time
from threading import Thread

def no_arg(func, instances): # func is function withOUT arguments
    for i in range(instances): # number of threads equals instances
        t = Thread(target=func)
        t.start()

def with_arg(func, instances,args): # func is function with arguments
    for i in range(instances): # number of threads equals instances
        t = Thread(target=func, args = args) # arguments in tuple
	t.start()

		
#--------------------------------------------------------------------------------------------------------		
# Parallel programming with Pool

# Importing the multiprocessing 
from multiprocessing import Pool

# function to which we'll perform multi-processing
def cube(i):
    i = i+1
    z = i**3
    return z

# using pool class to map the function with iterable arguments-
print(Pool().map(cube, [1, 2, 3]))
		

