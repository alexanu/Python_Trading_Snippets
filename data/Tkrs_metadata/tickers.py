
######################################################################################################################################

# Main US indexes constitutes + historical constits

    import pandas as pd
    import finnhub
    token='xxxx'
    hub = finnhub.Client(api_key=token)

    Indexes =['^GSPC', '^NDX', '^DJI']
    hub.indices_const(symbol=Indexes[2])
    All_constitu = set([item for sublist in [hub.indices_const(symbol=x)['constituents'] for x in ['^GSPC', '^NDX', '^DJI']] for item in sublist])

    pd.concat((pd.json_normalize(hub.indices_hist_const(symbol=x)['historicalConstituents']).assign(Index=x) for x in ['^GSPC', '^NDX', '^DJI']), ignore_index=True).to_csv("Indices_add_remove.csv")

# --------------------------------------------------------------------------------------------------------
# SP500 historical constitutes

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from datetime import datetime, timedelta
    import json

    def get_constituents():
        # request page
        html = requests.get("https://www.ishares.com/us/products/239726/#tabsAll").content
        soup = BeautifulSoup(html)

        # find available dates
        holdings = soup.find("div", {"id": "holdings"})
        dates_div = holdings.find_all("div", "component-date-list")[1]
        dates_div.find_all("option")
        dates = [option.attrs["value"] for option in dates_div.find_all("option")]

        # download constituents for each date
        constituents = pd.Series()
        for date in dates:
            resp = requests.get(
                f"https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?tab=all&fileType=json&asOfDate={date}"
            ).content[3:]
            tickers = json.loads(resp)
            tickers = [(arr[0], arr[1]) for arr in tickers["aaData"]]
            date = datetime.strptime(date, "%Y%m%d")
            constituents[date] = tickers

        constituents = constituents.iloc[::-1]  # reverse into cronlogical order
        return constituents

    constituents = get_constituents()["2013-02-28":"2018-02-28"]
    for i in range(0, len(constituents) - 1):
        start = str(constituents.index[i].date())
        end = str((constituents.index[i + 1].to_pydatetime() - timedelta(days=1)).date()    )
        for company in constituents[i]:
            ...


### All tickers from main exchanges  --------------------------------------        

    import pandas as pd
    import finnhub
    token='XXXX'
    hub = finnhub.Client(api_key=token)

    FinnHub_code_main_X =[US','PA','T','DE','F','L','PA','SW','TO','SZ']
    MAIN_STOCKS = pd.concat((pd.json_normalize(hub.stock_symbols(exch)).assign(source = exch) # pull all available symbols ...
                            for exch in FinnHub_code_main_X), ignore_index = True) # ... for every main stock exhange
    MAIN_STOCKS.to_csv("All_tickers_from_main_X.csv")
    STOCKS=MAIN_STOCKS.loc[MAIN_STOCKS['type'].isin(['EQS','DR'])]


###------------------------------------------------------------------------------------------------------
# Get NASDAQ-listed and not-NASDAQ-listed tickers
# Source: https://github.com/alexanu/atpy/blob/master/atpy/data/util.py


def _get_nasdaq_symbol_file(filename):
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('symboldirectory')

    class Reader:
        def __init__(self):
            self.data = ""

        def __call__(self, s):
            self.data += s.decode('ascii')

    r = Reader()

    ftp.retrbinary('RETR ' + filename, r)
    return pd.read_csv(StringIO(r.data), sep="|")[:-1]


def get_nasdaq_listed_companies():
    result = _get_nasdaq_symbol_file('nasdaqlisted.txt')
    result = result.loc[(result['Financial Status'] == 'N') 
                        & (result['Test Issue'] == 'N')]
    include_only = set()
    include_only_index = list()
    for i in range(result.shape[0]):
        s = result.iloc[i]
        if len(s['Symbol']) < 5 or s['Symbol'][:4] not in include_only:
            include_only_index.append(True)
            include_only.add(s['Symbol'])
        else:
            include_only_index.append(False)

    return result[include_only_index]


def get_non_nasdaq_listed_companies():
    result = _get_nasdaq_symbol_file('otherlisted.txt')
    result = result[result['Test Issue'] == 'N']
    return result


def get_us_listed_companies():
    nd = get_nasdaq_listed_companies()
    non_nd = get_non_nasdaq_listed_companies()
    symbols = list(set(list(non_nd[0]) + list(nd[0])))
    symbols.sort()

    return pd.DataFrame(symbols)



###########################################################################

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



#########################################################################################

# IQFeed tickers
# Source: https://github.com/alexanu/atpy/blob/master/atpy/data/iqfeed/util.py

import logging
import zipfile
import os
import tempfile
import requests


with tempfile.TemporaryDirectory() as td:
    with tempfile.TemporaryFile() as tf:
        logging.getLogger(__name__).info("Downloading symbol list... ")
        tf.write(requests.get('http://www.dtniq.com/product/mktsymbols_v2.zip', allow_redirects=True).content)
        zipfile.ZipFile(tf).extractall(td)

    with open(os.path.join(td, 'mktsymbols_v2.txt')) as f:
        content = f.readlines()

flt = {'SECURITY TYPE': 'EQUITY', 'EXCHANGE': {'NYSE', 'NASDAQ'}}
cols = content[0].split('\t') # column names

IQF_all = pd.DataFrame([x.split('\t') for x in content[1:]], columns=cols)
IQF_all.head()
# Show unique values ...



positions = {cols.index(k): v if isinstance(v, set) else {v} for k, v in flt.items()}
result = dict()
for c in content[1:]:
    split = c.split('\t')
    if all([split[col] in positions[col] for col in positions]):
        result[split[0]] = {cols[i]: split[i] for i in range(1, len(cols))}



#########################################################################################

# 'NYSE','AMEX','NASDAQ' symbols from nasdaq.com
# Source https://github.com/skillachie/finsymbols

import urllib.request as urllib
import re

exchange = ['NYSE','AMEX','NASDAQ']
url = ("http://www.nasdaq.com/screening/companies-by-industry.aspx?"
            "exchange={}&render=download".format(exchange))
file_fetcher = urllib.build_opener()
file_fetcher.addheaders = [('User-agent', 'Mozilla/5.0')]
file_data = file_fetcher.open(url).read()
if isinstance(file_data, bytes):  # Python3
    return file_data.decode("utf-8")

csv_file = exchange_name + '.csv'

symbol_list = list()
symbol_data = re.split("\r?\n", file_data)

headers = file_data[0] # symbol,company,sector,industry,headquaters
symbol_data = list(csv.reader(file_data, delimiter=','))
for row in symbol_data[1:-1]: # cut off the the last row because it is a null string
    symbol_data_dict = dict()
    symbol_data_dict['symbol'] = row[0]
    symbol_data_dict['company'] = row[1]
    symbol_data_dict['sector'] = row[6]
    symbol_data_dict['industry'] = row[7]
    
    symbol_list.append(symbol_data_dict)

##############################################################################################

nasdaq_request = requests.get("https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download" )
nasdaq_dataframe = nasdaq_request.content %>% .decode("utf-8") %>% io.StringIO() %>% pandas.read_csv()
nyse_request = requests.get("https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download" )
nyse_dataframe = nyse_request.content %>% .decode("utf-8") %>% io.StringIO() %>% pandas.read_csv()
amex_request = requests.get("https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download" )
amex_dataframe = amex_request.content %>% .decode("utf-8") %>% io.StringIO() %>% pandas.read_csv()

dataframe = nyse_dataframe.append(nasdaq_dataframe, ignore_index=True)
dataframe = dataframe.append(amex_dataframe, ignore_index=True)
dataframe = dataframe.drop(columns=["Summary Quote", "Unnamed: 8"])



######################################################################

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


############################################################################
