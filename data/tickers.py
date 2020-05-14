import pandas as pd
import BeautifulSoup as bs
import requests
import pickle
import os
import os.path
import datetime
import time


def promt_time_stamp():
    return str(datetime.datetime.fromtimestamp(time.time()).strftime('[%H:%M:%S] '))


def get_index_tickers(list_indexes=list(), load_all=False):
    tickers_all = []

    path = 'indexes/'
    if not os.path.exists(path):
        os.mkdir(path)

    if load_all:
        list_indexes = ['dowjones', 'sp500', 'dax', 'sptsxc', 'bovespa', 'ftse100', 'cac40', 'ibex35',
                        'eustoxx50', 'sensex', 'smi', 'straitstimes', 'rts', 'nikkei', 'ssec', 'hangseng',
                        'spasx200', 'mdax', 'sdax', 'tecdax']

    for index in list_indexes:
        tickers = []
        implemented = True

        if os.path.isfile(path + index + '.pic'):
            print(promt_time_stamp() + 'load ' + index + ' tickers from db ..')
            with open(path + index + '.pic', "rb") as input_file:
                for ticker in pickle.load(input_file):
                    tickers.append(ticker)

        elif index == 'dowjones':
            print(promt_time_stamp() + 'load dowjones tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')
            for ticker in r[1][2][1:].tolist():
                tickers.append(ticker)

        elif index == 'sp500':
            print(promt_time_stamp() + 'load sp500 tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
            for ticker in r[0][0][1:].tolist():
                tickers.append(ticker)

        elif index == 'dax':
            print(promt_time_stamp() + 'load dax tickers ..')
            r = pd.read_html('https://it.wikipedia.org/wiki/DAX_30')[1]
            for ticker in pd.DataFrame(r)[1][1:].tolist():
                tickers.append(ticker)

        elif index == 'sptsxc':
            print(promt_time_stamp() + 'load sptsxc tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index')
            for ticker in r[0][0][1:].tolist():
                tickers.append(ticker)

        elif index == 'bovespa':
            print(promt_time_stamp() + 'load bovespa tickers ..')
            r = pd.read_html('https://id.wikipedia.org/wiki/Indeks_Bovespa')
            for ticker in r[0][1][1:].tolist():
                tickers.append(ticker)

        elif index == 'ftse100':
            print(promt_time_stamp() + 'load ftse100 tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/FTSE_100_Index')
            for ticker in r[2][1][1:].tolist():
                tickers.append(ticker)

        elif index == 'cac40':
            print(promt_time_stamp() + 'load cac40 tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/CAC_40')
            for ticker in r[2][0][1:].tolist():
                tickers.append(ticker)

        elif index == 'ibex35':
            print(promt_time_stamp() + 'load ibex35 tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/IBEX_35')
            for ticker in r[1][1][1:].tolist():
                tickers.append(ticker)

        elif index == 'eustoxx50':
            print(promt_time_stamp() + '-eustoxx50 not implemented-')
            implemented = False

        elif index == 'sensex':
            print(promt_time_stamp() + '-sensex not implemented-')
            implemented = False

        elif index == 'smi':
            print(promt_time_stamp() + 'load smi tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/Swiss_Market_Index')
            for ticker in r[1][3][1:].tolist():
                tickers.append(ticker)

        elif index == 'straitstimes':
            print(promt_time_stamp() + 'load straitstimes tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/Straits_Times_Index')
            df = pd.DataFrame(r[1])
            df = df.drop(df.index[[32]])
            df = pd.DataFrame(df)[0][2:]
            for ticker in df.tolist():
                tickers.append(ticker)

        elif index == 'rts':
            print(promt_time_stamp() + 'load rts tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/RTS_Index')
            for ticker in r[0][1][1:].tolist():
                tickers.append(ticker)

        elif index == 'nikkei':
            print(promt_time_stamp() + 'load nikkei tickers ..')
            resp = requests.get('https://www.bloomberg.com/quote/NKY:IND/members')
            html = bs.BeautifulSoup(resp.text)
            html_list = str(html).split('security-summary__ticker')

            for h in html_list:
                try:
                    e = h.split(':JP">')[1].split(':JP</a>')[0]
                    tickers.append(e)
                except IndexError as e:
                    pass

        elif index == 'ssec':
            print(promt_time_stamp() + '-ssec not implemented-')
            implemented = False

        elif index == 'hangseng':
            print(promt_time_stamp() + 'load hangseng tickers ..')
            resp = requests.get('https://en.wikipedia.org/wiki/Hang_Seng_Index')
            html = bs.BeautifulSoup(resp.text)
            html_list = str(html).split('<a href="/wiki')

            for h in html_list:
                try:
                    s = h.split('<li>')[1]
                    x = int(s)
                    tickers.append(s)
                except (IndexError, ValueError) as e:
                    pass

        elif index == 'spasx200':
            print(promt_time_stamp() + 'load spasx200 tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/S%26P/ASX_200')
            for ticker in r[0][0][1:].tolist():
                tickers.append(ticker)

        elif index == 'mdax':
            print(promt_time_stamp() + 'load mdax tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/MDAX')
            for ticker in r[1][1][1:].tolist():
                tickers.append(ticker)

        elif index == 'sdax':
            print(promt_time_stamp() + 'load sdax tickers ..')
            r = pd.read_html('https://en.wikipedia.org/wiki/SDAX')
            for ticker in r[2][1][1:].tolist():
                tickers.append(ticker)

        elif index == 'tecdax':
            print(promt_time_stamp() + 'load tecdax tickers ..')
            r = pd.read_html('https://de.finance.yahoo.com/quote/%5ETECDAX/components?p=%5ETECDAX')
            for ticker in r[1]['Symbol'].tolist():
                tickers.append(ticker)

        if implemented:
            with open(path + index + '.pic', 'wb') as f:
                pickle.dump(tickers, f)

        for t in tickers:
            tickers_all.append(t)

    return tickers_all


get_index_tickers(load_all=True)

i = 0
items = []
for item in get_index_tickers(list_indexes=['sp500']):
    items.append(item)
    if i % 5 == 0:
        print(items)
        items = []
    i += 1


######################################################################################################################################

# SPY Holdings
# S&P 500



from time import sleep, strftime
from ftplib import FTP
from io import StringIO
import os
import pandas as pd
import urllib.request, urllib.parse, urllib.error
import xlrd # module for excel file reading


# Based on: https://github.com/sjev/trading-with-python/blob/master/lib/extra.py

# list of SPDRs ETFs is taken on 22.02.2020 from https://www.ssga.com/de/en_gb/intermediary/etfs/fund-finder

String_of_SPDR_ETF = ("SPPY%20GY, SYBD%20GY, SYBF%20GY, SYBQ%20GY, SYBK%20GY, SYBR%20GY, ZPRM%20GY, "
                    "ZPR1%20GY, SYB3%20GY, SYBW%20GY, SYB5%20GY, SYBV%20GY, SYBN%20GY, SPPX%20GY, SYBL%20GY, "
                    "SYB4%20GY, SPP3%20GY, SYB7%20GY, SPP7%20GY, SYBI%20GY, SPFA%20GY, SYBM%20GY, SPFD%20GY, "
                    "SYBA%20GY, SYBC%20GY, SYBB%20GY, SYBJ%20GY, GLAC%20SE, SPFE%20GY, SPFB%20GY, SYBZ%20GY, "
                    "SPFV%20GY, SPFU%20GY, SYBS%20GY, SYBU%20GY, SYB1%20GY, SYBY%20GY, SYBT%20GY, SYBG%20GY, "
                    "SPY2%20GY, SPYJ%20GY, ZPRL%20GY, ZPRP%20GY, SPYF%20GY, ZPRD%20GY, ZPR6%20GY, ZPR5%20GY, "
                    "SYBE%20GY, SPP1%20GY, SPYI%20GY, SPYY%20GY, SPYA%20GY, ZPRE%20GY, SPYX%20GY, SPYM%20GY, "
                    "STT%20FP, STR%20FP, STS%20FP, STN%20FP, STZ%20FP, STW%20FP, STQ%20FP, STP%20FP, SMC%20FP, "
                    "ZPRX%20GY, STK%20FP, ERO%20FP, STU%20FP, ZPRW%20GY, ZPDW%20GY, ZPDJ%20GY, ZPRV%20GY, "
                    "ZPRU%20GY, WTEL%20NA, WCOD%20NA, WCOS%20NA, WNRG%20NA, WFIN%20NA, WHEA%20NA, WIND%20NA, "
                    "WMAT%20NA, ZPRS%20GY, WTCH%20NA, SPPW%20GY, WUTI%20NA, ZPRI%20GY, ZPRR%20GY, SPY4%20GY, "
                    "SPPE%20GY, SPY1%20GY, SPY5%20GY, SPYV%20GY, SPYW%20GY, ZPRG%20GY, ZPRA%20GY, ZPDK%20GY, "
                    "ZPDD%20GY, ZPDS%20GY, SPPD%20GY, SPYD%20GY, ZPDE%20GY, ZPDF%20GY, ZPDH%20GY, ZPDI%20GY, "
                    "ZPDM%20GY, ZPDT%20GY, ZPDU%20GY, SPYG%20GY, ZPDX%20GY, GCVC%20SE, SPF1%20GY, ZPRC%20GY")
SPDR_ETF=String_of_SPDR_ETF.split(", ")
dataDir="D:\\Data\\tickers\\"
def State_ETFs_holdings_download(dataDir, SPDR):
    try:
        os.path.join(dataDir,SPDR[:4]+"_holdings.xls")
        print('saving to', dest)
        endpoint = "https://www.ssga.com/de/en_gb/intermediary/etfs/library-content/products/fund-data/etfs/emea/holdings-daily-emea-en-{symbol}.xlsx".format(symbol=SPDR)
        urllib.request.urlretrieve (endpoint,dest) # download xls file and save it to data directory
        wb = xlrd.open_workbook(dest) # open xls file, create a workbook
        sh = wb.sheet_by_index(0) # select first sheet
        '''
        data = {'name':[], 'symbol':[], 'weight':[],'sector':[]}
        for rowNr  in range(5,505): # cycle through the rows
            v = sh.row_values(rowNr) # get all row values
            data['name'].append(v[0])
            data['symbol'].append(v[1]) # symbol is in the second column, append it to the list
            data['weight'].append(float(v[2]))
            data['sector'].append(v[3])
        
        return  pd.DataFrame(data)    
        '''
    except:
        pass


for ETF in SPDR_ETF:
    start_time = time.clock()
    State_ETFs_holdings_download(dataDir, ETF)
    print('Done %s out of %s in %s seconds', i, len(SPDR_ETF), round(time.clock() - start_time, 2))
    print('sleeping.....')
    sleep(randint(45,60))

# -------------------------------------------------------------------------------------------------------------------------


def save_sp500_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)
        
    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(tickers,f)
        
    return tickers


import urllib.request as urllib
from bs4 import BeautifulSoup

def get_sp500_symbols():
    url = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    file_fetcher = urllib.build_opener()
    file_fetcher.addheaders = [('User-agent', 'Mozilla/5.0')]
    file_data = file_fetcher.open(url).read()
    wiki_soup = BeautifulSoup(file_data, "html.parser")
    symbol_table = wiki_soup.find(attrs={'class': 'wikitable sortable'})

    symbol_data_list = list()

    for symbol in symbol_table.find_all("tr"):
        symbol_data_content = dict()
        symbol_raw_data = symbol.find_all("td")
        td_count = 0
        for symbol_data in symbol_raw_data:
            if(td_count == 0):
                symbol_data_content['symbol'] = symbol_data.text
            elif(td_count == 1):
                symbol_data_content['company'] = symbol_data.text
            elif(td_count == 3):
                symbol_data_content['sector'] = symbol_data.text
            elif(td_count == 4):
                symbol_data_content['industry'] = symbol_data.text
            elif(td_count == 5):
                symbol_data_content['headquarters'] = symbol_data.text

            td_count += 1

        symbol_data_list.append(symbol_data_content)

    return symbol_data_list[1::]

# --------------------------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------------------------

def get_s_and_p_500():
    return pd.read_csv('https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv').set_index('Symbol', drop=True)



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



###############################################################################################

"""download to csv file ETF holdings

    Materials (XLB)
    Energy (XLE)
    Financials (XLF)
    Industrials (XLI)
    Technology (XLK)
    Staples (XLP)
    Utilities (XLU)
    Health care (XLV)
    Consumer discretionary (XLY)

"""

import pandas as pd

def get_holdings(spdr_ticker):
    
    url = f'http://www.sectorspdr.com/sectorspdr/IDCO.Client.Spdrs.Holdings/Export/ExportCsv?symbol={spdr_ticker}'
    df = pd.read_csv(url, skiprows=1).to_csv(f'{spdr_ticker}_holdings.csv', index=False)
        
    return df
    
    
if __name__ == "__main__":
    
    tickers = ['XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY']

    for t in tickers:
        get_holdings(t)


#################################################################################################


"""
Download the ticker list from NASDAQ and save as csv.
Output filename: ./input/tickerList.csv
"""
import csv
import sys

from urllib.request import urlopen

import numpy as np


def get_tickers(percent):
    """Keep the top percent market-cap companies."""
    assert isinstance(percent, int)

    file = open('./input/tickerList.csv', 'w')
    writer = csv.writer(file, delimiter=',')
    cap_stat, output = np.array([]), []
    for exchange in ["NASDAQ", "NYSE", "AMEX"]:
        url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange="
        repeat_times = 10 # repeat downloading in case of http error
        for _ in range(repeat_times):
            try:
                print("Downloading tickers from {}...".format(exchange))
                response = urlopen(url + exchange + '&render=download')
                content = response.read().decode('utf-8').split('\n')
                for num, line in enumerate(content):
                    line = line.strip().strip('"').split('","')
                    if num == 0 or len(line) != 9:
                        continue # filter unmatched format
                    # ticker, name, last_sale, market_cap, IPO_year, sector, industry
                    ticker, name, _, market_cap, _, _, _ = line[0:4] + line[5:8]
                    cap_stat = np.append(cap_stat, float(market_cap))
                    output.append([ticker, name.replace(',', '').replace('.', ''),
                                   exchange, market_cap])
                break
            except:
                continue

    for data in output:
        market_cap = float(data[3])
        if market_cap < np.percentile(cap_stat, 100 - percent):
            continue
        writer.writerow(data)



#################################################################################################


def getSpyHoldings(dataDir):
    ''' get SPY holdings from the net, uses temp data storage to save xls file '''

    import urllib.request, urllib.parse, urllib.error
    
    dest = os.path.join(dataDir,"spy_holdings.xls")
    
    if os.path.exists(dest):
        print('File found, skipping download')
    else:
        print('saving to', dest)
        urllib.request.urlretrieve ("https://www.spdrs.com/site-content/xls/SPY_All_Holdings.xls?fund=SPY&docname=All+Holdings&onyx_code1=1286&onyx_code2=1700",
                             dest) # download xls file and save it to data directory
        
    # parse
    import xlrd # module for excel file reading
    wb = xlrd.open_workbook(dest) # open xls file, create a workbook
    sh = wb.sheet_by_index(0) # select first sheet
    
    
    data = {'name':[], 'symbol':[], 'weight':[],'sector':[]}
    for rowNr  in range(5,505): # cycle through the rows
        v = sh.row_values(rowNr) # get all row values
        data['name'].append(v[0])
        data['symbol'].append(v[1]) # symbol is in the second column, append it to the list
        data['weight'].append(float(v[2]))
        data['sector'].append(v[3])
      
    return  pd.DataFrame(data)    