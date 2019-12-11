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