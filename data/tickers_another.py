# Get NASDAQ-listed and not-NASDAQ-listed tickers
# Source: https://github.com/alexanu/atpy/blob/master/atpy/data/util.py


from ftplib import FTP
from io import StringIO

import pandas as pd


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
    result = result.loc[(result['Financial Status'] == 'N') & (result['Test Issue'] == 'N')]

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


def get_s_and_p_500():
    return pd.read_csv('https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv').set_index('Symbol', drop=True)
