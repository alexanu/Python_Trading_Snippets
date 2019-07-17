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


