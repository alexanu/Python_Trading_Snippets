
# List of ETFs from Finnhub

    import pandas as pd
    import finnhub
    token='XXXX'
    hub = finnhub.Client(api_key=token)

    FinnHub_code_main_X =[US','PA','T','DE','F','L','PA','SW','TO','SZ']
    MAIN_STOCKS = pd.concat((pd.json_normalize(hub.stock_symbols(exch)).assign(source = exch) # pull all available symbols ...
                            for exch in FinnHub_code_main_X), ignore_index = True) # ... for every main stock exhange
    ETFs=MAIN_STOCKS.loc[MAIN_STOCKS['type'].isin(['ETF','ETC'])]


####################################################################################
# List of ETFs from Yahoo

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


