# https://github.com/ranaroussi/yfinance

# I had big problems with installation of yfinance to conda
# here is what helped: https://github.com/ranaroussi/yfinance/issues/92#issuecomment-785331111
import yfinance as yf

import finnhub
token='br4gvpvrh5r8ufeot14g'
hub = finnhub.Client(api_key=token)

import fmpsdk
fmp_key="d0e821d6fc75c551faef9d5c495136cc"


import os
import pandas as pd
import datetime as dt
import time


msft = yf.Ticker("MSFT")

hist = msft.history(period="max")
data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30")


data = yf.download(
        tickers = "SPY AAPL MSFT",
        # period = "5d", # 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        start="2021-02-01", end="2021-02-05",
        interval = "1m", # 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo, intraday if period < 60 days
        group_by = 'ticker', # by ticker (to access via data['SPY']), default is 'column'
        auto_adjust = True, # adjust all OHLC automatically, default is False
        prepost = True # download pre/post regular market hours data
    )


'''
XHB - SPDR Homebuilders ETF
XLB - Materials Select Sector SPDR Fund
XLE - Energy Select Sector SPDR Fund
XLF - Financial Select Sector SPDR Fund
XLI - Industrial Select Sector SPDR Fund
XLK - Technology Select Sector SPDR Fund
XLP - Consumer Staples Select Sector SPDR Fund
XLU - Utilities Select Sector SPDR Fund
XLV - Health Care Select Sector SPDR Fund
XLY - Consumer Discretionary Select Sector SPDR Fund
XME - SPDR S&P Metals & Mining ETF
XOP - SPDR S&P Oil & Gas Exploration & Production ETF
'''


Spiders = yf.download(
        tickers = "XHB, XLB, XLE, XLF, XLI, XLK, XLP, XLU, XLV, XLY, XME, XOP",
        period = "max", # 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # start="2021-02-01", end="2021-02-05",
        interval = "1d", # 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo, intraday if period < 60 days
        group_by = 'ticker', # by ticker (to access via data['SPY']), default is 'column'
        auto_adjust = True, # adjust all OHLC automatically, default is False
        prepost = True # download pre/post regular market hours data
    )
[Spiders[x].to_csv(x +".csv") for x in ['XHB', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'XME', 'XOP']]



pd.json_normalize(fmpsdk.historical_survivorship_bias_free_eod(apikey=fmp_key, symbol="AAPL",date='2020:07:22')) #Prem


Financials = pd.json_normalize(hub.etfs_holdings(symbol='XLF')['holdings'])
data = yf.download(
        tickers = ' '.join(Financials.symbol.to_list()),
        period = "1mo", # 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        interval = "5m", # 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo, intraday if period < 60 days
        group_by = 'ticker', # by ticker (to access via data['SPY']), default is 'column'
        auto_adjust = True, # adjust all OHLC automatically, default is False
        prepost = True # download pre/post regular market hours data
    )




# All daily prices from yahoo for all current index members

    # Use finnhub to get current index members
    import finnhub
    token='br4gvpvrh5r8ufeot14g'
    hub = finnhub.Client(api_key=token)
    All_constitu = set([item for sublist in [hub.indices_const(symbol=x)['constituents'] for x in ['^GSPC', '^NDX', '^DJI']] for item in sublist])

    def chunkIt(seq, num): # util to split long list into many small
        avg = len(seq) / float(num)
        out = []
        last = 0.0

        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out

    All_constitu_split = chunkIt(list(All_constitu), 50) # Split all tickers into 50 groups

    def bulk_down(tickers_set): #dowlload daily quotes for a group and save each ticker as csv
        daily_Quotes = yf.download(tickers = ' '.join(tickers_set),
                                    period = "max", 
                                    interval = "1d", 
                                    group_by = 'ticker', auto_adjust = True, prepost = True)
        [daily_Quotes[x].to_csv(x +".csv") for x in tickers_set]

    [bulk_down(x) for x in All_constitu_split] # for each small group do bulk_down


# Downloading 1 month of minute data from yahoo for all index members

    import datetime as dt

    # Use finnhub to get current index members
    import finnhub
    token='br4gvpvrh5r8ufeot14g'
    hub = finnhub.Client(api_key=token)
    All_constitu = set([item for sublist in [hub.indices_const(symbol=x)['constituents'] for x in ['^GSPC', '^NDX', '^DJI']] for item in sublist])

    def chunkIt(seq, num): # util to split long list into many small
        avg = len(seq) / float(num)
        out = []
        last = 0.0
        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg
        return out

    All_constitu_split = chunkIt(list(All_constitu), 50) # Split all tickers into 50 groups

    def bulk_down_minute(tickers_set): #dowlload daily quotes for a group and save each ticker as csv
        minute_quotes = pd.concat((yf.download(tickers = ' '.join(tickers_set),
                                                start=(dt.datetime.now() - dt.timedelta(days=5*y)).strftime("%Y-%m-%d"), 
                                                end=(dt.datetime.now() - dt.timedelta(days=5*x)).strftime("%Y-%m-%d"), 
                                                interval = "1m", 
                                                group_by = 'ticker', auto_adjust = True, prepost = True)
                                    for x,y in zip([0,1,2,3,4,5],[1,2,3,4,5,6]))) # for 30 days time period
        [minute_quotes[x].to_csv(x +".csv") for x in tickers_set]

    [bulk_down_minute(x) for x in All_constitu_split] # for each small group do bulk_down


# Yahoo allows only last month of 1m quotes
# Let's try with Finnhub

    # Candles (from 1 minute)
        ''' def stock_candles(self, symbol, resolution, _from, to, **kwargs):
                params = self._merge_two_dicts({
                    "symbol": symbol,
                    "resolution": resolution,
                    "from": _from,
                    "to": to
                }, kwargs)

                return self._get("/stock/candle", params=params) '''
        supported_resol = [1, 5, 15, 30, 60, 'D', 'W', 'M']

    import calendar
    
    def bulk_down_minute(ticker, yea, mont):
        _, num_days = calendar.monthrange(yea, mont)
        first_day = int(dt.datetime(yea, mont, 1).timestamp())
        last_day = int(dt.datetime(yea, mont, num_days).timestamp())
        all_candles = hub.stock_candles(ticker, '1', first_day, last_day)
        d = {'timestamp': all_candles['t'], 
            'open': all_candles['o'], 'high': all_candles['h'], 'low': all_candles['l'],'close': all_candles['c'], 
            'volume': all_candles['v']}
        df = pd.DataFrame(data=d)
        df['timestamp'] = pd.to_datetime(df['timestamp'],unit='s') # this is UTC
        df.set_index('timestamp', inplace=True)
        df.to_csv(ticker+".csv", header=not pathlib.Path(ticker+".csv").exists(), mode='a' if pathlib.Path(ticker+".csv").exists() else 'w')
    
    [bulk_down_minute(x,2020,y) for x,y in zip(['AAPL','F'],range(1,13))] 
    
    pd.concat((bulk_down_minute('AAPL', 2020, mont) for mont in [1,2,3])).to_csv('AAPL.csv')


# Minute data from FMP. I bought 1 month access and download hell a lot

        fmp_key="d0e821d6fc75c551faef9d5c495136cc"

        # Example of endpoint (it was not implemented in fmpsdk package, I got it by twitting to fmp)
        # https://financialmodelingprep.com/api/v4/historical-price/UN/1/minute/2020-04-26/2020-05-26?apikey=d0e821d6fc75c551faef9d5c495136cc'

        import requests

        BASE_URL_v4="https://financialmodelingprep.com/api/v4/historical-price"
        time_delta: int = 1 # 1 minute

        # Symbols to proceed with:
        symbols=['VOO','VXUS','BND','BNDX','VUG','VO','VB','VTV','VXF','IEFA',
                'BSV','VIG','IEMG','VCIT','VCSH','BIV','VGT','VEU','VTIP','VYM',
                'USMV','VBR','VV','VBK','SCHX','IXUS','IGSB','VT','QUAL','SCHF',
                'VOE','VOT','VGK','SCHB','GOVT','JPST','VGSH','VMBS','SCHD','SCHP',
                'VHT','ACWI','DGRO','SCHG','MTUM','BLV','VGIT','IGIB','ESGU','VTEB',
                'EFAV','SCHA','FDN','GSLC','ARKK','MGK','FVD','SCHZ','TQQQ','SCHO',
                'VONG','SCHE','VLUE','SCHV','HYLB','SCHM','PGX','LMBS','VPL','VFH',
                'MCHI','VSS','USHY','VDC','NOBL','VCLT','FTCS','USIG','VGLT','VPU',
                'FLOT','ACWV','IUSB','FPE','BBJP','EXSA.DE','FTSM','HDV','SHYG',
                'ICSH','SKYY','ISTB','ESGE','AAXJ','FNDX','NEAR','DGRW','FTEC',
                'FIXD','VOOG','VCR','SCHH','FNDF','EEMV','BKLN','SUB','VONV','BBCA',
                'BBEU','VONE','SCHR','DBEF','MOAT','VIS','IDEV','SJNK','KWEB','ANGL',
                'AMLP','IEUR','TOTL','IAGG','ACWX','INDA','SPTS','IWY','ESGD','EMLC',
                'GSY','MGC','EDV','MGV','GBIL','ONEQ','FNDE','GUNR','FNDA','STIP',
                'QTEC','ARKW','ARKG','HYD','VOX','VIGI','VNLA','SGOL','VDE','PDBC',
                'FLRN','USSG','SPSM','SUSL','VTWO','SCHC','VAW','IGLB','DON','SPHD',
                'DLN','VIOO','VWOB','FXL','CIBR','HEFA','BSCM','BAB','BSCL','SLQD',
                'IVOO','FHLC','IQLT','FV','REET','HYLS','SPYD','ICLN','ASHR','BBIN',
                'FBT','RODM','NFRA','BOTZ','TAN','FNDC','PTLC','JETS','GSIE','FMB',
                'ITM','JKE','GEM','SRLN','EMLP','HEDJ','VTHR','FLCB','VYMI','JHMM',
                'SQQQ','IBDM','FBND','HACK','USMC','QLTA','BBAX','DEM','DXJ','FPX',
                'HYS','DGS','VRP','TDTT','AOR','BSCN','IBDN','VOOV','USFR','BSCK',
                'AMJ','QYLD','RDVY','AGGY','FXH','IBDO','ROBO','AOM','DES','FTSL',
                'QDF','BAR','DLS','FDL']


        for idx, symbol in enumerate(symbols):
            try:
                prof=fmpsdk.company_profile(apikey=fmp_key, symbol=symbol)
                data = []
                for x,y in zip(list(range(0,600)),list(range(1,601))):
                    start=(dt.datetime.now() - dt.timedelta(days=7*y)).strftime("%Y-%m-%d")
                    end=(dt.datetime.now() - dt.timedelta(days=7*x)).strftime("%Y-%m-%d")
                    url = f"{BASE_URL_v4}/{symbol}/{time_delta}/minute/{start}/{end}?apikey={fmp_key}"
                    try:
                        response = requests.get(url)
                        data.append(response.json()['results'])
                        print('Done with %s for %s.' % (symbol, start))
                    except:
                        #break
                        print('No data for %s for %s.' % (symbol, start))
                        pass
                flat_data = [item for sublist in data for item in sublist] # flatten the list
                return_var = pd.json_normalize(flat_data)
                return_var = return_var.drop_duplicates()
                return_var['datetime'] = pd.to_datetime(return_var['formated'])
                return_var.set_index('datetime',inplace=True) # "inplace" make the changes in the existing df
                return_var=return_var.drop(return_var.columns[[5,6]],axis=1) # delete last 2 columns
                return_var=return_var.rename(columns={'o':'Open','h':'High','l':'Low','c':'Close','v':'Volume'})
                return_var = return_var[['Open','High','Low','Close','Volume']] # Change order of columns
                return_var = return_var.sort_index()
                return_var.to_csv(symbol+'.txt')
                print('Done with %s. Still %i to go' % (symbol, len(symbols)-idx-1))
            except:
                pass



# Tick data from Finnhub
    ''' def stock_tick(self, symbol, date, limit, skip, _format='json', **kwargs):
            params = self._merge_two_dicts({
                "symbol": symbol,
                "date": date,
                "limit": limit,
                "skip": skip,
                "format": _format
            }, kwargs)
            return self._get("/stock/tick", params=params) '''
    hub.stock_tick('AAPL', date='2020-12-10',limit=20, skip=0)



# FX minute data from Finnhub
    All_FX = pd.concat((pd.json_normalize(hub.forex_symbols(exch)).assign(source = exch) # pull all available FX asset (FX pairs and CFDs) ...
                            for exch in hub.forex_exchanges()), ignore_index = True) # ... from every broker supported by FinnHub

        ''' def forex_candles(self, symbol, resolution, _from, to, _format='json'):
                    return self._get("/forex/candle", params={
                    "symbol": symbol,
                    "resolution": resolution,
                    "from": _from,
                    "to": to,
                    "format": _format
                }) '''

