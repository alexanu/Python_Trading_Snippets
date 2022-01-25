
# https://github.com/Finnhub-Stock-API/finnhub-python
# pip install finnhub-python

import os
import pandas as pd
import datetime as dt
import time

from keys_config import *

import fmpsdk


import finnhub
hub = finnhub.Client(api_key=finnhub_key)



# https://github.com/ranaroussi/yfinance
# I had big problems with installation of yfinance to conda
# here is what helped: https://github.com/ranaroussi/yfinance/issues/92#issuecomment-785331111
import yfinance as yf


# Add https://github.com/iexcloud/pyEX

## Helpers -------------------------------------

    def _rename_quote(quotes):
        quotes['t'] = to_datetime(quotes['t'], unit='s', utc=True)
        return {names_dict[k]: v for k, v in quotes.items()}


    def _json_to_df_candle(
            _json
    ):
        df = DataFrame(_json)
        df = df.set_index(
            to_datetime(df['t'], unit='s', utc=True),
        )
        df = df.drop(['t', 's'], axis=1)
        df = _rename_candle_columns(df)
        df.index.name = 'datetime'
        return df


##########################################################################################

### Symbols from different FX / CFD providers / Crypto --------------------------------------

    hub.forex_exchanges()
    pd.json_normalize(hub.forex_symbols("fxcm"))

    All_FX = pd.concat((pd.json_normalize(hub.forex_symbols(exch)).assign(source = exch) # pull all available FX asset (FX pairs and CFDs) ...
                            for exch in hub.forex_exchanges()), ignore_index = True) # ... from every broker supported by FinnHub
    All_FX.to_csv("FX_tickers_FinnHub.csv")

    pd.json_normalize(fmpsdk.forex(apikey=fmp_key))
    pd.json_normalize(fmpsdk.forex_list(apikey=fmp_key)).transpose()
    pd.json_normalize(fmpsdk.available_forex(apikey=fmp_key))

    pd.json_normalize(fmpsdk.available_commodities(apikey=fmp_key))
    pd.json_normalize(fmpsdk.commodities_list(apikey=fmp_key))

    pd.json_normalize(fmpsdk.cryptocurrencies_list(apikey=fmp_key))
    pd.json_normalize(fmpsdk.available_cryptocurrencies(apikey=fmp_key))


### FX quotes pairs from different providers --------------------------------------

    EURUSD_quote=All_FX[All_FX["displaySymbol"] == "EUR/USD"]

    hub.quote(EURUSD_quote["symbol"].to_list()[1])['c']
    
    EURUSD = pd.concat((pd.json_normalize(hub.quote(x)).assign(source = exch) 
                            for x,exch in zip(All_FX[All_FX["displaySymbol"] == "EUR/USD"]["symbol"].to_list(),
                                                All_FX[All_FX["displaySymbol"] == "EUR/USD"]["source"].to_list()))
                    ,axis=0) # get last quote of EUR/USD from different providers

    EURUSD_quote['quote']=hub.quote(EURUSD_quote["symbol"].to_list())['c']
    EURUSD_quote['quote']= EURUSD_quote.apply(lambda row: hub.quote(EURUSD_quote['symbol'])['c'])


### Symbols (exchanges, symbols, all from main, ETFs, all German, 1 ger on many Xs) ------------------------------------

    hub.exchange().keys() # was working with mistake

    pd.json_normalize(hub.stock_symbols('TO'))
    hub.stock_symbols('US')[0].keys()

    pd.json_normalize(fmpsdk.symbols_list(apikey=fmp_key))
    pd.json_normalize(fmpsdk.available_traded_list(apikey=fmp_key))
    pd.json_normalize(fmpsdk.delisted_companies(apikey=fmp_key,limit=1000)).to_csv('delisted_companies.csv')
    pd.json_normalize(fmpsdk.cik_list(apikey=fmp_key))
    pd.json_normalize(fmpsdk.available_euronext(apikey=fmp_key))
    pd.json_normalize(fmpsdk.euronext_list(apikey=fmp_key))
    pd.json_normalize(fmpsdk.available_tsx(apikey=fmp_key))
    pd.json_normalize(fmpsdk.tsx_list(apikey=fmp_key))


    # All tickers from main exchanges  --------------------------------------        
        FinnHub_code_main_X =[US','PA','T','DE','F','L','PA','SW','TO','SZ']
        MAIN_STOCKS = pd.concat((pd.json_normalize(hub.stock_symbols(exch)).assign(source = exch) # pull all available symbols ...
                                for exch in FinnHub_code_main_X), ignore_index = True) # ... for every main stock exhange
        MAIN_STOCKS.to_csv("All_tickers_from_main_X.csv")
        ETFs=MAIN_STOCKS.loc[MAIN_STOCKS['type'].isin(['ETF','ETC'])]
        STOCKS=MAIN_STOCKS.loc[MAIN_STOCKS['type'].isin(['EQS','DR'])]

    ### German stocks ### -------------------------------------------------------
        German_exchanges= ['SG','DE','MU','F','BE','DU','HM'] # Finnhub codes for german stock exchanges
        GER_STOCKS = pd.concat((pd.json_normalize(hub.stock_symbols(exch)).assign(source = exch) # pull all available symbols ...
                                for exch in German_exchanges), ignore_index = True) # ... for every german stock exhange
        GER_STOCKS['Exch_name']=GER_STOCKS.source.map( # vlookup name of exchange..
                                    hub.exchanges('stock').drop_duplicates('code').set_index('code').name) # we need drop_dublicates as there is 1 dublicate
        GER_STOCKS['Family']=GER_STOCKS["symbol"].str.split('.').str[0] # 1 stock is traded on different exchanges. Ticker goes XXX.YY where YY is the code of exchange
        GER_STOCKS.to_csv("German_tickers.csv")
        GER_ETF=GER_STOCKS.loc[GER_STOCKS['type'].isin(['ETF','ETC'])]
        GER_ONLY_STOCKS=GER_STOCKS.loc[GER_STOCKS['type'].isin(['EQS','DR'])]

    ### Stock traded on many German exhances --------------------------------------
        counts = GER_ONLY_STOCKS['Family'].value_counts() # calculate how many times each family appears
        On_Many_GER_X = GER_ONLY_STOCKS[GER_ONLY_STOCKS['Family'].isin(counts.index[counts==7])] # ... Max was 7, i.e. a stock is traded on 7 german exchanges
        # For example, "Merck" (ticker "6MK"):
        Merck = pd.concat((pd.json_normalize(hub.quote(x)) for x in On_Many_GER_X[On_Many_GER_X.Family=="6MK"].symbol.to_list()))

    # Stock screener
        market_cap_more_than: int = 10**10
        volume_more_than: int = 10**10
        sector = 'Airlines' # full list if settings.py
        industry = "Biotechnology" # full list if settings.py
        limit: int = 4000
        screen_cap = fmpsdk.stock_screener(apikey=fmp_key, market_cap_more_than=market_cap_more_than, industry=industry, limit=limit)

        pd.json_normalize(screen_cap).to_csv('screen_cap.csv')



### Indices, ETFs, constitutes -----------------------------------------

    # Indices and histor constitutes
        Indexes =['^GSPC', '^NDX', '^DJI']
        hub.indices_const(symbol=Indexes[2])
        All_constitu = set([item for sublist in [hub.indices_const(symbol=x)['constituents'] 
                                for x in ['^GSPC', '^NDX', '^DJI']] 
                            for item in sublist])

        pd.concat((pd.json_normalize(hub.indices_hist_const(symbol=x)['historicalConstituents']).assign(Index=x) for x in ['^GSPC', '^NDX', '^DJI']), ignore_index=True).to_csv("Indices_add_remove.csv")


        FMP_indexes = pd.json_normalize(fmpsdk.indexes(apikey=fmp_key))
        FMP_indexes['O/N Chg'] = FMP_indexes.open/FMP_indexes.previousClose
        FMP_indexes['Today Chg'] = FMP_indexes.price/FMP_indexes.open
        FMP_indexes['50vs200'] = FMP_indexes.priceAvg50/FMP_indexes.priceAvg200
        FMP_indexes['1vs50'] = FMP_indexes.price/FMP_indexes.priceAvg50
        FMP_indexes['1vsYHigh'] = FMP_indexes.price/FMP_indexes.yearHigh

        pd.json_normalize(fmpsdk.available_indexes(apikey=fmp_key))


        pd.json_normalize(fmpsdk.sp500_constituent(apikey=fmp_key)).to_csv('sp500_hist.csv') # premium
        pd.json_normalize(fmpsdk.historical_sp500_constituent(apikey=fmp_key)).to_csv('sp500_hist.csv') # premium
        pd.json_normalize(fmpsdk.nasdaq_constituent(apikey=fmp_key)).to_csv('nasda_hist.csv')
        pd.json_normalize(fmpsdk.historical_nasdaq_constituent(apikey=fmp_key)).to_csv('nasda_hist.csv')
        pd.json_normalize(fmpsdk.dowjones_constituent(apikey=fmp_key)).to_csv('dj_hist.csv')
        pd.json_normalize(fmpsdk.historical_dowjones_constituent(apikey=fmp_key)).to_csv('dj_hist.csv')

        Index_const = pd.json_normalize(fmpsdk.sp500_constituent(apikey=fmp_key)).assign(Index="SP500")
        Index_const = Index_const.append(pd.json_normalize(fmpsdk.nasdaq_constituent(apikey=fmp_key)).assign(Index="NASD"))
        Index_const = Index_const.append(pd.json_normalize(fmpsdk.dowjones_constituent(apikey=fmp_key)).assign(Index="DJ"))
        Index_const[Index_const.duplicated(subset=['symbol'],keep=False)]


    # Available ETFs
        FMP_ETFs = pd.json_normalize(fmpsdk.etf_list(apikey=fmp_key))
        fmp_etf = pd.json_normalize(fmpsdk.available_efts(apikey=fmp_key))

        FinnHub_code_main_X =['US','PA','T','DE','F','L','PA','SW','TO','SZ']
        FinnHub_code_main_X =['US']
        MAIN_STOCKS = pd.concat((pd.json_normalize(hub.stock_symbols(exch)).assign(source = exch) # pull all available symbols ...
                                for exch in FinnHub_code_main_X), ignore_index = True) # ... for every main stock exhange
        ETFs=MAIN_STOCKS.loc[MAIN_STOCKS['type'].isin(['ETP'])]

        comb=ETFs.merge(FMP_ETFs, how='outer', on='symbol')
        comb.to_csv("comb.csv")

        # Investment managers (15k rows)
        pd.json_normalize(fmpsdk.cik_list(apikey=fmp_key))
        # there some more formulas on asset managers


    ''' ETFs:
        GACB.DE	    Goldman Sachs ActiveBeta Emerging Markets Equity UCITS ETF
        GACA.DE	    Goldman Sachs ActiveBeta US Large Cap Equity UCITS ETF
        H41J.DE	    HSBC Multi-Factor Worldwide Equity UCITS ETF
            
        GURU	Global X Guruâ„¢ ETF
        GVIP	Goldman Sachs Hedge Industry VIP ETF
        HDG	    ProShares Hedge Replication
        QLS	    IQ Hedge Long/Short Tracker ETF
        PKW	    Invesco BuyBack Achievers ETF
        ROBO	Robo GlobalÂ® Robotics&Automation ETF
        RYJ	    Invesco Raymond James SB-1 Equity ETF
            
        RCD	    Invesco S&P 500Â® Equal Wt Cnsm Disc ETF
        RGI	    Invesco S&P 500Â® Equal Wt Indls ETF
        RSP	    Invesco S&P 500Â® Equal Weight ETF
        RTM	    Invesco S&P 500Â® Equal Weight Matrls ETF
        RYE	    Invesco S&P 500Â® Equal Weight Energy ETF
        RYF	    Invesco S&P 500Â® Equal Weight Fincl ETF
        RYH	    Invesco S&P 500Â® Equal Wt Hlth Care ETF
        RYT	    Invesco S&P 500Â® Equal Weight Tech ETF
        RYU	    Invesco S&P 500Â® Equal Weight Utilts ETF
            
        FENY	FidelityÂ® MSCI Energy ETF
        FHLC	FidelityÂ® MSCI Health Care ETF
        FIDU	FidelityÂ® MSCI Industrials ETF
        FMAT	FidelityÂ® MSCI Materials ETF
        FNCL	FidelityÂ® MSCI Financials ETF
        FREL	FidelityÂ® MSCI Real Estate ETF
        FSTA	FidelityÂ® MSCI Consumer Staples ETF
        FTEC	FidelityÂ® MSCI Information Tech ETF


        ARKQ	ARK Autonomous Technology&Robotics ETF
        ARKF	ARK Fintech Innovation ETF
        ARKG	ARK Genomic Revolution ETF
        ARKK	ARK Innovation ETF
        ARKW	ARK Next Generation Internet ETF


        ARB	    AltShares Merger Arbitrage ETF
        MARB	First Trust Vivaldi Merger Arbitrage ETF
        MNA	    IQ Merger Arbitrage ETF
        MRGR	ProShares Merger
    '''

    # ETF profile, holdings, exposure (not price - it goes separately)
        hub.etfs_profile(symbol='VOO')
        pd.json_normalize(hub.etfs_profile(symbol='UOIL')['profile']).transpose()
        pd.concat((pd.json_normalize(hub.etfs_profile(symbol=x)['profile']) 
                    for x in ['FENY','FHLC','FIDU','FMAT','FNCL','FREL','FSTA','FTEC'])
                  , ignore_index=True).transpose().to_csv("ETF_profile.csv")


        needed_etfs = ['FXI','DJP','XLY','XLP','YANG','YINN','DVY', 'PCY','XLE','XLF','XLV','XLI','XMMO','XMHQ','XMVM','XSMO',
        'XSVM','JJATF','JJCTF','JJETF','JJGTF','JJMTF','COWTF','JJNTF', 'SGGFF','BWVTF','GBBEF','ICITF','OILNF','ILCG','IMCV','IMCG',
        'IMCB','ISCB','ISCG','ISCV','ILCB','ILCV','XLB','XLG','VXX', 'GSP','XHB','XME','XES','XOP','XPH','XRT','XSD','XNTK','XLK', 'ZSL','XLU','PRN']
        pd.concat((pd.json_normalize(hub.etfs_profile(symbol=x)['profile']).assign(ticker = x)
                    for x in needed_etfs), ignore_index=True).to_csv("ETF_profile.csv")




        ARKK = yf.Ticker("ARKK")
        [ARKK.info[x] for x in ['fundInceptionDate', 'annualReportExpenseRatio',
                                'fundFamily','legalType', 'navPrice',
                                'bookValue','annualHoldingsTurnover',
                                'morningStarRiskRating','morningStarOverallRating']]


        # CARZ - global autos
        # KARS - self-driv

        # Only for US
        pd.json_normalize(hub.etfs_holdings(symbol='NIFE')['holdings'])
        pd.json_normalize(hub.etfs_holdings(symbol='IBUY')['holdings'])
        pd.json_normalize(hub.etfs_holdings(symbol='ARKX')['holdings']).to_csv("ARKX_holdings.csv")

        # CARZ - global autos
        # KARS, IDRV, EKAR - self-driv
        # LIT, BATT - battery

        CARZ = pd.json_normalize(hub.etfs_holdings(symbol='CARZ')['holdings'])
        KARS = pd.json_normalize(hub.etfs_holdings(symbol='KARS')['holdings'])
        CAR = CARZ.merge(KARS, how='outer', on='isin')

        pd.concat((pd.json_normalize(hub.etfs_holdings(symbol=x)['holdings']).assign(ticker = x)
                    for x in ['IDRV','KARS','EKAR']), ignore_index=True).to_csv("cars_hold.csv")

        pd.concat((pd.json_normalize(hub.etfs_holdings(symbol=x)['holdings']).assign(ticker = x)
                    for x in ['XLP','VDC']), ignore_index=True).to_csv("stapl_hold.csv")


        pd.json_normalize(fmpsdk.etf_holders(apikey=fmp_key, symbol="CARZ")).head(10)


        import requests
        US_ETFs=pd.read_csv('D:\\Data\\Other_data\\ETFs_tickers.csv')['Symbol']
        data_you_need=pd.DataFrame()
        for x in list(US_ETFs)[700:713]:
            try:
                data = pd.json_normalize(fmpsdk.etf_holders(apikey=fmp_key, symbol=x)).assign(symbol = x)
                data = data.sort_values(by='weightPercentage', ascending=False).head(20)
                data_you_need=data_you_need.append(data,ignore_index=True)
            except:
                pass
        data_you_need.to_csv("ETF_hold.csv")


        # Only for US
        pd.json_normalize(hub.etfs_country_exp(symbol='ROBO')['countryExposure'])
        pd.json_normalize(hub.etfs_country_exp(symbol='GACA.DE')['countryExposure'])
        pd.json_normalize(fmpsdk.etf_sector_weightings(apikey=fmp_key, symbol="ARKK"))
        pd.json_normalize(fmpsdk.etf_country_weightings(apikey=fmp_key, symbol="ARKK"))


### Company (profile, capitalisation, peers, executives, ownership, insider) ------------------------------------------------
    
    all_fields = set(hub.company_profile(symbol="NFLX").keys()) # premium
    free_fields = set(hub.company_profile2(symbol="NFLX").keys()) # symbol, CUSIP, ISIN
    all_fields^free_fields # difference btw premium profile and free profile

    hub.company_profile(symbol="NFLX")['description'] # PREMIUM: Get general information of a company 
    hub.company_profile(symbol="SGP")
    hub.company_profile(isin="US5949181045")
    hub.company_profile2(symbol='SGP')['finnhubIndustry']


    fmpsdk.company_profile(apikey=fmp_key, symbol="SIX3.DE")[0].keys()
    pd.json_normalize(fmpsdk.company_profile(apikey=fmp_key, symbol="AAPL")[0]).transpose()
    needed = ['symbol', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange','exchangeShortName','ipoDate','mktCap','industry', 'description', 'sector', 'country']
    ['price', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes']

    pd.json_normalize(fmpsdk.company_outlook(apikey=fmp_key, symbol="AAPL")).transpose()



    msft = yf.Ticker("SGP")
    [msft.info[x] for x in ['symbol','shortName', 'longName', 'sector', 'industry',
                            'country', 'longBusinessSummary','marketCap']]
    msft.isin
    msft.sustainability
    msft.info['isEsgPopulated']

    pd.json_normalize(fmpsdk.historical_market_capitalization(apikey=fmp_key, symbol="F",limit=3000))
    fmpsdk.market_capitalization(apikey=fmp_key, symbol="AAPL")[0]['marketCap']



    fmpsdk.shares_float(apikey=fmp_key, symbol="AAPL")
    all_float = pd.json_normalize(fmpsdk.shares_float_all(apikey=fmp_key)) # 27k rows


    # same country and GICS sub-industry
    hub.company_peers(symbol="TSLA")
    hub.company_peers(symbol="ABIO")
    hub.company_peers(symbol="ALV.DE")
        # No peers for ETFs
        hub.company_peers(symbol="ACWI")
        hub.company_peers(symbol="LU1452600270.SG")

    # ... but usually we need global peers: e.g. cars: ['DAI.F','RNO.PA','7267.T','TSLA','1211.HK','000270.KS']

    # flat_list = [item for sublist in t for item in sublist], where t is the list to flatten
    cars_global_list = set([item for sublist in [hub.company_peers(symbol=x) for x in ['DAI.F','RNO.PA','7267.T','TSLA','1211.HK','000270.KS']] for item in sublist])
    cars_profiles = pd.concat((pd.json_normalize(hub.company_profile(symbol=x)).transpose() for x in cars_global_list),axis=1)
        not_needed =['address','city','currency','description','employeeTotal','logo','phone','state','weburl']
        # df.drop(not_needed, axis=1)
    cars_profiles.to_csv("cars_profiles.csv")

    fmpsdk.stock_peers(apikey=fmp_key, symbol="AAPL")[0]['peersList']


    hist_index_member=set(pd.read_excel('D:\\Data\\Other_data\\Tickers\\main_ticker_db.xlsx')['symbol'])
    data = []
    for idx, symbol in enumerate(list(hist_index_member)):
        try:
            data.append(fmpsdk.company_profile(apikey=fmp_key, symbol=symbol))
            print('Done with %s. Still %i to go' % (symbol, len(hist_index_member)-idx-1))
        except:
            pass
    flat_data = [item for sublist in data for item in sublist] # flatten the list
    needed = ['symbol', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange','exchangeShortName','ipoDate','mktCap','industry', 'description', 'sector', 'country']
    pd.json_normalize(flat_data).to_csv("memb_profiles.csv")


    not_needed =['address','city','currency','description','employeeTotal','logo','phone','state','weburl']
    pd.concat((pd.json_normalize(hub.company_profile(symbol=x)) for x in hub.indices_const(symbol='^GSPC')['constituents'] if time.sleep(1) is None), ignore_index=True).drop(not_needed, axis=1).to_csv("SP500_profiles.csv")


    pd.json_normalize(hub.company_executive(symbol="TSLA")['executive'])
    pd.json_normalize(fmpsdk.key_executives(apikey=fmp_key, symbol="AAPL"))



    # Ownership and Insider Trading

        pd.json_normalize(hub.ownership(symbol="TSLA")['ownership']) # limit =10 - Limit number of results

        msft = yf.Ticker("MSFT")
        msft.major_holders
        msft.institutional_holders
        [msft.info[x] for x in ['floatShares','sharesOutstanding','impliedSharesOutstanding', 'heldPercentInstitutions','heldPercentInsiders', 'sharesPercentSharesOut']]
        [msft.info[x] for x in ['sharesShort','sharesShortPriorMonth','shortPercentOfFloat',  'dateShortInterest', 'shortRatio', 'sharesShortPreviousMonthDate']]

        pd.json_normalize(fmpsdk.institutional_holders(apikey=fmp_key, symbol="AAPL"))
        pd.json_normalize(fmpsdk.mutual_fund_holders(apikey=fmp_key, symbol="AAPL"))

        # For global ETFs, but old data (from quarterly forms)
        pd.json_normalize(hub.fund_ownership(symbol='AAPL',limit=10)['ownership'])
        pd.json_normalize(hub.fund_ownership(symbol='PUM.DE',limit=10)['ownership'])


        pd.json_normalize(fmpsdk.insider_trading_rss_feed(apikey=fmp_key))
        ins_tr = pd.json_normalize(fmpsdk.insider_trading(apikey=fmp_key, symbol="AAPL",limit=10))
        ins_tr.dtypes
        fmpsdk.insider_trans(apikey=fmp_key)
        pd.json_normalize(fmpsdk.fail_to_deliver(apikey=fmp_key, symbol="AAPL"))


### Company corporate events (divs, splits, merger, IPO) -----------------------------

    # Dividends
        pd.json_normalize(hub.stock_dividends('AAPL', _from="2017-06-10", to="2020-06-10"))
        pd.json_normalize(hub.stock_dividends('ACWI', _from="2017-06-10", to="2020-06-10"))
        pd.json_normalize(hub.stock_dividends('ALV.DE', _from="2017-06-10", to="2020-06-10"))

        hub.stock_dividends('AAPL', _from="2017-06-10", to="2020-06-10")[1].keys()
        # end = datetime.now().strftime("%Y-%m-%d")

        Dividends = pd.concat((pd.json_normalize(hub.stock_dividends(x, _from="2017-06-10")) 
                                for x in ['AAPL','DELL','HPQ']))
        Dividends_cars = pd.concat((pd.json_normalize(hub.stock_dividends(x, _from="2017-06-10")) 
                                for x in hub.company_peers(symbol="TSLA")))

        pd.json_normalize(fmpsdk.dividend_calendar(apikey=fmp_key)) # looks in the future; max interval 3mo
        pd.json_normalize(fmpsdk.historical_stock_dividend(apikey=fmp_key,symbol="AAPL")['historical'])


        # Histor divids from FMP
            Ticker_DB=pd.read_excel('D:\\Data\\Other_data\\Tickers\\main_ticker_db.xlsx')
            Tickers_listed= Ticker_DB[Ticker_DB["was_us_index_const"] == 'Yes']["symbol"].to_list()
            Divids=pd.DataFrame()
            for symbol in Tickers_listed:
                try:
                    data = pd.json_normalize(fmpsdk.historical_stock_dividend(apikey=fmp_key, symbol=symbol)['historical']).assign(symbol = symbol)
                    Divids=Divids.append(data,ignore_index=True)
                except:
                    pass
            Divids.to_csv("Historical_divs_FMP.csv")


        msft = yf.Ticker("MSFT")
        msft.dividends
        [msft.info[x] for x in ['trailingAnnualDividendYield','payoutRatio','trailingAnnualDividendRate','dividendRate', 'fiveYearAvgDividendYield',
        'dividendYield','lastDividendDate','lastDividendValue','exDividendDate']]

    # Splits

        # https://raw.githubusercontent.com/alexanu/stock-split-calendar/master/stock-splits-1982-2017_12_20.tsv
        hub.stock_splits('AAPL', _from="2005-06-10", to="2020-06-10")
        # German stocks splits: https://boersengefluester.de/aktiensplit-monitor/
        hub.stock_splits('PUM.DE', _from="2005-06-10", to="2020-10-10"))

        fmpsdk.stock_split_calendar(apikey=fmp_key) # max interval 3mo
        # Histor splits from FMP
            Ticker_DB=pd.read_excel('D:\\Data\\Other_data\\Tickers\\main_ticker_db.xlsx')
            Tickers_listed= Ticker_DB[Ticker_DB["was_us_index_const"] == 'Yes']["symbol"].to_list()
            Splits=pd.DataFrame()
            for symbol in Tickers_listed:
                try:
                    data = pd.json_normalize(fmpsdk.historical_stock_split(apikey=fmp_key, symbol=symbol)['historical']).assign(symbol = symbol)
                    Splits=Splits.append(data,ignore_index=True)
                except:
                    pass
            Splits.to_csv("Historical_splits_FMP.csv")







        msft = yf.Ticker("MSFT")
        msft.splits
        [msft.info[x] for x in ['lastSplitDate', 'lastSplitFactor']]

    # IPO & Merger
        pd.json_normalize(hub.merger()).to_csv("Mergers_from_Finnhub.csv") # Delivers around 50 mergers from 2009 rill 2019 year. IS it full list?

        # ipo_calendar delivers around 60 lines, so we run a loop of different date ranges, remove duplicates = IPOs from 2005
        IPO = pd.concat((pd.json_normalize(hub.ipo_calendar(_from=x,to=y)['ipoCalendar']) 
                            for x,y in zip(pd.date_range("2019-01-01", periods=25,freq="30d"),
                                        pd.date_range("2019-01-31", periods=25,freq="30d"))))

        fmpsdk.ipo_calendar(apikey=fmp_key, from_date='2021:06:22',to_date='2021:07:22')


### Financials (filings, fin metrics, -------------------------------------------

    # SEC fillings
        # SEC fillings from 1994 archive is here: https://www.kaggle.com/finnhub/sec-filings
        SEC_Fill = pd.json_normalize(hub.filings())
        Germany_fill = pd.json_normalize(hub.international_filings(country='DE')) # + symbol=""
        SEC_Fill[SEC_Fill.columns[:-2]]
        Germany_fill.columns
        Germany_fill.to_csv('Ger_fillings.csv')

        pd.json_normalize(fmpsdk.sec_rss_feeds(apikey=fmp_key))

        F_Fill = pd.json_normalize(hub.filings(symbol='F'))
        Puma_fill = pd.json_normalize(hub.international_filings(symbol='PUM.DE'))

        F_Fill[F_Fill.columns[:-2]]

    # textual difference between a company's 10-K / 10-Q reports
        # def sec_similarity_index(self, symbol="", cik="", freq="annual"): return self._get("/stock/similarity-index", params={"symbol": symbol, "cik": cik, "freq": freq})
        # textual difference between a company's 10-K / 10-Q reports ...
        # ... and the same type of report in the previous year using Cosine Similarity
        # Companies breaking from its routines in disclosure of financial condition and risk analysis section ...
        # ... can signal a significant change in the company's stock price in the upcoming 4 quarters.        
        pd.json_normalize(hub.sim_index(symbol='AAPL')['similarity'])
        pd.json_normalize(hub.sim_index(symbol='AAPL')['similarity']).columns.to_list()
            # item1: Cos sim of Business
            # item1a: Risk Factors
            # item2: for quart - Mgmt Discus & Ana of Fin Condi & Res of Ops
            # item7: for year - Mgmt Discus & Ana of Fin Condi & Res of Ops
            # item7a: Quant & Qual Disclosures About Market Risk

    # Sentiment analysis of 10-K and 10-Q filings from SEC
        pd.json_normalize(hub.sec_sentiment_analysis('0001193125-15-356351')['sentiment']).transpose()
        # abnormal increase in the number of positive/negative words in filings ...
        # ... can signal a significant change in the company's stock price in the upcoming 4 quarters
        #  Loughran and McDonald Sentiment Word Lists is used to calculate the sentiment for each filing
        # https://sraf.nd.edu/textual-analysis/resources/

    # Financial ratios & metrics

        pd.read_csv("https://static.finnhub.io/csv/metrics.csv")
        available_metrics = ['price','valuation','growth','margin','management','financialStrength','perShare']

        pd.json_normalize(hub.company_basic_financials('F','all')['metric']).keys()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','margin')['metric']).transpose()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','price')['metric']).transpose()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','valuation')['metric']).transpose()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','growth')['metric']).transpose()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','management')['metric']).transpose()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','financialStrength')['metric']).transpose()
        pd.json_normalize(hub.company_basic_financials('PUM.DE','perShare')['metric']).transpose()
        
        Financials_cars = pd.concat((pd.json_normalize(hub.company_basic_financials(x, 'all')['metric']).transpose().rename(columns={0 :x}) 
                                    for x in hub.company_peers(symbol="TSLA")),axis=1)
        

        # here are some formulas: https://site.financialmodelingprep.com/developer/docs/formula
        pd.json_normalize(fmpsdk.financial_ratios_ttm(apikey=fmp_key, symbol="AAPL")).transpose()
        pd.json_normalize(fmpsdk.financial_ratios(apikey=fmp_key, symbol="AAPL", period = "annual")).transpose()
        pd.json_normalize(fmpsdk.financial_ratios(apikey=fmp_key, symbol="AAPL", period = "quarter")).transpose()


        pd.json_normalize(fmpsdk.enterprise_values(apikey=fmp_key, symbol="AAPL", period = "annual")).transpose()


        pd.json_normalize(fmpsdk.key_metrics_ttm(apikey=fmp_key, symbol="AAPL")).transpose()
        pd.json_normalize(fmpsdk.key_metrics(apikey=fmp_key, symbol="AAPL", period = "annual")).transpose()

        pd.json_normalize(fmpsdk.financial_growth(apikey=fmp_key, symbol="AAPL", period = "annual")).transpose()

        pd.json_normalize(fmpsdk.rating(apikey=fmp_key, symbol="AAPL")).transpose()
        pd.json_normalize(fmpsdk.historical_rating(apikey=fmp_key, symbol="AAPL",limit=100)).transpose()

    # Financial statements

        # List of symbols that have financial statements at FMP
        fmpsdk.financial_statement_symbol_lists(apikey=fmp_key)
        pd.json_normalize(fmpsdk.financial_reports_dates(apikey=fmp_key, symbol="AAPL"))

        pd.json_normalize(fmpsdk.income_statement_growth(apikey=fmp_key, symbol="AAPL")).transpose()

        fmpsdk.balance_sheet_statement_growth(apikey=fmp_key, symbol="AAPL")
        fmpsdk.cash_flow_statement_growth(apikey=fmp_key, symbol="AAPL")
        fmpsdk.income_statement_as_reported(apikey=fmp_key, symbol="AAPL")
        fmpsdk.balance_sheet_statement_as_reported(apikey=fmp_key, symbol="AAPL")
        fmpsdk.cash_flow_statement_as_reported(apikey=fmp_key, symbol="AAPL")
        fmpsdk.financial_statement_full_as_reported(apikey=fmp_key, symbol="AAPL")


        msft = yf.Ticker("MSFT")
        msft.financials
        msft.quarterly_financials
        msft.balance_sheet
        msft.quarterly_balance_sheet
        msft.cashflow
        msft.quarterly_cashflow
        msft.earnings
        msft.quarterly_earnings
        [msft.info[x] for x in ['nextFiscalYearEnd', 'mostRecentQuarter','lastFiscalYearEnd',
        [msft.info[x] for x in ['totalAssets','profitMargins', 'enterpriseToEbitda','enterpriseToRevenue', 'enterpriseValue','revenueQuarterlyGrowth','earningsQuarterlyGrowth']]
        [msft.info[x] for x in ['forwardEps','trailingEps','pegRatio', 'forwardPE','trailingPE','priceToBook','netIncomeToCommon','priceToSalesTrailing12Months']


        # def financials(self, symbol, statement, freq): return self._get("/stock/financials", params={"symbol": symbol,"statement": statement,"freq": freq})
            # Statement: 'bs' - Balance Sheet, 'ic' - Income Statement, 'cf' - Cash Flow
            # freq: 'annual', 'quarterly', 'ttm' - Trailing Twelve Months fo IS and CF, 'ytd' - only for CF

        F_IS = pd.json_normalize(hub.financials('F','ic','quarterly')['financials'])
        F_IS.columns
        F_IS.to_csv('Ford_IS.csv')

        # def financials_reported(self, **params): return self._get("/stock/financials-reported", params=params)
            # freq: annual or quarterly
            # symbol or CIK
            # accessNumber - access number of a specific report you want to retrieve
        F_Other_IS = pd.json_normalize(hub.financials_reported(symbol='F')['data'])
        # The bulk download  is here: 
        # https://www.kaggle.com/finnhub/reported-financials
        # Financial data parsed from 10-Q, 10-Q/A, 10-K, 10-K/A SEC filings from 2010


### Earnings & Ratings ------------------------------------------------

    # Earnings announcements and estimates

        # Only 1 year of data
        pd.json_normalize(hub.earnings_calendar(international=False)['earningsCalendar']).to_csv("Earnings_calendar.csv")
        pd.json_normalize(hub.earnings_calendar(_from="2020-01-01", to="2020-01-10",international=True)['earningsCalendar'])

        Earn = pd.concat((pd.json_normalize(hub.earnings_calendar(_from=x,to=y,international=True)['earningsCalendar']) 
                            for x,y in zip(pd.date_range("2019-12-01", periods=60,freq="7d"),
                                           pd.date_range("2019-12-07", periods=60,freq="7d"))))
            # Cleaning the Earings DB:
                Earn=pd.read_csv("Earnings_calendar.csv")
                Earn=Earn.drop(Earn.columns[[0]],axis=1)
                Earn=Earn.drop_duplicates()
                Earn = Earn[(Earn == 0).sum(1) < 3] # remove rows with no data
                Earn.to_csv("Earnings_calendar.csv")

        msft = yf.Ticker("MSFT")
        msft.calendar


        Earn = pd.json_normalize(fmpsdk.earning_calendar(apikey=fmp_key))

        # Histor earning calend
            Ticker_DB=pd.read_csv('D:\\Data\\Other_data\\Tickers\\main_ticker_db.csv')
            Tickers_listed= Ticker_DB[Ticker_DB["was_index_mem"] == 1]["symbol"].to_list()
            Earn = pd.concat((pd.json_normalize(fmpsdk.historical_earning_calendar(apikey=fmp_key, symbol=symbol,limit=100)) for symbol in Tickers_listed))
            Earn.to_csv("Historical_earn_surpr_FMP.csv")


        pd.json_normalize(hub.company_earnings(symbol='PUM.DE')) # Earn suprises. Free tier - last 4 quarters
        pd.json_normalize(fmpsdk.earnings_surprises(apikey=fmp_key, symbol="AAPL"))
        
        pd.json_normalize(hub.company_revenue_estimates(symbol='F')['data']) # freq=None
        pd.json_normalize(hub.company_eps_estimates(symbol='PUM.DE')['data']) # freq=None

        msft = yf.Ticker("MSFT")
        msft.calendar

    # Target & Ratings
        pd.json_normalize(hub.recommendation_trends(symbol='PUM.DE'))
        pd.json_normalize(hub.price_target(symbol='F'))
        cars_recoms = pd.concat((pd.json_normalize(hub.price_target(symbol=x)) for x in cars_global_list))

        pd.json_normalize(fmpsdk.historical_rating(apikey=fmp_key, symbol="AAPL")).transpose()
        pd.json_normalize(fmpsdk.rating(apikey=fmp_key, symbol="AAPL")).transpose()

        msft = yf.Ticker("MSFT")
        msft.recommendations

        pd.json_normalize(hub.upgrade_downgrade(symbol='F'))
        # _df['gradeTime'] = to_datetime(_df['gradeTime'], unit='s', utc=True)


### News (company/gener news, press rel, news sent, transcripts, merger news) -------------------------------------------------

    pd.json_normalize(hub.company_news('AAPL', _from="2020-06-10", to="2020-06-10"))
    pd.json_normalize(fmpsdk.stock_news(apikey=fmp_key, tickers=["AAPL",'TSLA']))
    merger_news=pd.json_normalize(hub.general_news(category='merger')) # category: general, forex, crypto, merger
    merger_news['datetime'] = pd.to_datetime(merger_news['datetime'],unit='s')
    merger_news[['datetime','headline','id']] # other columns: 'related', 'source', 'summary', 'url'

    pd.json_normalize(hub.press_releases('AAPL', _from="2020-01-10", to="2020-06-10")['majorDevelopment'])    
    pd.json_normalize(fmpsdk.press_releases(apikey=fmp_key, symbol="AAPL"))

    hub.news_sentiment('AAPL')

    # Only for US: earnings call transcripts
    pd.json_normalize(hub.transcripts_list('AAPL')['transcripts'])
    hub.transcripts('AAPL_176494')
    fmpsdk.earning_call_transcript(apikey=fmp_key, symbol="AAPL", year=2020,quarter=4)
    fmpsdk.batch_earning_call_transcript(apikey=fmp_key, symbol="AAPL", year=2020)

    fmpsdk.earning_call_transcripts_available_dates(apikey=fmp_key, symbol="AAPL")


### Macro (country, econ data, calend) ---------------------------------------------------------

    pd.json_normalize(hub.country()).to_csv("Country_codes.csv") # I've integrated it to my country DB

        # Database of countries is already created using also the Finnhub data
        Country_DB_directory= "...\\Google Drive\\..."
        Country_DB=pd.read_csv(Country_DB_directory + 'country_mapping.csv', usecols=[0,5,6,8], skiprows=1) 
        Country_in_scope=Country_DB[Country_DB['Country Group']!="No"]
        Country_codes = Country_in_scope.code3.to_list()

    pd.json_normalize(hub.economic_code()).to_csv("Econ_codes.csv")

    # Examples of indicator codes
        '''
        "MA-USA-74677677" USA Initial Jobless Claims

        716871	Government Debt to GDP

        7382	Interest Rate
        71897668	Government Bond 10Y

        828389	Retail Sales YoY
        67786778	Consumer Confidence
        6667797870	Business Confidence

        6773	Competitiveness Index
        678482	Corporate Tax Rate
        716780	GDP Constant Prices
        717182	GDP Growth Rate
        807980	Population
        857882	Unemployment Rate
        71806780	GDP per capita PPP
        80738482	Personal Income Tax Rate
        '''

    pd.json_normalize(hub.economic_data("MA-USA-74677677")['data']) # you cannot get "from date"


    Popul = pd.concat((pd.json_normalize(hub.economic_data('MA-'+ Cntr + '-807980')['data']) # Population
                for Cntr in Country_in_scope.code3.sample(frac=0.04).to_list()), axis=1, join='inner')
	
    pd.json_normalize(hub.calendar_economic()['economicCalendar']) # 1 week of upcoming events
    pd.json_normalize(fmpsdk.economic_calendar(apikey=fmp_key)) # max interval 3mo


    pd.json_normalize(hub.economic_data('MA-'+ 'KWT' + '-807980'),max_level=2)


### Quotes ----------------------------------------------------------------------

    pd.json_normalize(fmpsdk.market_hours(apikey=fmp_key)).transpose()
    pd.json_normalize(pd.json_normalize(fmpsdk.market_hours(apikey=fmp_key))['stockMarketHolidays'][0])

    fmpsdk.is_the_market_open(apikey=fmp_key)[['isTheStockMarketOpen']
                                                    # isTheEuronextMarketOpen
                                                    # isTheForexMarketOpen
                                                    # isTheCryptoMarketOpen


    msft = yf.Ticker("MSFT")
    [msft.info[x] for x in ['quoteType', 'symbol','shortName','market',
                            'currency','exchange', 'exchangeTimezoneName', 
                            'exchangeTimezoneShortName','gmtOffSetMilliseconds']]
    [msft.info[x] for x in ['volume', 'volume24Hr','averageDailyVolume10Day',
                            'averageVolume10days','averageVolume',
                            'regularMarketVolume','volumeAllCurrencies']]
    [msft.info[x] for x in ['previousClose', 'open','dayHigh','dayLow','lastMarket',
                            'regularMarketPreviousClose','regularMarketOpen', 
                            'regularMarketDayHigh','regularMarketDayLow',
                            'regularMarketPrice', 

    [msft.info[x] for x in ['fiftyTwoWeekHigh', 'fiftyTwoWeekLow','52WeekChange',
                            'twoHundredDayAverage', 'fiftyDayAverage']
    [msft.info[x] for x in ['ytdReturn',  'threeYearAverageReturn','fiveYearAverageReturn']
   
    pd.json_normalize(fmpsdk.actives(apikey=fmp_key))
    pd.json_normalize(fmpsdk.gainers(apikey=fmp_key))
    pd.json_normalize(fmpsdk.losers(apikey=fmp_key))
    pd.json_normalize(fmpsdk.sectors_performance(apikey=fmp_key))
    

    # Get real-time quote data 
        # real-time for US stocks, for international EOD?
        #   Constant polling is not recommended. 
        #   Use websocket if you need real-time update.
        hub.quote('AAPL')
        Daimler =['DAI.SG','DAI.DE','DAI.MU','DAI.F','DAII.F','DAI.BE','DAI.DU','DAI.HM']
        Daimler_quote = pd.concat((pd.json_normalize(hub.quote(x)).assign(Ticker=x) for x in Daimler))
        Daimler_quote['t'] = pd.to_datetime(Daimler_quote['t'],unit='s')

        NYSE_now = pd.json_normalize(fmpsdk.exchange_realtime(apikey=fmp_key,exchange='NYSE'))

        needed_x = ["TSX","AMEX","NASDAQ","NYSE","EURONEXT"]
        all_data_from_x = pd.concat((pd.json_normalize(fmpsdk.exchange_realtime(apikey=fmp_key,exchange=x)).assign(Exchange=x) for x in needed_x))
        all_data_from_x.columns

        pd.json_normalize(fmpsdk.quote_short(apikey=fmp_key, symbol="AAPL"))

        ListAAPL = fmpsdk.stock_peers(apikey=fmp_key, symbol="AAPL")[0]['peersList']
        pd.json_normalize(fmpsdk.quote(apikey=fmp_key, symbol=ListAAPL)).transpose()


        pd.json_normalize(fmpsdk.euronext_list(apikey=fmp_key))

        pd.json_normalize(fmpsdk.etf_price_realtime(apikey=fmp_key))


    # Bid Ask
        pd.json_normalize(hub.last_bid_ask('AAPL'))
        Cars_US = pd.concat((pd.json_normalize(hub.last_bid_ask(x)).assign(Ticker=x) 
                            for x in hub.company_peers(symbol="TSLA")))
        Cars_US['t'] = pd.to_datetime(Cars_US['t'],unit='ms') # this is UTC

        DJIA = pd.concat((pd.json_normalize(hub.last_bid_ask(x)).assign(Ticker=x) 
                            for x in hub.indices_const(symbol='^DJI')['constituents']))
        DJIA['t'] = pd.to_datetime(DJIA['t'],unit='ms') # this is UTC

        [msft.info[x] for x in ['ask','askSize','bid', 'bidSize']]


### Tech indicators -------------------------------------------------------


    # def pattern_recognition(self, symbol, resolution): return self._get("/scan/pattern", params={"symbol": symbol,"resolution": resolution})
        # Support double top/bottom, triple top/bottom, head and shoulders, triangle, wedge, channel, flag, and candlestick patterns
    # def support_resistance(self, symbol, resolution): return self._get("/scan/support-resistance", params={"symbol": symbol,"resolution": resolution})
    hub.support_resistance("AAPL","D")['levels'] # resolution: 1, 5, 15, 30, 60, D, W, M
        
    def aggregate_indicator(self, symbol, resolution):
        return self._get("/scan/technical-indicator", params={
            "symbol": symbol,
            "resolution": resolution,
        })
    # aggregate signal of multiple technical indicators such as MACD, RSI, Moving Average
    hub.aggregate_indicator("AAPL","D")



    ''' def technical_indicator(self, symbol, resolution, _from, to, indicator, indicator_fields=None):
            indicator_fields = indicator_fields or {}
            params = self._merge_two_dicts({
                "symbol": symbol,
                "resolution": resolution,
                "from": _from,
                "to": to,
                "indicator": indicator
            }, indicator_fields)

            return self._get("/indicator", params=params)
    '''

    indicator_schema = {
        'sma_10': ('sma', {'timeperiod': 10}),
        'sma_30': ('sma', {'timeperiod': 30}),
        'macd_7_14': ('macd', {'fastperiod': 7, 'solwperiod': 14, 'signalperiod': 4}),
        'bbands_7': ('bbands', {'timeperiod':7, 'seriestype': 'open'})
    }
    symbol = ['AAPL','MSFT']
    start='2020-01-01'

    hub.indicators_bulk(symbol, start, indicators_schema=indicator_schema)


        '''
    The Commodity Futures Trading Commission (Commission or CFTC) publishes the Commitments of Traders (COT)
    reports, which provide a breakdown of each Tuesday’s open interest for futures and options 
    on futures markets in which 20 or more traders hold positions equal to or above the reporting levels established by the CFTC.
    
    Generally, the data in the COT reports is from Tuesday and released Friday. 
    The CFTC receives the data from the reporting firms on Wednesday morning ...
    ... and then corrects and verifies the data for release by Friday afternoon.

    '''
    fmpsdk.commitment_of_traders_report_list(apikey=fmp_key)
    fmpsdk.commitment_of_traders_report(apikey=fmp_key, symbol="AAPL",from_date='2020:06:22',to_date='2019:06:22')
    fmpsdk.commitment_of_traders_report_analysis(apikey=fmp_key, symbol="AAPL",from_date='2020:06:22',to_date='2019:06:22')


