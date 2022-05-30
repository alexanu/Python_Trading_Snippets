
import pandas as pd

# load text file from NASDAQs FTP server
nasdaq_symbols_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
all_stock_symbols = pd.read_csv(nasdaq_symbols_url, sep="|").rename(columns={x: x.lower().replace(" ", "_") for x in all_stock_symbols})
all_stock_symbols[all_stock_symbols.etf=="Y"]


# links to parse
    '''
    https://www.nasdaq.com/market-activity/stocks/hal/dividend-history
    https://www.nasdaq.com/market-activity/stocks/hal/financials
    https://www.nasdaq.com/market-activity/stocks/hal/earnings
    https://www.nasdaq.com/market-activity/stocks/hal/price-earnings-peg-ratios
    https://www.nasdaq.com/market-activity/stocks/hal/revenue-eps
    https://www.nasdaq.com/market-activity/stocks/hal/option-chain
    https://www.nasdaq.com/market-activity/stocks/hal/short-interest
    https://www.nasdaq.com/market-activity/stocks/hal/institutional-holdings
    https://www.nasdaq.com/market-activity/stocks/hal/insider-activity

    https://www.nasdaq.com/market-activity/unusual-volume
    '''


# Combination of Futures and Option names
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

    dome_sign = 'cu'
    dome_expire_year = ['17', '18']
    dome_expire_month = ['01', '02', '03', '04', '05','06', '07', '08', '09', '10', '11', '12']
    dome_all_symbols = [dome_sign + x + y for x in dome_expire_year for y in dome_expire_month]

    fore_sign = 'HG'
    fore_expire_year = ['7', '8']
    fore_expire_month = ['F', 'G', 'H', 'J', 'K', 'M','N', 'Q', 'U', 'V', 'X', 'Z']
    fore_all_symbols = [fore_sign + x + y for x in fore_expire_year for y in fore_expire_month]

    symbol_pairs = zip(dome_all_symbols, fore_all_symbols)


# All possible FX pairs
    CURRENCIES = ['eur', 'gbp', 'aud', 'nzd', 'usd', 'cad', 'chf', 'jpy']
    def pairs():
        return list(itertools.combinations(CURRENCIES, 2))


# Pick random FX pair
    def PickRandomPair(self, pair_type):
        pairs = {
                'major': ['EUR_USD', 'USD_JPY', 'GBP_USD', 'USD_CAD', 'USD_CHF', 'AUD_USD', 'NZD_USD'],
                'minor': ['EUR_GBP', 'EUR_CHF', 'EUR_CAD', 'EUR_AUD', 'EUR_NZD', 'EUR_JPY', 'GBP_JPY', 'CHF_JPY', 'CAD_JPY', 'AUD_JPY', 'NZD_JPY', 'GBP_CHF', 'GBP_AUD', 'GBP_CAD'],
                'exotic': ['EUR_TRY', 'USD_SEK', 'USD_NOK', 'USD_DKK', 'USD_ZAR', 'USD_HKD', 'USD_SGD'],
                'all': ['EUR_USD', 'USD_JPY', 'GBP_USD', 'USD_CAD', 'USD_CHF', 'AUD_USD', 'NZD_USD', 'EUR_GBP', 'EUR_CHF', 'EUR_CAD', 'EUR_AUD', 'EUR_NZD', 'EUR_JPY', 'GBP_JPY', 'CHF_JPY', 'CAD_JPY', 'AUD_JPY', 'NZD_JPY', 'GBP_CHF', 'GBP_AUD', 'GBP_CAD', 'EUR_TRY', 'USD_SEK', 'USD_NOK', 'USD_DKK', 'USD_ZAR', 'USD_HKD', 'USD_SGD']
            }
        return pairs[pair_type][randint(0, len(pairs[pair_type]) - 1)]


# FX triangles

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


# calculate ib commission

    def calculate_ib_commission(self):
        """
        Calculates the fees of trading based on an Interactive
        Brokers fee structure for API, in USD.
        This does not include exchange or ECN fees.
        Based on "US API Directed Orders":
        https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
        """
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else: # Greater than 500
            full_cost = max(1.3, 0.008 * self.quantity)
        return full_cost


# Simulate leverage for ETFs

    import pandas_datareader.data as web
    import pandas as pd
    import datetime


    def sim_leverage(df, leverage=1, expense_ratio=0.0, initial_value=1.0):
        pct_change = df["Adj Close"].pct_change(1)
        sim = pd.DataFrame().reindex_like(df)
        pct_change = (pct_change - expense_ratio / 252) * leverage
        sim["Adj Close"] = (1 + pct_change).cumprod() * initial_value
        sim["Close"] = (1 + pct_change).cumprod() * initial_value

        sim.loc[sim.index[0], "Adj Close"] = initial_value
        sim = sim.drop(columns=["Volume"])

        sim["Open"] = sim["Adj Close"]
        sim["High"] = sim["Adj Close"]
        sim["Low"] = sim["Adj Close"]
        sim["Close"] = sim["Adj Close"]

        return sim


    def main():
        start = datetime.datetime(1989, 1, 1)
        end = datetime.datetime(2019, 1, 1)
        vfinx = web.DataReader("VFINX", "yahoo", start, end)
        vusxt = web.DataReader("VUSTX", "yahoo", start, end)
        upro_sim = sim_leverage(vfinx, leverage=3.0, expense_ratio=0.0092)
        tmf_sim = sim_leverage(vusxt, leverage=3.0, expense_ratio=0.0111)
        spxu_sim = sim_leverage(vfinx, leverage=-3.0, expense_ratio=0.0091, initial_value=100000)

        spxu_sim.to_csv("../data/SPXU_SIM.csv")
        upro_sim.to_csv("../data/UPRO_SIM.csv")
        tmf_sim.to_csv("../data/TMF_SIM.csv")


    if __name__ == "__main__":
        main()


# split request for many tickers
    def get_stockdata(symbols):
        '''Get stock data (key stats and previous) from IEX.
        Just deal with IEX's 99 stocks limit per request.
        '''
        partlen = 99
        result = {}
        for i in range(0, len(symbols), partlen):
            part = symbols[i:i + partlen]
            kstats = iex.Stock(part).get_key_stats()
            previous = iex.Stock(part).get_previous()
            for symbol in part:
                kstats[symbol].update(previous[symbol])
            result.update(kstats)

        return pd.DataFrame(result)


# analyse this :https://www.linkedin.com/pulse/data-analysis-example-python-q-ferenc-bodon-ph-d-/
# and this: https://www.linkedin.com/pulse/python-data-analysis-really-simple-ferenc-bodon-ph-d-/
# https://github.com/BodonFerenc/bestSize/blob/master/python/bestSizeFn.py


# Filtering for historical capitalisation

    all_hist_capital = pd.read_csv('D:\\Data\\Other_data\\hist_capitalisation_index_constit.csv',parse_dates=['date'])
    all_hist_capital = all_hist_capital.loc[all_hist_capital['rank'] < 51] # keep only top50 on every date

    # delete symbols which appeared by mistake
    unique_symb= all_hist_capital['symbol'].value_counts() # counts how many times each symbol appears
    check_symb = all_hist_capital[all_hist_capital['symbol'].isin(unique_symb[unique_symb < 10].index)] # select rows, where symbol appears <10 times
    check_symb = check_symb[check_symb['rank'] < 48] # if it is on the border than it's ok that it appear too few times
    all_hist_capital = all_hist_capital[~all_hist_capital['symbol'].isin(set(check_symb.symbol))] # these symbols we shouldexclude from df as they probably appeared among top50 by mistake

    # extract
    test_capital = all_hist_capital.loc[all_hist_capital['rank'] < 5]
    test_capital = test_capital.loc[test_capital['date'] > "2019"]
    test_capital['date'].min().strftime("%Y-%m-%d") # check the earliest date
    test_capital.to_csv('test_capital.csv')


# Getting TOP 10 symbols for every sector
    TOP_US_TICKERS=[]
    hist_index_member=pd.read_excel('D:\\Data\\Other_data\\ETFDB.xlsx',sheet_name='Stock_tickers',skiprows=0,header=1,usecols=['symbol', 'was_us_index_const','marketCap_Bn','sector']) # read hist index components
    hist_index_member = hist_index_member.dropna(axis = "rows") # drop any row that has missing values
    hist_index_member = hist_index_member[hist_index_member.was_us_index_const=="Yes"]
    [TOP_US_TICKERS.extend(hist_index_member[hist_index_member.sector==i].sort_values(['marketCap_Bn']).symbol[0:10].to_list()) for i in hist_index_member.sector.unique()]


# Group by market cap
    top_market_capitalisation = 'SCREEN(U(IN(0#.SPX)),TOP(TR.CompanyMarketCap,100,nnumber),CURN=USD)'
    df, err = ek.get_data(top_market_capitalisation, ['TR.TickerSymbol','TR.CommonName','TR.CompanyMarketCap(Scale=9)','TR.TRBCActivity','TR.TRBCEconomicSector'])
    df.groupby('TRBC Economic Sector Name')['Company Market Cap'].nlargest(3) # largest companies in every sector
    df.groupby('TRBC Economic Sector Name',as_index=False).apply(lambda x: x.nlargest(3, 'Company Market Cap')) # largest companies in every sector
    df['cap_bin']= pd.qcut(df['Company Market Cap'], q = 4, labels = ["small", "medium", "big", "very big"]) # split into bins
    df.pivot_table(index='TRBC Economic Sector Name', columns='cap_bin', aggfunc='size', fill_value=0)


fb.assign(volume_pct_change=fb.volume.pct_change(),
          avg_volume=lambda x: x.volume.rolling('30D').mean()
          ewma_volume=lambda x: x.volume.ewm(span=30).mean()
          pct_change_rank=lambda x: x.volume_pct_change.abs().rank(ascending=False),
          abs_z_score_volume=lambda x: x.volume.sub(x.volume.mean()).div(x.volume.std()).abs() # Volume Z-Score (subtracting the mean and dividing by the standard deviation)
          )

volume_binned = pd.cut(fb.volume, bins=3, labels=['low', 'med', 'high'])
volume_binned.value_counts()
