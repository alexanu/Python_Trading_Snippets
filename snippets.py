
import pandas as pd

# load text file from NASDAQs FTP server
nasdaq_symbols_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
all_stock_symbols = pd.read_csv(nasdaq_symbols_url, sep="|").rename(columns={x: x.lower().replace(" ", "_") for x in all_stock_symbols})
all_stock_symbols[all_stock_symbols.etf=="Y"]




https://www.nasdaq.com/market-activity/stocks/hal/dividend-history
https://www.nasdaq.com/market-activity/stocks/hal/financials
https://www.nasdaq.com/market-activity/stocks/hal/earnings
https://www.nasdaq.com/market-activity/stocks/hal/price-earnings-peg-ratios
https://www.nasdaq.com/market-activity/stocks/hal/revenue-eps
https://www.nasdaq.com/market-activity/stocks/hal/option-chain
https://www.nasdaq.com/market-activity/stocks/hal/short-interest
https://www.nasdaq.com/market-activity/stocks/hal/institutional-holdings
https://www.nasdaq.com/market-activity/stocks/hal/insider-activity

# ETFs with HAL as top 10 holding
https://old.nasdaq.com/symbol/hal

https://old.nasdaq.com/symbol/hal/premarket
https://www.nasdaq.com/market-activity/stocks/hal/pre-market-trades
https://old.nasdaq.com/symbol/hal/after-hours
https://www.nasdaq.com/market-activity/stocks/hal/after-hours-trades


# Calendars
https://www.nasdaq.com/market-activity/dividends
https://www.nasdaq.com/market-activity/earnings
https://www.nasdaq.com/market-activity/economic-calendar
https://www.nasdaq.com/market-activity/ipos
https://www.nasdaq.com/market-activity/ipo-performance

https://www.nasdaq.com/market-activity/stock-splits


# Symbol change history:
https://www.nasdaq.com/market-activity/stocks/symbol-change-history


https://www.nasdaq.com/market-activity/unusual-volume

https://www.nasdaq.com/market-activity/nasdaq-52-week-hi-low


https://www.nasdaq.com/market-activity/stocks/screener
https://www.nasdaq.com/market-activity/stocks/screener









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


# Claculate return per sec on irregularly spaced tick data
    # https://www.thertrader.com/2020/03/15/speeding-up-your-python-code/

    import numpy as np    
    import pandas as pd
    import os
    import time
    
    # regularly spaced data is needed to calculate 1 second return ...
    # ... however tick data is completely irregularly spaced in time 
    # # I need first at each point in time, to find the right time stamp looking backward then do the calculation itself.

    mf = pd.read_csv(theMessageFile, 
                    names = ['timeStamp','EventType','Order ID','Size','Price','Direction'], 
                    float_precision='round_trip') # keeps the same number of decimals as in the original csv file
    stop = []

    #----- Method #1 : Standard method with Pandas - very slow!!!
    timeStamp = mf['timeStamp'].to_frame() 
    timeStamp = timeStamp[:100000]
    stop = [timeStamp.timeStamp[abs(timeStamp.timeStamp 
                                - (timeStamp.timeStamp[i] - 1)).idxmin()] # look for index 1 second back in the past 
            for i in range(len(timeStamp)-1)]

    #----- Method #2 : Numpy Array - very similar but much faster
    timeStamp = mf['timeStamp'].to_frame().values # convert to Numpy array
    timeStamp = timeStamp[:100000]
    stop = [timeStamp[np.abs(timeStamp 
                            - (timeStamp[i] - 1)).argmin()]  
            for i in np.arange(len(timeStamp))]

    #-----  Method #3: Numpy + optimal experiment design - 150 times faster!!
    timeStamp = mf['timeStamp']
    timeStamp = timeStamp[:100000]
    timeStampInSeconds = timeStamp.round(0) 


    # at each point in time,  I don’t need to search for the entire set of indexes ...
    # ... but only indexes located before i ...
    # ... so it can be of the form i-n with n being anything between 0 and i-1. 
    # I find the maximum number of ticks per second in the entire data set 
    # this will be the maximum number of ticks I will have to look back in the past ...
    # ... to find the right index to calculate the return per second. 

    lookBack = max(timeStampInSeconds.value_counts()) + 10 
    timeStamp = timeStamp.to_frame().values # convert to Numpy array like above
    myPos = []
    for i in range(len(timeStamp)):
        if i == 0:
            pos = timeStamp[0]
        elif i < lookBack:
            pos = timeStamp[abs(timeStamp[:i,0] - (timeStamp[i,0] - 1)).argmin()]
        elif i >= lookBack:    
            a = i - lookBack
            bb = timeStamp[a:i,0]
            pos = bb[abs(bb - (timeStamp[i,0] - 1)).argmin()]
        myPos.append(pos)


#convert tick data to 15 minute data
    data_frame = pd.read_csv(tick_data_file, 
                            names=['id', 'deal', 'Symbol', 'Date_Time', 'Bid', 'Ask'], 
                            index_col=3, parse_dates=True, skiprows= 1)
    ohlc_M15 =  data_frame['Bid'].resample('15Min').ohlc()
    ohlc_H1 = data_frame['Bid'].resample('1H').ohlc()
    ohlc_H4 = data_frame['Bid'].resample('4H').ohlc()
    ohlc_D = data_frame['Bid'].resample('1D').ohlc()


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


# Simulate leverage

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


# identify outliers and plot them

    import matplotlib.pyplot as plt
    plt.style.use('seaborn') #set style to `seaborn`

    df_ma = df[['simple_rtn']].rolling(window=21).agg(['mean', 'std']) #calculate rolling mean and standard deviation
    df_ma.columns = df_ma.columns.droplevel() # drop multi-level index

    # identify outliers
    df_outliers = df.join(df_ma)
    df_outliers['outlier'] = [1 if (x > mu + 3 * sigma) 
                                or (x < mu - 3 * sigma) else 0 
                            for x, mu, sigma in zip(df_outliers.simple_rtn, 
                                                        df_outliers['mean'], 
                                                        df_outliers['std'])] 
    fig, ax = plt.subplots(figsize=(15, 9)) # create instance of plot
    outliers = df_outliers.loc[df_outliers['outlier'] == 1, ['simple_rtn']] # define outliers for convenience
    ax.plot(df_outliers.index, df_outliers.simple_rtn, color='blue', label='Normal') # add line plot of returns
    ax.scatter(outliers.index, outliers.simple_rtn, color='red', label='Anomaly') # add points for outliers
    plt.legend(loc='lower right')
    plt.title('Apple stock returns', fontsize = 20)
    plt.show();


# Comparison of dataframes
    def overlap_by_symbol(old_df: pd.DataFrame, new_df: pd.DataFrame, overlap: int):
        """
        Overlap dataframes for timestamp continuity. 
        Prepend the end of old_df to the beginning of new_df, grouped by symbol.
        If no symbol exists, just overlap the dataframes
        :param old_df: old dataframe
        :param new_df: new dataframe
        :param overlap: number of time steps to overlap
        :return DataFrame with changes
        """
        if isinstance(old_df.index, pd.MultiIndex) and isinstance(new_df.index, pd.MultiIndex):
            old_df_tail = old_df.groupby(level='symbol').tail(overlap)

            old_df_tail = old_df_tail.drop(set(old_df_tail.index.get_level_values('symbol')) - set(new_df.index.get_level_values('symbol')), level='symbol')

            return pd.concat([old_df_tail, new_df], sort=True)
        else:
            return pd.concat([old_df.tail(overlap), new_df], sort=True)


# Analysing minutes quote file
    dateparse = lambda x: pd.datetime.strptime(x, '%d.%m.%Y %H:%M')
    AAPL = pd.read_csv("D:\\Data\\minute_data\\AAPL.txt", sep='\t', decimal=",", 
                        parse_dates={'datetime': ['Date', 'Time']}, date_parser=dateparse)

    AAPL.dtypes
    AAPL.count()
    AAPL.isna().mean() # calculate the % of missing values in each row
    AAPL.memory_usage(deep = True) # Show the usage of memory of every column
    AAPL=AAPL.drop(AAPL.columns[[-1]],axis=1) # delete last column
    AAPL.nlargest(20,'Volume')
    AAPL[AAPL.Close - AAPL.Open>5]
    AAPL[AAPL.Open - AAPL.Close.shift()>15] # shows where the diff btw t-1 close and t > smth
    AAPL[(AAPL.nlargest(20,'Volume')) & (AAPL.nlargest(20,'Close'))]

    AAPL[(AAPL.Open - AAPL.Close.shift()>15)(AAPL.Close - AAPL.Open>5)] # shows where the diff btw t-1 close and t > smth

    amzn['Log_Ret'] = np.log(amzn['Close'] / amzn['Close'].shift(1))
    amzn['Volatility'] = amzn['Log_Ret'].rolling(window=252).std() * np.sqrt(252)

    AAPL['42d'] = np.round(AAPL['Close'].rolling(window=42).mean(), 2)
    AAPL['42-252'] = AAPL['42d'] - AAPL['252d']
    SD = 0.5
    AAPL['Position'] = np.where(AAPL['42-252'] > SD, 1, 0)
    AAPL['Position'] = np.where(AAPL['42-252'] < -SD, -1, AAPL['Position'])
    AAPL['Position'].value_counts()
    AAPL['Market'] = np.log(AAPL['Close'] / AAPL['Close'].shift(1))
    AAPL['Strategy'] = AAPL['Position'].shift(1) * AAPL['Market']
