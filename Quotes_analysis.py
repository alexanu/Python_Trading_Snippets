import pandas as pd
import datetime as dt
import btalib
import os
import time

from keys_config import *
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus
from alpaca.trading.requests import GetCalendarRequest, GetAssetsRequest, GetOrdersRequest, MarketOrderRequest, LimitOrderRequest, StopLossRequest, TrailingStopOrderRequest, GetPortfolioHistoryRequest
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockTradesRequest, StockQuotesRequest, StockBarsRequest, StockSnapshotRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed, Exchange
from alpaca.broker.client import BrokerClient
import alpaca

trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER)
stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)


# Analysing minutes quote file

    Alpaca_directory = 'D:\\Data\\minute_data\\US\\alpaca_ET_adj\\gesamt\\'
    alpaca_quotes = pd.read_csv(Alpaca_directory+"Alpaca_min_quotes_ET_adj.csv",index_col='timestamp', parse_dates=['timestamp'])
    alpaca_quotes.groupby('ticker')['trade_count'].nlargest(5) # largest trade_count for every ticker



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

# convert tick data to 15 minute data
    data_frame = pd.read_csv(tick_data_file, 
                            names=['id', 'deal', 'Symbol', 'Date_Time', 'Bid', 'Ask'], 
                            index_col=3, parse_dates=True, skiprows= 1)
    ohlc_M15 =  data_frame['Bid'].resample('15Min').ohlc()
    ohlc_H1 = data_frame['Bid'].resample('1H').ohlc()
    ohlc_H4 = data_frame['Bid'].resample('4H').ohlc()
    ohlc_D = data_frame['Bid'].resample('1D').ohlc()

# Back adjust prices relative to adj_close for dividends and splits.
    ts['open'] = ts['open'] * ts['adj_close'] / ts['close']
    ts['high'] = ts['high'] * ts['adj_close'] / ts['close']
    ts['low'] = ts['low'] * ts['adj_close'] / ts['close']
    ts['close'] = ts['close'] * ts['adj_close'] / ts['close']


# Calculate returns & volatility:

    change = df['Adj Close'].diff(1)
    df['Momentum_1D'] = (df['Adj Close'] - df['Adj Close'].shift(1)).fillna(0)
    df['Momentum'] = pd.Series(df['Adj Close'].diff(n))

    df['pct_change'] = df["Adj Close"].pct_change(1)

    daily_returns['ret'] = daily_df.groupby("symbol")["close"].pct_change(1).fillna(0)

    df['pct_change_rank'] = df.pct_change.abs().rank(ascending=False)
    df['ROC'] = ((df['Adj Close'] - df['Adj Close'].shift(n))/df['Adj Close'].shift(n)) * 100
    df['ROC100'] = (df['Adj Close']/df['Adj Close'].shift(n)) * 100

    from scipy.stats import gmean
    df['Geometric_Return'] = pd.Series(df['Adj Close']).rolling(n).apply(gmean)

    df_ma = df[['simple_rtn']].rolling(window=21).agg(['mean', 'std']) #calculate rolling mean and standard deviation

    amzn['Log_Ret'] = np.log(amzn['Close'] / amzn['Close'].shift(1))
    df['Logarithmic_Return'] = np.log(df['Adj Close']) - np.log(df['Adj Close'].shift(1))

    amzn['Volatility'] = amzn['Log_Ret'].rolling(window=252).std() * np.sqrt(252)

    data_mean = np.mean(data['Volume'])
    data_std = np.std(data['Volume'])

    # cumulative return: (1 + return_1) * (1 + return_2) * …
    btc['BTC_minutely_return'] = btc['close'].pct_change().dropna()
    btc['BTC_return'] = btc['BTC_daily_return'].add(1).cumprod().sub(1)

    spy_daily['close_to_close_return'] = spy_daily['close'].pct_change()
    spy_daily['close_to_close_return'].cumsum().plot()

    import talib
    df["roc5"] = talib.ROC(close, timeperiod=5)

    df["oc"] = np.log(bars.Close / bars.Open)

    quote['Returns'] = quote['Close'].pct_change()
    vol = quote['Returns'].std()*np.sqrt(252)
    
    df['STD'] = df['Adj Close'].rolling(10).std()
    df['Variance'] = df['Adj Close'].rolling(20).var()
    
    df['abs_z_score_volume'] = df.volume.sub(df.volume.mean()).div(df.volume.std()).abs() # Volume Z-Score (subtracting the mean and dividing by the standard deviation)

    # Moving dispersion
    from math import sqrt, log
    n = 14 # Number of days
    df['Disp'] = np.sqrt(((abs(np.log(df['Adj Close']/df['Adj Close'].shift()))).rolling(n).sum())/n)



    rets = df['Adj Close'].pct_change().dropna()
    std = rets.rolling(n).std() 
    historical_vol_annually = std*math.sqrt(252)  
    df['RV'] = 100*historical_vol_annually

# Calculate return per sec on irregularly spaced tick data
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

    lookBack = max(timeStampInSeconds.value_counts()) + 10 # maximum number of ticks per second
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

# Get top 10 for every sector from blob & calculate returns by Alpaca

    BLB_URL = f'https://{STORAGE_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{FILE_NAME_SP500}?{BLB_SAS}'
    sp500=pd.read_excel(BLB_URL)
    symbols_in_scope = sp500.symbol.to_list()
    today = trading_client.get_clock().timestamp
    previous_day = today - pd.Timedelta('1D')
    previous_day_10 = today - pd.Timedelta('40D')
    bars_request_params = StockBarsRequest(symbol_or_symbols=symbols_in_scope[20:25], start = previous_day_10, end = previous_day, timeframe=TimeFrame.Day, adjustment= Adjustment.RAW,feed = DataFeed.SIP)
    df = stock_client.get_stock_bars(bars_request_params).df
    df = df.reset_index()
    df.timestamp = df.timestamp.dt.date
    df['days'] = (df.timestamp - previous_day.date()).astype('timedelta64[D]')

    # HACK:This block is needed as there could be no -30 or -5 days because of wknds/holidays
    d0=0
    d5=-5
    d30=-30
    while len(daily_df[daily_df.days==d0])==0:
        d0 = d0 - 1
    while len(daily_df[daily_df.days==d5])==0:
        d5 = d5 - 1
    while len(daily_df[daily_df.days==d30])==0:
        d30 = d30 - 1

    returns_df = daily_df[daily_df.days.isin([d0,d5,d30])]
    returns_df['chg30'] = round(returns_df['close'].pct_change(2)*100,1)
    returns_df['chg5'] = round(returns_df['close'].pct_change(1)*100,1)
    returns_df['chg1'] = round(100*(returns_df.close-returns_df.open)/returns_df.open,1)
    returns_df = returns_df[returns_df.days.isin([d0])]

# 
    minute_frame = 10
    bars_request_params = StockBarsRequest(
        symbol_or_symbols=['AAPL','F','NVDA'], 
        start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(4, "days")).floor(freq='T'), # S: sec, T: minutes, H: hours
        end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
        # limit = 10, # upper limit of number of data points
        timeframe=TimeFrame(minute_frame, TimeFrameUnit.Minute), # 'Day', 'Hour', 'Minute', 'Month', 'Week'
        adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
        feed = DataFeed.SIP
        )
    hist_bars = stock_client.get_stock_bars(bars_request_params).df.reset_index()
    hist_bars.timestamp = hist_bars.timestamp.dt.tz_convert('America/New_York').dt.tz_localize(None) 
                                                    # Convert to market time for easier reading
                                                                                # remove +00:00 from datetime


# Overnight returns
    many_snaps = pd.DataFrame()

    snap = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=symbols_in_scope[20:25], feed = DataFeed.SIP))
    snapshot_data = {stock: [
                            snapshot.latest_trade.timestamp,                        
                            snapshot.latest_trade.price, 

                            snapshot.daily_bar.timestamp,
                            snapshot.daily_bar.open,
                            snapshot.daily_bar.close,

                            snapshot.previous_daily_bar.timestamp,
                            snapshot.previous_daily_bar.close,

                            ]
                    for stock, snapshot in snap.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
                    }
    snapshot_columns=['price time', 'price', 'today', 'today_open', 'today_close','yest', 'yest_close']
    snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)

    snapshot_df['price time'] = snapshot_df['price time'].dt.tz_convert('America/New_York').dt.tz_localize(None) # convert from UTC to ET and remove +00:00 from datetime
    snapshot_df['today'] = snapshot_df['today'].dt.tz_convert('America/New_York').dt.tz_localize(None)
    snapshot_df['yest'] = snapshot_df['yest'].dt.tz_convert('America/New_York').dt.tz_localize(None)

    snapshot_df['FULL'] = (snapshot_df['price']-snapshot_df['yest_close'])/snapshot_df['yest_close']
    snapshot_df['ON'] = (snapshot_df['today_open']-snapshot_df['yest_close'])/snapshot_df['yest_close']
    snapshot_df['DAY'] = (snapshot_df['today_close']-snapshot_df['today_open'])/snapshot_df['today_open']
    snapshot_df['POST'] = (snapshot_df['price']-snapshot_df['today_close'])/snapshot_df['today_close']



    snapshot_df['Now_time'] = trading_client.get_clock().timestamp
    many_snaps = pd.concat([many_snaps, snapshot_df])

    many_snaps_test = many_snaps.copy()
    many_snaps_test['price time'] = many_snaps_test['price time'].dt.tz_convert('America/New_York').dt.tz_localize(None)
    many_snaps_test['today'] = many_snaps_test['today'].dt.tz_convert('America/New_York').dt.tz_localize(None)
    many_snaps_test['yest'] = many_snaps_test['yest'].dt.tz_convert('America/New_York').dt.tz_localize(None)
    many_snaps_test['Now_time'] = many_snaps_test['Now_time'].dt.tz_localize(None)
    many_snaps[['price time', 'today', 'yest', 'Now_time']] = many_snaps_test[['price time', 'today', 'yest', 'Now_time']].dt.tz_localize(None)
    many_snaps_test.to_excel('many_snaps.xlsx')




# identify outliers and plot them

    import matplotlib.pyplot as plt
    plt.style.use('seaborn') #set style to `seaborn`

    df_ma = df[['simple_rtn']].rolling(window=21).agg(['mean', 'std']) #calculate rolling mean and standard deviation
    df_ma.columns = df_ma.columns.droplevel() # drop multi-level index

    # identify outliers
    df_outliers = df.join(df_ma)
    df_outliers['outlier'] = [1 if (x > mu + 3 * sigma) 
                                or (x < mu - 3 * sigma) else 0 
                            for x, mu, sigma in zip(df_outliers.simple_rtn, df_outliers['mean'], df_outliers['std'])] 
    fig, ax = plt.subplots(figsize=(15, 9)) # create instance of plot
    outliers = df_outliers.loc[df_outliers['outlier'] == 1, ['simple_rtn']] # define outliers for convenience
    ax.plot(df_outliers.index, df_outliers.simple_rtn, color='blue', label='Normal') # add line plot of returns
    ax.scatter(outliers.index, outliers.simple_rtn, color='red', label='Anomaly') # add points for outliers
    plt.legend(loc='lower right')
    plt.title('Apple stock returns', fontsize = 20)
    plt.show();

# identify outliers
    STD_CUTTOFF = 9
    indexs = []
    outliers = []
    data_std = np.std(data['Volume'])
    data_mean = np.mean(data['Volume'])
    anomaly_cut_off = data_std * STD_CUTTOFF
    upper_limit = data_mean + anomaly_cut_off
    for i in range(len(data)):
        temp = data['Volume'].iloc[i]
        if temp > upper_limit:
            indexs.append(str(data['Date'].iloc[i])[:-9])
            outliers.append(temp)
    d = {'Dates': indexs, 'Volume': outliers}


# Indicators
    # MA
        df['High_Highest'] = df['Adj Close'].rolling(n).max()
        df['Low_Lowest'] = df['Adj Close'].rolling(n).min()


        AAPL['42d'] = np.round(AAPL['Close'].rolling(window=42).mean(), 2)
        AAPL['42-252'] = AAPL['42d'] - AAPL['252d']
        SD = 0.5
        AAPL['Position'] = np.where(AAPL['42-252'] > SD, 1, 0)
        AAPL['Position'] = np.where(AAPL['42-252'] < -SD, -1, AAPL['Position'])
        AAPL['Position'].value_counts()
        AAPL['Market'] = np.log(AAPL['Close'] / AAPL['Close'].shift(1))
        AAPL['Strategy'] = AAPL['Position'].shift(1) * AAPL['Market']

        import talib as ta
        df['SMA'] = ta.SMA(df['Adj Close'], timeperiod=3) # EMA


        def MACD(df_dict, a=12 ,b=26, c=9):
            """function to calculate MACD
            typical values a(fast moving average) = 12; 
                            b(slow moving average) =26; 
                            c(signal line ma window) =9"""
            for df in df_dict:
                df_dict[df]["ma_fast"] = df_dict[df]["close"].ewm(span=a, min_periods=a).mean()
                df_dict[df]["ma_slow"] = df_dict[df]["close"].ewm(span=b, min_periods=b).mean()
                df_dict[df]["macd"] = df_dict[df]["ma_fast"] - df_dict[df]["ma_slow"]
                df_dict[df]["signal"] = df_dict[df]["macd"].ewm(span=c, min_periods=c).mean()
                df_dict[df].drop(["ma_fast","ma_slow"], axis=1, inplace=True)

        def stochastic(df_dict, lookback=14, k=3, d=3):
            """function to calculate Stochastic Oscillator
            lookback = lookback period
            k and d = moving average window for %K and %D"""
            for df in df_dict:
                df_dict[df]["HH"] = df_dict[df]["high"].rolling(lookback).max()
                df_dict[df]["LL"] = df_dict[df]["low"].rolling(lookback).min()
                df_dict[df]["%K"] = (100 * (df_dict[df]["close"] - df_dict[df]["LL"])/(df_dict[df]["HH"]-df_dict[df]["LL"])).rolling(k).mean()
                df_dict[df]["%D"] = df_dict[df]["%K"].rolling(d).mean()
                df_dict[df].drop(["HH","LL"], axis=1, inplace=True)

    # RSI
        df['RSI'] = ta.RSI(df['Adj Close'], timeperiod=14)

        data= alpaca.get_bars(ticker, TimeFrame.Hour, (datetime.date.today() - datetime.timedelta(days=+2)).isoformat(), datetime.date.today().isoformat(), adjustment='raw').df
        rsi= btalib.rsi(data).df # RSI > 50 < 75 

        # RSI step-by-step:
            change = df['Adj Close'].diff(1)
            df['Gain'] = change.mask(change<0,0)
            df['Loss'] = abs(change.mask(change>0,0))
            df['AVG_Gain'] = df.Gain.rolling(n).mean()
            df['AVG_Loss'] = df.Loss.rolling(n).mean()
            df['RS'] = df['AVG_Gain']/df['AVG_Loss']
            df['RSI'] = 100 - (100/(1+df['RS']))

    # True Range
        df['Prior Close'] = df['Adj Close'].shift()
        df['BP'] = df['Adj Close'] - df[['Low','Prior Close']].min(axis=1)
        df['TR'] = df[['High','Prior Close']].max(axis=1) - df[['Low','Prior Close']].min(axis=1)

    # Regime 
        apple["Regime"] = np.where(apple['20d-50d'] > 0, 1, 0) # np.where() is a vectorized if-else function
        apple["Regime"] = np.where(apple['20d-50d'] < 0, -1, apple["Regime"]) # and to maintain the rest of the vector, the second argument is apple["Regime"]

    # Drawdown
        df["cum_return"] = (1 + df["return"]).cumprod()
        df["cum_max"] = df["cum_return"].cummax()
        df["drawdown"] = df["cum_max"] - df["cum_return"]
        df["drawdown_pct"] = df["drawdown"]/df["cum_max"]
        max_drawdown = df["drawdown_pct"].max()
        df.drop(["cum_return","cum_max","drawdown","drawdown_pct"], axis=1, inplace=True)

    # Correlation
        df = pd.concat([df1['Adj Close'], df2['Adj Close']],axis=1)
        df.columns = [symbol1,symbol2]
        df['Corr'] = df['AAPL'].rolling(20).corr(df['QQQ'])
        df['M_Cor'] = df['AAPL'].rolling(20).corr(df['QQQ']).rolling(20).mean() # moving corr coef
        df['Price Relative'] = df['AAPL']/df['^GSPC']
        df['Percentage Change in Price Relative'] = ((df['Price Relative']-df['Price Relative'].shift())/df['Price Relative'].shift())*100

    # VWAP
        def VWAP(df):
            return (df['Adj Close'] * df['Volume']).sum() / df['Volume'].sum()
        n = 14
        df['VWAP'] = pd.concat([(pd.Series(VWAP(df.iloc[i:i+n]), index=[df.index[i+n]])) for i in range(len(df)-n)])
        df = df.dropna()

    # Bollinger Bands
        df['20 Day MA'] = df['Adj Close'].rolling(window=20).mean()
        df['20 Day STD'] = df['Adj Close'].rolling(window=20).std()
        df['Upper Band'] = df['20 Day MA'] + (df['20 Day STD'] * 2)
        df['Lower Band'] = df['20 Day MA'] - (df['20 Day STD'] * 2)

