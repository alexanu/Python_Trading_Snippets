import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
import yfinance as yf
yf.pdr_override()
import datetime as dt

# input
symbol = 'AAPL'
start = dt.date.today() - dt.timedelta(days = 365*2)
end = dt.date.today()

# Read data 
df = yf.download(symbol,start,end)

df['Upper_Channel_Line'] = pd.Series.rolling(df['High'], window=20).max()
df['Lower_Channel_Line'] = pd.Series.rolling(df['Low'], window=20).min()
df['Middle_Channel_Line'] = (df['Upper_Channel_Line'] + df['Lower_Channel_Line'])/2
df = df.dropna()

plt.figure(figsize=(16,10))
plt.plot(df['Adj Close'])
plt.fill_between(df.index, df['Lower_Channel_Line'], df['Upper_Channel_Line'],  color='lightblue', alpha=0.4)
plt.plot(df['Upper_Channel_Line'], c='darkred', linestyle='-', drawstyle="steps")
plt.plot(df['Lower_Channel_Line'], c='forestgreen', linestyle='-', drawstyle="steps")
plt.plot(df['Middle_Channel_Line'], c='blue', linestyle='-')
plt.title('Dochain Channel for Stock')
plt.legend(loc='best')
plt.xlabel('Price')
plt.ylabel('Date')
plt.show()

# ## Candlestick with Donchain Channel
from matplotlib import dates as mdates

dfc = df.copy()
dfc['VolumePositive'] = dfc['Open'] < dfc['Adj Close']
#dfc = dfc.dropna()
dfc = dfc.reset_index()
dfc['Date'] = mdates.date2num(dfc['Date'].tolist())
from mplfinance.original_flavor import candlestick_ohlc
fig, ax1 = plt.subplots(figsize=(20,12))
candlestick_ohlc(ax1,dfc.values, width=0.5, colorup='g', colordown='r', alpha=1.0)
#colors = ['red', 'green', 'blue']
#labels = ['Upper Channel Line', 'Lower Channel Line', 'Middle Channel Line']
for i in dfc[['Upper_Channel_Line', 'Lower_Channel_Line', 'Middle_Channel_Line']]:
    ax1.plot(dfc['Date'], dfc[i])
ax1.xaxis_date()
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
ax1.grid(True, which='both')
ax1.minorticks_on()
ax1v = ax1.twinx()
colors = dfc.VolumePositive.map({True: 'g', False: 'r'})
ax1v.bar(dfc.Date, dfc['Volume'], color=colors, alpha=0.4)
ax1v.axes.yaxis.set_ticklabels([])
ax1v.set_ylim(0, 3*df.Volume.max())
ax1.set_title('Stock '+ symbol +' Closing Price')
ax1.set_ylabel('Price')
ax1.set_xlabel('Date')
ax1.legend(loc='best')
plt.show()


# -------------------------------------------------------------------------------------------------
# Implementation from https://raposa.trade/blog/use-python-to-trade-the-donchian-channel/

import pandas as pd
import numpy as np

def calcDonchianChannels(data: pd.DataFrame, period: int):
    data["upperDon"] = data["High"].rolling(period).max()
    data["lowerDon"] = data["Low"].rolling(period).min()
    data["midDon"] = (data["upperDon"] + data["lowerDon"]) / 2
    return data

def calcReturns(df):
  df['returns'] = df['Close'] / df['Close'].shift(1)
  df['log_returns'] = np.log(df['returns'])
  df['strat_returns'] = df['position'].shift(1) * df['returns']
  df['strat_log_returns'] = df['position'].shift(1) * df['log_returns']
  df['cum_returns'] = np.exp(df['log_returns'].cumsum()) - 1
  df['strat_cum_returns'] = np.exp(df['strat_log_returns'].cumsum()) - 1
  df['peak'] = df['cum_returns'].cummax()
  df['strat_peak'] = df['strat_cum_returns'].cummax()
  return df

# enter a position when the price closes above the middle value and will go short when the price drops below
def midDonCrossOver(data: pd.DataFrame, period: int=20, shorts: bool=True):
    data = calcDonchianChannels(data, period)
    data["position"] = np.nan
    data["position"] = np.where(data["Close"]>data["midDon"], 1, data["position"])
    if shorts:
        data["position"] = np.where(data["Close"]<data["midDon"], -1, data["position"])
    else:
        data["position"] = np.where(data["Close"]<data["midDon"], 0, data["position"])
    data["position"] = data["position"].ffill().fillna(0)

    return calcReturns(data)


# go long when the price breaks through the upper channel ...
# ... and short or exit the trade if it breaks below the lower channel
def donChannelBreakout(data, period=20, shorts=True):
    data = calcDonchianChannels(data, period)  
    data["position"] = np.nan
    data["position"] = np.where(data["Close"]>data["upperDon"].shift(1), 1, data["position"])

    if shorts:
        data["position"] = np.where(data["Close"]<data["lowerDon"].shift(1), -1, data["position"])
    else:
        data["position"] = np.where(data["Close"]<data["lowerDon"].shift(1), 0, data["position"])
        
    data["position"] = data["position"].ffill().fillna(0)

    return calcReturns(data)

