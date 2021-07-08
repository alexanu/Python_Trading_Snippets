import os
import math
import numpy as np
import pandas as pd
from pylab import plt, mpl

plt.style.use('seaborn')
mpl.rcParams['savefig.dpi'] = 300
mpl.rcParams['font.family'] = 'serif'
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.float_format', '{:.4f}'.format)
np.set_printoptions(suppress=True, precision=4)
os.environ['PYTHONHASHSEED'] = '0'

url = 'http://hilpisch.com/aiif_eikon_eod_data.csv'
symbol = 'EUR='
data = pd.DataFrame(pd.read_csv(url, index_col=0,parse_dates=True).dropna()[symbol])
data.info()

data['SMA1'] = data[symbol].rolling(42).mean()
data['SMA2'] = data[symbol].rolling(258).mean()
data.dropna(inplace=True)
data['p'] = np.where(data['SMA1'] > data['SMA2'], 1, -1)
data['p'] = data['p'].shift(1) # Shifts the position values by one day to avoid foresight bias

data.plot(figsize=(10, 6));
data.plot(figsize=(10, 6), secondary_y='p');

data['r'] = np.log(data[symbol] / data[symbol].shift(1)) # log returns
data.dropna(inplace=True)
data['s'] = data['p'] * data['r'] # strategy returns
data[['r', 's']].sum().apply(np.exp) # gross performances
data[['r', 's']].sum().apply(np.exp) - 1 # net performances
data[['r', 's']].cumsum().apply(np.exp).plot(figsize=(10, 6));

sum(data['p'].diff() != 0) + 2 # number of trades, including entry and exit trade
pc = 0.005 # Fixes the proportional transaction costs (deliberately set quite high)
data['s_'] = np.where(data['p'].diff() != 0, data['s'] - pc, data['s']) # Adjusts the strategy performance for the transaction costs
data['s_'].iloc[0] -= pc # Adjusts the strategy performance for the entry trade
data['s_'].iloc[-1] -= pc # Adjusts the strategy performance for the exit trade
data[['r', 's', 's_']][data['p'].diff() != 0] # Shows the adjusted performance values for the regular trades
data[['r', 's', 's_']].sum().apply(np.exp)
data[['r', 's', 's_']].sum().apply(np.exp) - 1
data[['r', 's', 's_']].cumsum().apply(np.exp).plot(figsize=(10, 6));

data[['r', 's', 's_']].std() # Daily volatility
data[['r', 's', 's_']].std() * math.sqrt(252) # Annualized volatility
