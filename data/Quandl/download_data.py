## Copyright Enzo Busseti 2016

from __future__ import print_function
import pandas as pd
import numpy as np
import datetime as dt
import quandl

#all_assets=pd.read_csv('assets.txt', comment='#').set_index('Symbol')
#all_assets=pd.read_csv('NASDAQ100.txt', comment='#').set_index('Symbol')
all_assets=pd.read_csv('SP500.txt', comment='#').set_index('Symbol')

NSTOCKS=100

QUANDL={
    ## Get a key (free) from quandl.com and copy it here
    'authtoken':"",
    'start_date':dt.date(2007, 1, 1),
    'end_date':dt.date(2016, 12, 31)
}
RISK_FREE_SYMBOL = "USDOLLAR"


np.random.seed(0)
assets=all_assets.loc[np.random.choice(all_assets.index[:-1], int(NSTOCKS*1.2), replace=False)]
assets.loc[RISK_FREE_SYMBOL] = all_assets.loc[RISK_FREE_SYMBOL]
    
# download assets' data
data={}
for ticker in assets.index:
    print('downloading %s from %s to %s' %(ticker, QUANDL['start_date'], QUANDL['end_date']))
    try:
        data[ticker] = quandl.get(assets.Quandlcode[ticker], **QUANDL)
    except quandl.NotFoundError:
        print('\tInvalid asset code')
        
def select_first_valid_column(df, columns):
    for column in columns:
        if column in df.columns:
            return df[column]
# extract prices
prices=pd.DataFrame.from_items([(k,select_first_valid_column(v, ["Adj. Close", "Close", "VALUE"])) 
                                for k,v in data.items()])

#compute sigmas
high=pd.DataFrame.from_items([(k,select_first_valid_column(v, ["High"])) for k,v in data.items()])
low=pd.DataFrame.from_items([(k,select_first_valid_column(v, ["Low"])) for k,v in data.items()])
sigmas = (high-low) / (2*high)

# extract volumes
volumes=pd.DataFrame.from_items([(k,select_first_valid_column(v, ["Adj. Volume", "Volume"])) for k,v in data.items()])

# fix risk free
prices[RISK_FREE_SYMBOL]=10000*(1 + prices[RISK_FREE_SYMBOL]/(100*250)).cumprod()

# filter NaNs - threshold at 2% missing values
bad_assets = prices.columns[prices.isnull().sum()>len(prices)*0.02]
if len(bad_assets):
    print('Assets %s have too many NaNs, removing them' % bad_assets)

prices = prices.loc[:,~prices.columns.isin(bad_assets)]
sigmas = sigmas.loc[:,~sigmas.columns.isin(bad_assets)]
volumes = volumes.loc[:,~volumes.columns.isin(bad_assets)]

nassets=prices.shape[1]

# days on which many assets have missing values
bad_days1=sigmas.index[sigmas.isnull().sum(1) > nassets*.9]
bad_days2=prices.index[prices.isnull().sum(1) > nassets*.9]
bad_days3=volumes.index[volumes.isnull().sum(1) > nassets*.9]
bad_days=pd.Index(set(bad_days1).union(set(bad_days2)).union(set(bad_days3))).sort_values()
print ("Removing these days from dataset:")
print(pd.DataFrame({'nan price':prices.isnull().sum(1)[bad_days],
                    'nan volumes':volumes.isnull().sum(1)[bad_days],
                    'nan sigmas':sigmas.isnull().sum(1)[bad_days]}))

prices=prices.loc[~prices.index.isin(bad_days)]
sigmas=sigmas.loc[~sigmas.index.isin(bad_days)]
volumes=volumes.loc[~volumes.index.isin(bad_days)]

# extra filtering
print(pd.DataFrame({'remaining nan price':prices.isnull().sum(),
                    'remaining nan volumes':volumes.isnull().sum(),
                    'remaining nan sigmas':sigmas.isnull().sum()}))
prices=prices.fillna(method='ffill')
sigmas=sigmas.fillna(method='ffill')
volumes=volumes.fillna(method='ffill')
print(pd.DataFrame({'remaining nan price':prices.isnull().sum(),
                    'remaining nan volumes':volumes.isnull().sum(),
                    'remaining nan sigmas':sigmas.isnull().sum()}))

# make volumes in dollars
volumes = volumes*prices

# compute returns
returns = (prices.diff()/prices.shift(1)).fillna(method='ffill').ix[1:]
bad_assets = returns.columns[((-.5>returns).sum()>0)|((returns > 2.).sum()>0)]
if len(bad_assets):
    print('Assets %s have dubious returns' % bad_assets)
    
prices = prices.loc[:,~prices.columns.isin(bad_assets)]
sigmas = sigmas.loc[:,~sigmas.columns.isin(bad_assets)]
volumes = volumes.loc[:,~volumes.columns.isin(bad_assets)]
returns = returns.loc[:,~returns.columns.isin(bad_assets)]

# discard extra stocks
if prices.shape[1]<NSTOCKS+1:
    raise Exception('Too many discarded stock. Change universe.')
    
used_symbols=list(prices.columns[:NSTOCKS])+[RISK_FREE_SYMBOL]
    
prices = prices.loc[:,prices.columns.isin(used_symbols)]
sigmas = sigmas.loc[:,sigmas.columns.isin(used_symbols)]
volumes = volumes.loc[:,volumes.columns.isin(used_symbols)]
returns = returns.loc[:,returns.columns.isin(used_symbols)]

# save data
prices.to_csv('prices.txt', float_format='%.3f')
volumes.to_csv('volumes.txt', float_format='%d')
returns.to_csv('returns.txt', float_format='%.3e')
sigmas.to_csv('sigmas.txt', float_format='%.3e')