# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 16:31:08 2018
http://docs.tradingeconomics.com/#introduction
@author: javif
"""

import tradingeconomics as te

# %%
user = 'guest'
password = 'guest'
te.login('%s:%s' % (user, password))

# %%
gdpDF = te.getIndicatorData(country='united states', indicators='gdp')

# %% earnings
symbol = 'msft'
country = 'us'
earningsDF = te.getEarnings(symbols='%s:%s' % (symbol, country), initDate='2016-01-01', endDate='2018-12-31')

# %%
hist = te.getHistoricalData(country=['united states', 'china'], indicator=['exports', 'imports'],
                            initDate='1990-01-01', endDate='2015-01-01', output_type='df')
