import pandas as pd
from fbprophet import Prophet

import pandas as pd
import matplotlib.pyplot as plt

import pandas_ta as ta
from alphaVantageAPI.alphavantage import AlphaVantage 

ticker='CW'
AV = AlphaVantage(
        api_key='PUT YOUR API KEY HERE',
        premium=False,       
        datatype='json',
        export=False,
        export_path= 'C:/Users/Andy/Downloads',
        output='csv',
        output_size='full',
        clean=True,
        proxy={})

df = AV.data(symbol=ticker, function='DA') # daily adjusted
predper = 60 #number of days to predict ahead
print(f"Shape: {df.shape}")
df.set_index(['date'], inplace=True)
df.fillna(0.0, inplace=True)

df.to_csv (r'C:/Users/Andy/Downloads/pricedataCW.csv', index = 'date', header=True)
df = pd.read_csv('C:/Users/Andy/Downloads/pricedataCW.csv',
                usecols=['date','close'])
df.columns = ['ds', 'y']
m = Prophet(
    growth="linear",
    #holidays=holidays,
    #seasonality_mode="multiplicative",
    changepoint_prior_scale=30,
    seasonality_prior_scale=35,
    ###cap=3.00,
    ###floor=.65*125,
    holidays_prior_scale=20,
    daily_seasonality=False,weekly_seasonality=False,yearly_seasonality=False)
    .add_seasonality(name='monthly',period=30.5,fourier_order=55)
    .add_seasonality(name='daily', period=1, fourier_order=15)
    .add_seasonality(name='weekly',period=7, fourier_order=20)
    .add_seasonality(name='yearly',period=365.25,fourier_order=20)
    .add_seasonality(name='quarterly',period=365.25/4,fourier_order=5,prior_scale=15)
m.fit(df)
future = m.make_future_dataframe(periods=predper)
forecast = m.predict(future)

fig1 = m.plot(forecast)
fig2 = m.plot_components(forecast)