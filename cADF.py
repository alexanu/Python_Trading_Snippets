
# Cointegrated Augmented Dickey-Fuller: if a time # series is mean reverting or not. 
# Portfolio of equities possesses mean-reverting behavior. 
# The simplest combination would be a "pairs trade" with a dollar-neutral long-short pair of equities. 
# Find optimal hedging ratio of the two time series + test the stationarity of the linear combination 

import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import pandas.io.data as web
import pprint
import statsmodels.tsa.stattools as ts

from pandas.stats.api import ols

from __future__ import print_function

if __name__ == "__main__":
    import sys
    ticker = str(sys.argv[1])
    series = web.DataReader(ticker, "yahoo", dt.datetime(2000,1,1), dt.datetime(2015,1,1))
    dat = ts.adfuller(series['Adj Close'], 1)
    print(dat)



# TODO: take start and end dates as inputs
def plot_2Price_Series(df, ts1, ts2):
    """
    plots two time series line graphs on the same chart
    """
    months = mdates.MonthLocator() 
    fig, ax = plt.subplots()
    ax.plot(df.index, df[ts1], label=ts1)
    ax.plot(df.index, df[ts2], label=ts2)
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(dt.datetime(2012, 1, 1), dt.datetime(2013,1,1))
    ax.grid(True)
    fig.autofmt_xdate()

    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('%s and %s Daily Prices' % (ts1, ts2))
    plt.legend()
    plt.show()

# TODO: take start and end dates as inputs
def plot_Scatter_Series(df, ts1, ts2):
    """
    Plots the scatter plot of ts1 vs ts2 time series
    """
    plt.xlabel('%s Price ($)' % ts1)
    plt.ylabel('%s Price ($)' % ts2)
    plt.title('%s and %s Price Scatterplot' % (ts1, ts2))
    plt.scatter(df[ts1], df[ts2])
    plt.show()

# TODO: take start and end dates as inputs
def plot_Residuals(df):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df.index, df["res"], label="Residuals")
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(dt.datetime(2012, 1, 1), dt.datetime(2013, 1, 1))
    ax.grid(True)
    fig.autofmt_xdate()

    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('Residual Plot')
    plt.legend()

    plt.plot(df["res"])
    plt.show()


# TODO: make prompts for both ticker names, and date ranges
if __name__ == "__main__":
    start = dt.datetime(2012, 1, 1)
    end = dt.datetime(2013, 1, 1)

    # Store commandline input of the two tickers
    #string = str(sys.argv[1])

    # Split into a list of the two ticker names
    #stock_lst = string.split()

    # Store the individual ticker names
    #stock1 = stock_lst[0]
    #stock2 = stock_lst[1]
    stock1 = "AREX"
    stock2 = "WLL"

    # Retrieve the separate  time series
    stock1_series = web.DataReader(stock1, "yahoo", start, end)
    stock2_series = web.DataReader(stock2, "yahoo", start, end)

    # Create DataFrame with the first ticker as the major index
    df = pd.DataFrame(index=stock1_series.index)
    
    # store the Adjusted close series of each ticker in the DataFrame
    df[stock1] = stock1_series["Adj Close"]
    df[stock2] = stock2_series["Adj Close"]
   
    # Plot the two series on the same graph to eyeball any correlation
    plot_2Price_Series(df, stock1, stock2)
    
    # Plot the scatterplot of ts1 vs ts2
    plot_Scatter_Series(df, stock1, stock2)

    # Calculate the optimal hedge ratio (basically the slope of the OLS line
    # of best fit of the scatterplot)
    #res = sm.OLS(y=df[stock2], x=df[stock1])
    #res = sm.OLS(df[stock2], df[stock1])
    ols_res = ols(y=df[stock2], x=df[stock1])
    beta_hedge = ols_res.beta.x

    # Calculate the residuals of the OLS vs the scatterplot data points
    df["res"] = df[stock2] - beta_hedge*df[stock1]

    # Plot the residuals
    plot_Residuals(df)

    # Calculate and output the CADF test on the residuals
    cadf = ts.adfuller(df["res"])
    pprint.pprint(cadf)



#################################################################################
import pandas as pd
from statsmodels.tsa.stattools import adfuller, kpss

def adf_test(series):
    '''
    Null Hypothesis: time series is not stationary
    Alternate Hypothesis: time series is stationary
   '''
    adf_test = adfuller(series, autolag='AIC')
    adf_results = pd.Series(adf_test[0:4], index=['Test Statistic',
                                                  'p-value',
                                                  '# of Lags Used',
                                                  '# of Observations Used'])
    for key, value in adf_test[4].items():
        adf_results[f'Critical Value ({key})'] = value

    print('Results of Augmented Dickey-Fuller Test ----')
    print(adf_results)


def kpss_test(series, h0_type='c'):
    '''
    Null Hypothesis: time series is stationary
    Alternate Hypothesis: time series is not stationary
    When null='c' then we are testing level stationary, when 'ct' trend stationary.
    '''
    kpss_test = kpss(series, regression=h0_type)
    kpss_results = pd.Series(kpss_test[0:3], index=['Test Statistic',
                                                    'p-value',
                                                    '# of Lags'])
    for key, value in kpss_test[3].items():
        kpss_results[f'Critical Value ({key})'] = value

    print('Results of KPSS Test ----')
    print(kpss_results)