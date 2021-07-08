import pandas as pd
import quandl
import datetime
 
start = datetime.datetime(2016,1,1)
end = datetime.date.today()
 
s = "AAPL"
# get() function in quandl will return a pandas DataFrame
apple = quandl.get("WIKI/" + s, start_date=start, end_date=end)
 
type(apple)

import matplotlib.pyplot as plt
%matplotlib inline # necessary for the plot to appear in Jupyter
%pylab inline # Control the default size
pylab.rcParams['figure.figsize'] = (15, 9)   # Change the size of plots
 
apple["Adj. Close"].plot(grid = True)

microsoft, google = (quandl.get("WIKI/" + s, 
                                start_date=start, end_date=end) 
                        for s in ["MSFT", "GOOG"])
 
stocks = pd.DataFrame({"AAPL": apple["Adj. Close"],
                      "MSFT": microsoft["Adj. Close"],
                      "GOOG": google["Adj. Close"]})

stocks.plot(secondary_y = ["AAPL", "MSFT"], grid = True)

# If all else fails, read from a file, 
# obtained from here: http://www.nasdaq.com/symbol/spy/historical
spyderdat = pd.read_csv("/home/curtis/Downloads/HistoricalQuotes.csv")    # Obviously specific to my system; set to                                                                        # location on your machine
spyderdat = pd.DataFrame(spyderdat.loc[:, ["open", "high", "low", "close", "close"]]
                        .iloc[1:].as_matrix(),
                        index=pd.DatetimeIndex(spyderdat.iloc[1:, 0]),
                        columns=["Open", "High", "Low", "Close", "Adj Close"])
            .sort_index()
spyder = spyderdat.loc[start:end]
stocks = stocks.join(spyder.loc[:, "Adj Close"])
                .rename(columns={"Adj Close": "SPY"})

stock_return = stocks.apply(lambda x: x / x[0])
stock_return.plot(grid = True)
            .axhline(y = 1, color = "black", lw = 2)
import numpy as np 
stock_change = stocks.apply(lambda x: np.log(x) - np.log(x.shift(1))) # shift moves dates back by 1
stock_change.plot(grid=True).axhline(y = 0, color = "black", lw = 2)

tbill = quandl.get("FRED/TB3MS", start_date=start, end_date=end)
rrf = tbill.iloc[-1, 0] # Get the most recent Treasury Bill rate
smcorr = stock_change_apr.drop("SPY", 1).corrwith(stock_change_apr.SPY)


apple["20d"] = np.round(apple["Adj. Close"]
                        .rolling(window = 20, center = False)
                        .mean(), 
                        2)
apple["50d"] = np.round(apple["Adj. Close"]
                        .rolling(window = 50, center = False).mean(), 2)
apple["200d"] = np.round(apple["Adj. Close"]
                        .rolling(window = 200, center = False).mean(), 2)
apple['20d-50d'] = apple['20d'] - apple['50d']

# np.where() is a vectorized if-else function
apple["Regime"] = np.where(apple['20d-50d'] > 0, 1, 0)
# and to maintain the rest of the vector, the second argument is apple["Regime"]
apple["Regime"] = np.where(apple['20d-50d'] < 0, -1, apple["Regime"])
apple.loc['2016-01-04':'2016-12-31',"Regime"]
     .plot(ylim = (-2,2))
     .axhline(y = 0, color = "black", lw = 2) 
apple["Regime"].plot(ylim = (-2,2))
               .axhline(y = 0, color = "black", lw = 2)
apple["Regime"].value_counts()

# To ensure that all trades close out, 
# I temporarily change the regime of the last row to 0
regime_orig = apple.loc[:, "Regime"].iloc[-1]
apple.loc[:, "Regime"].iloc[-1] = 0
apple["Signal"] = np.sign(apple["Regime"] - apple["Regime"].shift(1))
# Restore original regime data
apple.loc[:, "Regime"].iloc[-1] = regime_orig

# what the prices of the stock is at every buy and every sell?
apple.loc[apple["Signal"] == 1, "Close"]
apple.loc[apple["Signal"] == -1, "Close"]

# Create a DataFrame with trades, 
# including the price at the trade 
# and the regime under which the trade is made.
apple_signals = pd.concat([
        pd.DataFrame({"Price": apple.loc[apple["Signal"] == 1, "Adj. Close"],
                     "Regime": apple.loc[apple["Signal"] == 1, "Regime"],
                     "Signal": "Buy"}),
        pd.DataFrame({"Price": apple.loc[apple["Signal"] == -1, "Adj. Close"],
                     "Regime": apple.loc[apple["Signal"] == -1, "Regime"],
                     "Signal": "Sell"}),
    ])
apple_signals.sort_index(inplace = True)
# Let's see the profitability of long trades
apple_long_profits = pd.DataFrame({
        "Price": apple_signals.loc[(apple_signals["Signal"] == "Buy") &
                                  apple_signals["Regime"] == 1, "Price"],
        "Profit": pd.Series(apple_signals["Price"] - apple_signals["Price"].shift(1)).loc[
            apple_signals.loc[(apple_signals["Signal"].shift(1) == "Buy") & (apple_signals["Regime"].shift(1) == 1)].index
        ].tolist(),
        "End Date": apple_signals["Price"].loc[
            apple_signals.loc[(apple_signals["Signal"].shift(1) == "Buy") & (apple_signals["Regime"].shift(1) == 1)].index
        ].index
    })
apple_long_profits

