from tiingo import TiingoClient
from sp500 import *
import pandas as pd

class clsfunctions():
    def getTicker(self,i):
        tickers = ['A','AAL','AAP','AAPL','ABBV','GLD']
        return tickers[i]

    def tiingo(self, startNum, endNum):
        fx = clsfunctions()
        config = {}
        config['session'] = True
        config['api_key'] = ""
        client = TiingoClient(config)

        for i in range(startNum, endNum):
            ticker = fx.getTicker(i)

            data = client.get_ticker_price(ticker,
                                           fmt='json',
                                           startDate='1993-01-29',
                                           endDate='2019-07-12',
                                           frequency='daily')

            columns = ['date', 'open', 'high', 'low', 'close', 'volume']

            df = pd.DataFrame(data, columns=columns)
            df['ticker'] = ticker

            df.to_csv('c:/users/paul/desktop/tickers/' + ticker + '.csv')


import threading
import time
fx = clsfunctions()
startTime = time.time()

t1 = threading.Thread(target=fx.tiingo,args=(0, 50))
t2 = threading.Thread(target=fx.tiingo,args=(50, 100))
t3 = threading.Thread(target=fx.tiingo,args=(100, 150))
t4 = threading.Thread(target=fx.tiingo,args=(150, 200))

t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()

print(round(time.time() - startTime, 2), ' seconds')