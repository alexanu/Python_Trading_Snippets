import requests

#OHLC data
r = requests.get('https://finnhub.io/api/v1/stock/candle?symbol=AAPL&resolution=1&from=1572651390&to=1572910590')
print(r.json())

#Tick data
r = requests.get('https://finnhub.io/api/v1/stock/tick?symbol=AAPL&from=1575968404&to=1575968424')
print(r.json())

# Quote data
r = requests.get('https://finnhub.io/api/v1/quote?symbol=AAPL')
print(r.json())



import requests
import os
import time
from datetime import datetime

from .stock_history import StockHistory

TOKEN = os.environ['FINNHUB_TOKEN']


def get_stock_history(symbol: str, start, end, wait=1):
    """
    https://finnhub.io/docs/api#stock-candles
    """
    today = datetime.now()
    days = (today - start).days
    r = requests.get('https://finnhub.io/api/v1/stock/candle?symbol={SYMBOL}&token={TOKEN}&resolution=D&count={days}'.format(TOKEN=TOKEN, SYMBOL=symbol, days=days))
    try:
        body = r.json()
        if body["s"] == "ok":
            return StockHistory(symbol, body, end)
        else:
            return StockHistory(symbol, StockHistory.EMPTY_BODY, end)
    except:
        if r.status_code == 429:
            # API limiting, so let's wait and retry
            print("API rate limiting... retrying in {wait}".format(wait=wait))
            time.sleep(wait)
            return get_stock_history(symbol, start, end, wait=min(wait*2, 8))  # exponential backoff
        # if a different error, print out error message
        print(r.status_code, r.text)

