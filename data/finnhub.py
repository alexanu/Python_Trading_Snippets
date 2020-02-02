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


