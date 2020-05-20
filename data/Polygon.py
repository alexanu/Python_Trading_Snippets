'''
Intentionally blank

Polygon.io client components
-- Mike W (blackdog.gianttree@gmail.com)

'''

from datetime import timedelta
import requests


class PolygonAPI:
	''' Basic client for Polygon.io API '''

	def __init__(self, key, host='api.polygon.io', version='v2'):
		self.host = host
		self.version = version
		self._key = key
		self._session = requests.session()

	@property
	def _baseurl(self):
		return 'https://{}/{}'.format(self.host, self.version)

	def _get(self, resource):
		''' Get method '''
		endpoint = '{}/{}'.format(self._baseurl, resource)
		params = {'apiKey': self._key}
		return self._session.get(endpoint, params=params)

	def get_day(self, date, market='STOCKS'):
		''' Return entire universe prices for one day
			-- date of market data
			-- market (STOCKS, CRYPTO, etc)
		'''
		resource = 'aggs/grouped/locale/US/market/{}/{}'.format(market, date)
		return self._get(resource).json()['results']

	def get_history(self, ticker, end, days_back, multiplier=1, timespan='day'):
		''' Return history from one ticker
			-- ticker symbol
			-- end datetime object
			-- days back to start
			-- multiplier to skip number of observations
			-- timespan day, hour, minute
		'''
		start = end-timedelta(days=days_back)
		resource = 'aggs/ticker/{}/range/{}/{}/{}/{}'.format(ticker, multiplier, timespan, start, end)
		return self._get(resource).json()['results']
