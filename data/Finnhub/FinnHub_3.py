#   API Documentation: https://finnhub.io/docs/api
#   Source: https://github.com/wagnerio/finnhub-api-wrapper

import requests
import signal
import logging
import time
import os

# Globals
LOG_LEVEL = int(os.environ.get('LOG_LEVEL', logging.WARNING))
_stop = False


class FinnHub:
    # The logger
    log = None

    # The base part of the API endpoints
    base_uri = "https://finnhub.io/api/v1"
    
    # All GET request require a token parameter ?token= in your URL.
    # You can find your API Key under Dashboard: https://finnhub.io/dashboard
    API_KEY = None

    # Most endpoints will have a limit of 60 requests per minute per api key.
    # However, /scan endpoint have a limit of 10 requests per minute. 
    # If your limit is exceeded, you will receive a response with status code 429.
    LAST_HEADERS = None

    # Define a timeout in seconds for every request
    TIMEOUT_SEC = 5


    def get_stock_company_profile(self, symbol):
        """Get general information of a company."""
        
        params = {'symbol': symbol}
        return self.call_api('/stock/profile', params)

    
    def get_stock_ceo_compensation(self, symbol):
        """Get latest company's CEO compensation.
        This endpoint only available for US companies."""

        params = {'symbol': symbol}
        return self.call_api('/stock/ceo-compensation', params)

    
    def get_stock_recommendation_trends(self, symbol):
        """Get latest analyst recommendation trends for a company."""

        params = {'symbol': symbol}
        return self.call_api('/stock/recommendation', params)


    def get_stock_price_target(self, symbol):
        """Get latest price target consensus."""

        params = {'symbol': symbol}
        return self.call_api('/stock/price-target', params)


    def get_stock_option_chain(self, symbol):
        """Get company option chain.
        This endpoint only available for US companies."""

        params = {'symbol': symbol}
        return self.call_api('/stock/option-chain', params)
    

    def get_stock_peers(self, symbol):
        """Get company peers."""

        params = {'symbol': symbol}
        return self.call_api('/stock/peers', params)
    

    def get_stock_earnings(self, symbol):
        """Get company quarterly earnings."""

        params = {'symbol': symbol}
        return self.call_api('/stock/earnings', params)


    def get_stock_candles(self, symbol, resolution="D", count=200, format="json"):
        """Get candlestick data for stocks."""
        
        params = {'symbol': symbol, 'resolution': resolution, 'count': str(count), 'format': format}
        return self.call_api('/stock/candle', params)


    def get_stock_candles_by_timerange(self, symbol, resolution="D", start="", end="", format="json"):
        """Get candlestick data for stocks."""

        params = {'symbol': symbol, 'resolution': resolution, 'from': start, 'to': end, 'format': format}
        return self.call_api('/stock/candle', params)


    def get_forex_exchanges(self):
        """List supported forex exchanges"""

        return self.call_api('/forex/exchange')


    def get_forex_symbol(self, exchange):
        """List supported forex symbol"""

        params={'exchange': exchange}
        return self.call_api('/forex/symbol', params)


    def get_forex_candles(self, symbol, resolution="D", count=200, format="json"):
        """Get candlestick data for forex."""

        params={'symbol': symbol, 'resolution': resolution, 'count': str(count), 'format': format}
        return self.call_api('/forex/candle', params)


    def get_forex_candles_by_timerange(self, symbol, resolution="D", start="", end="", format="json"):
        """Get candlestick data for forex."""

        params={'symbol': symbol, 'resolution': resolution, 'from': start, 'to': end, 'format': format}
        return self.call_api('/forex/candle', params)


    def get_patterns(self, symbol, resolution="D"):
        """Run pattern recognition algorithm on a symbol.
        Support double top/bottom, triple top/bottom, head and shoulders, 
        triangle, wedge, channel, flag, and candlestick patterns"""

        params={'symbol': symbol, 'resolution': resolution}
        return self.call_api('/scan/pattern', params)


    def get_support_resistance(self, symbol, resolution="D"):
        """Get support and resistance levels for a symbol."""
        
        params={'symbol': symbol, 'resolution': resolution}
        return self.call_api('/scan/support-resistance', params)


    def get_technical_indicators(self, symbol, resolution="D"):
        """Get aggregate signal of multiple technical indicators 
        such as MACD, RSI, Moving Average v.v."""
        
        params={'symbol': symbol, 'resolution': resolution}
        return self.call_api('/scan/technical-indicator', params)


    def get_general_news(self, category="general", min_id=0):
        """Get latest market news."""

        params={'category': category, 'minId': min_id}
        return self.call_api('/news', params)


    def get_company_news(self, symbol):
        """List latest company news by symbol. 
        This endpoint is only available for US companies."""

        return self.call_api('/news/{}'.format(symbol))
        

    def get_news_sentiment(self, symbol):
        """Get company's news sentiment and statistics."""
        
        params={'symbol': symbol}
        return self.call_api('/news-sentiment', params)


    def get_merger_country(self):
        """List countries where merger and acquisitions take place."""

        return self.call_api('/merger/country')
    

    def get_merger_and_acquisitions(self, country):
        """List latest merger and acquisitions deal by country."""
        
        params={'country': country}
        return self.call_api('/merger', params)

    
    def get_economic_code(self):
        """List codes of supported economic data."""
        
        return self.call_api('/economic/code')
    

    def get_economic_data(self):
        """Get economic data."""

        return self.call_api('/economic')


    def get_economic_calendar(self):
        """Get recent and coming economic releases."""

        return self.call_api('/calendar/economic')


    def get_earnings_calendar(self):
        """Get recent and coming earnings release."""
        
        return self.call_api('/calendar/earnings')


    def get_ipo_calendar(self):
        """Get recent and coming IPO."""
        
        return self.call_api('/calendar/ipo')


    def get_ico_calendar(self):
        """Get recent and coming ICO."""

        return self.call_api('/calendar/ico')


    def __init__(self, api_key):
        def signal_handler(signal, frame):
            global _stop
            print('Stopping Crawler...')
            _stop = True
        signal.signal(signal.SIGINT, signal_handler)

        logging.basicConfig(format='%(levelname)s:%(message)s', level=LOG_LEVEL)
        self.log = logging.getLogger(__name__)
        self.log.debug("Initializing FinnHub API with API-Key {}.".format(api_key))
        self.API_KEY = api_key


    def remember_headers(self, headers):
        self.LAST_HEADERS = headers


    def check_limit(self):
        if self.LAST_HEADERS == None:
            self.log.info("LAST_HEADERS is None. OK!")
            return
        
        
        headers = self.LAST_HEADERS
        current_ts = int(time.time())
        reset_ts = int(headers['X-Ratelimit-Reset'])
        remaining_calls = int(headers['X-Ratelimit-Remaining'])
        seconds_until_reset = reset_ts - current_ts

        self.log.info("Remaining Call: {} | Seconds until reset: {}".format(remaining_calls, seconds_until_reset))

        if remaining_calls == 0:
            self.log.info("Sleeping for {} seconds until next call.".format(seconds_until_reset))
            time.sleep(seconds_until_reset + 2)


    def call_api(self, resource, params={}):
        if _stop == True:
            exit(0)
        
        self.check_limit()
        url = '{}{}'.format(self.base_uri, resource)
        params['token'] = self.API_KEY

        self.log.debug("Call URL: {} | Params: {}".format(url, params))

        result = None
        
        try:
            r = requests.get(url, params=params, timeout=self.TIMEOUT_SEC)
            r.raise_for_status()
            self.remember_headers(r.headers)
            result = r.json()
        except (ConnectionError, TimeoutError) as e:
            self.log.exception(e)
            self.remember_headers(None)
            result = None

        return result