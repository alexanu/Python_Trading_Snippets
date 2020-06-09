import requests
import logging
from pandas.io.json import json_normalize


from finnhub.exceptions import FinnhubAPIException
from finnhub.exceptions import FinnhubRequestException

class Client:
    API_URL = "https://finnhub.io/api/v1"
    log = None

    # Most endpoints will have a limit of 60 requests per minute per api key.
    # However, /scan endpoint have a limit of 10 requests per minute. 
    # If your limit is exceeded, you will receive a response with status code 429.
    LAST_HEADERS = None

    # Define a timeout in seconds for every request
    TIMEOUT_SEC = 5


    def __init__(self, api_key, requests_params=None):
        self.api_key = api_key
        self.session = self._init__session()
        self._requests_params = requests_params

    def _init__session(self):
        session = requests.session()
        session.headers.update({'Accept': 'application/json',
                                'User-Agent': 'finnhub/python'})
        return session

    def _request(self, method, uri, **kwargs):
        
        kwargs['timeout'] = 10

        data = kwargs.get('data', None)

        if data and isinstance(data, dict):
            kwargs['data'] = data
        else:
            kwargs['data'] = {}

        kwargs['data']['token'] = self.api_key
        kwargs['params'] = kwargs['data']

        del(kwargs['data'])

        response = getattr(self.session, method)(uri, **kwargs)

        return self._handle_response(response)


    def _create_api_uri(self, path):
        return "{}/{}".format(self.API_URL, path)

    def _request_api(self, method, path, **kwargs):
        uri = self._create_api_uri(path)
        return self._request(method, uri, **kwargs)

    def _handle_response(self, response):
        if not str(response.status_code).startswith('2'):
            raise FinnhubAPIException(response)
        try:
            return response.json()
        except ValueError:
            raise FinnhubRequestException("Invalid Response: {}".format(response.text))

    def _get(self, path, **kwargs):
        return self._request_api('get', path, **kwargs)


###### STOCK FUNDAMENTALS ################

    def company_profile(self, **params): # You can query by symbol, ISIN or CUSIP
        return self._get("stock/profile", data=params)

    def company_profile_base(self, **params): # You can query by symbol, ISIN or CUSIP
        return self._get("stock/profile2", data=params)

    def executives(self, **params): # list of company's executives and members of the Board
        return self._get("stock/executive", data=params)


    def news(self, **params): # category = "general", "forex", "merge". "minId" for latest news
        return self._get("news", data=params)

    def company_news(self, **params):
        return self._get("company-news", data=params)

    def news_sentiment(self, **params):
        return self._get("news-sentiment", data=params)

    def major(self, **params):
        return self._get("major-development", data=params)



    def peers(self, **params):
        return self._get("stock/peers", data=params)

    def metric(self, **params):
        return self._get("stock/metric", data=params)






###### STOCK ESTIMATES ################




###### STOCK PRICE ################



###### FOREX ################



###### TECHNICAL ANALYSIS ################




###### ALT DATA ################

    def covid(self):
        return self._get("covid19/us")





###### ECONOMIC ################









    def ceo_compensation(self, **params):
        return self._get("stock/ceo-compensation", data=params)

    def recommendation(self, **params):
        return self._get("stock/recommendation", data=params)

    def price_target(self, **params):
        return self._get("stock/price-target", data=params)

    def upgrade_downgrade(self, **params):
        return self._get("stock/upgrade-downgrade", data=params)

    def option_chain(self, **params):
        return self._get("stock/option-chain", data=params)


    def earnings(self, **params):
        return self._get("stock/earnings", data=params)




    

    def exchange(self):
        return self._get("stock/exchange")

    def stock_symbol(self, **params):
        return self._get("stock/symbol", data=params)

    def quote(self, **params):
        return self._get("quote", data=params)

    def stock_candle(self, **params):
        return self._get("stock/candle", data=params)

    def stock_tick(self, **params):
        return self._get("stock/tick", data=params)

    def forex_exchange(self):
        return self._get("forex/exchange")

    def forex_symbol(self, **params):
        return self._get("forex/symbol", data=params)

    def forex_candle(self, **params):
        return self._get("forex/candle", data=params)

    def crypto_exchange(self):
        return self._get("crypto/exchange")

    def crypto_symbol(self, **params):
        return self._get("crypto/symbol", data=params)

    def crypto_candle(self, **params):
        return self._get("crypto/candle", data=params)

    def scan_pattern(self, **params):
        return self._get("scan/pattern", data=params)

    def scan_support_resistance(self, **params):
        return self._get("scan/support-resistance", data=params)

    def scan_technical_indicator(self, **params):
        return self._get("scan/technical-indicator", data=params)



    def merger_country(self):
        return self._get("merger/country")

    def merger(self, **params):
        return self._get("merger", data=params)

    def economic_code(self):
        return self._get("economic/code")

    def economic(self, **params):
        return self._get("economic", data=params)

    def calendar_economic(self):
        return self._get("calendar/economic")

    def calendar_earnings(self):
        return self._get("calendar/earnings")

    def calendar_ipo(self, **params):
        return self._get("calendar/ipo", data=params)

    def calendar_ico(self):
        return self._get("calendar/ico")
    