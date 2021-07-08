'''
uses the Darwinex for investors mobile app API
How to use:
# d = DarwinexAPIClient(user='...', password='...')
# d = DarwinexAPIClient(user='...', password='...', demo=False) # by default the client uses the demo account
d.me
d.balance
d.search('QU')
d.darwin_quote('QUA.4.3')
d.buy('VTJ.4.2', 25)
'''

import json
import requests

class DarwinexAPIClientException(Exception):
    def __init__(self, message):
        super().__init__(message)

class DarwinexAPIClient:
    default_headers = {'User-Agent': 'Android-Darwinex-Investors',
                       'Host': 'app.darwinex.com',
                       'Connection': 'Keep-Alive',
                       'Accept-Encoding': 'gzip'}

    def __init__(self, user, password, demo=True, device_id="", apikey='a5fe75ff729a251cbbd4f11ee541c2c4'):
        self.user = user
        self.password = password
        self._apikey = apikey
        self.device_id = device_id
        if demo:
            self.base_url = 'https://app.darwinex.com/api-demo'
            print('Using DEMO account. To use real account add demo=False.')
        else:
            self.base_url = 'https://app.darwinex.com/api'
            print('Using REAL account. Use at YOUR OWN RISK!')
        self._set_token_into_default_headers()

    def _process_response(self, response):
        """ Process API response after receiving """
        try:
            result = response.json()
        except ValueError:
            result = response.content

        try:
            response.raise_for_status()
        except Exception as e:
            message = ''

            try:
                errors = result.get('errors', [])
                for error in errors:
                    code = error.get('code')
                    msg = error.get('message')
                    value = error.get('value')

                    if code:
                        message += 'Code: {}. '.format(code)
                    if msg:
                        message += 'Message: {} '.format(msg)
                    if value:
                        message += 'Value: {} '.format(value)
            except AttributeError:
                print(result)

            exception = message if message else e
            raise DarwinexAPIClientException(exception)

        return result

    def _process_request(self, url, method='get', params=None, data=None, headers=None):
        """ Process API request before sending """
        if not headers: # HTTP Headers to send in the HTTP request
            headers = self.default_headers
        if data: # data to send in the body of the HTTP request
            headers = headers.copy()
            headers.update({'Content-Type': 'application/json; charset=UTF-8',
                            'Content-Length': str(len(str(data).replace(" ", ""))),
                            })
            data = json.dumps(data)
        response = requests.request(method=method, # Values can take: ['get', 'post', 'put', 'patch', 'delete']
                                    url='{}{}'.format(self.base_url, url), 
                                    params=params, data=data,
                                    headers=headers)
        return self._process_response(response)

    ############
    # AUTH API #
    ############

    def login(self):
        """ Get authentication token for given user """
        url = '/auth/token'
        data = {"deviceId": self.device_id,
                "username": self.user,
                "password": self.password}

        # Note: It could be possible that app show a captcha and should be added the "captcha_response" in as a get
        # param. However I have never seen that case.

        headers = self.default_headers.copy()
        headers.update({'Authorization': 'ApiKey {}'.format(self._apikey)})
        response = self._process_request(url=url, method='post', data=data, headers=headers)
        return response

    def _set_token_into_default_headers(self):
        """
        Set darwinex user token into default headers to avoid build it every time.
        """
        login_info = self.login()

        try:
            token = 'DxToken {}'.format(login_info['authtoken'])
            self.default_headers.update({'Authorization': token})

        except KeyError:
            if login_info.get('errors'):
                print(login_info.get('errors'))

    def logout(self):
        """
        Logout. It makes Token invalid.
        Note: To set again the token in the default headers you should use: _set_token_into_default_headers() or
        instantiate another DarwinexAPIClient object.
        Response:
            ''
        """
        url = '/logout'

        response = self._process_request(url=url, method='post')
        return response

    def register(self, email, username, password, device_id, locale="en"):
        """ Register new user """
        url = '/auth/register'
        data = {"deviceId": device_id, # android uuid (example: dc714af6-6acb-2bb1-284b-5cee8c4cac7e)
                "email": email,
                "locale": locale,
                "username": username,
                "password": password} # Should be between 8 and 20 characters
        headers = self.default_headers.copy()
        headers.update({'Authorization': 'ApiKey {}'.format(self._apikey)})
        response = self._process_request(url=url, method='post', data=data, headers=headers)
        return response

    @property
    def me(self):
        """
        Get personal info.
        Response:
            {"me" : "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
             "investorAccount": {"type":"REAL","currency":"EUR"},
             "user": {"username":"XXXXX"}}
        """
        url = '/auth/me'
        response = self._process_request(url=url, method='get')
        return response

    ##############
    # SEARCH API #
    ##############

    def search(self, query, position=0, limit=10):
        """ Search darwins from a given string. """
        url = '/search/darwin'
        params = {'query': query, 'position': position, 'limit': limit}
        # limit (int)-- limit the number of results. Should be an integer in 0 < limit < 51. (default 10)
        response = self._process_request(url=url, method='get', params=params)
        return response

    ##############
    # DARWIN API #
    ##############

    def darwin_quote(self, name, zoom=''):
        url = '/darwin/{}/quote/{}'.format(name, zoom)
        response = self._process_request(url=url, method='get')
        return response

    def darwin_statistics(self, name, zoom='', locale='en'):
        url = '/darwin/{}/statistics/{}'.format(name, zoom)
        params = {'locale': locale}
        response = self._process_request(url=url, method='get', params=params)
        return response

    ################
    # INVESTOR API #
    ################

    @property
    def balance(self):
        """ Get investor balance."""
        url = '/investor/balance'
        response = self._process_request(url=url, method='get')
        return response

    @property
    def portfolio_summary(self):
        url = '/investor/portfolio/summary'
        response = self._process_request(url=url, method='get')
        return response

    @property
    def portfolio_distribution(self):
        url = '/investor/portfolio/distribution'
        response = self._process_request(url=url, method='get')
        return response

    def portfolio(self, zoom='1D'):
        """
        Get investor portfolio.
        Keyword arguments:
            zoom (str) -- search period (default '1D' - last day).
                Values can take: ['', '1D', '5D', '7D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '3Y', '5Y', '10Y', 'ALL']
        """
        url = '/investor/portfolio/{}'.format(zoom)
        response = self._process_request(url=url, method='get')
        return response

    @property
    def account(self):
        """
        Get investor account.
        """
        url = '/investor/account'
        response = self._process_request(url=url, method='get')
        return response

    def change_risk(self, amount):
        url = '/investor/account'
        params = {'operation': 'change_risk'}
        data = {'amount': amount}
        response = self._process_request(url=url, method='put', params=params, data=data)
        return response

    def add_funds(self, amount):
        url = '/investor/account'
        params = {'operation': 'add_funds'}
        data = {'amount': amount}
        response = self._process_request(url=url, method='put', params=params, data=data)
        return response

    def withdraw_funds(self, amount):
        """
        Withdraw funds.
        """
        url = '/investor/account'
        params = {'operation': 'withdraw_funds'}
        data = {'amount': amount}
        response = self._process_request(url=url, method='put', params=params, data=data)
        return response

    def get_investment(self, name):
        """
        Get investor investment in a darwin.
        Keyword arguments:
            name (str) -- darwin name.
        """
        url = '/investor/investment/{}'.format(name)
        response = self._process_request(url=url, method='get')
        return response

    def buy(self, name, amount):
        url = '/investor/investment/{}'.format(name)
        params = {'operation': 'buy'}
        data = {'amount': amount}
        response = self._process_request(url=url, method='post', params=params, data=data)
        return response

    def sell(self, name, amount):
        """
        Sell a darwin.
        Note: If the amount is bigger than the invested volume, it works, and all darwin is sold.
        """
        url = '/investor/investment/{}'.format(name)
        params = {'operation': 'sell'}
        data = {'amount': amount}
        response = self._process_request(url=url, method='post', params=params, data=data)
        return response

    ##############
    # FILTER API #
    ##############

    def get_filters(self, locale='en'):
        url = '/filter'
        params = {'locale': locale}
        response = self._process_request(url=url, method='get', params=params)
        return response

    def darwins_in_filter(self, filter_id, filter_type, order='RETURN', position=1, limit=5, zoom='2Y', locale='en'):
        """
        Get darwins in filter
        """
        url = '/filter/darwin'
        params = {'filter': filter_id,
                  'type': filter_type, # Values can take: ['USER', 'PREDEFINED']
                  'order': order, # Values can take: ['RETURN', 'DRAWDOWN', 'RETURN_DRAWDOWN', 'TS_SCORE', 'CURRENT_INVESTMENT']
                  'locale': locale,
                  'zoom': zoom, # Values can take: ['', '1D', '5D', '7D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '3Y', '5Y', '10Y', 'ALL']
                  'position': position,
                  'limit': limit, # integer in 0 < limit < 51. (default 10)
                  }
        response = self._process_request(url=url, method='get', params=params)
        return response
