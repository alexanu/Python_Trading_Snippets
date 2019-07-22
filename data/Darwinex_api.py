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
        """
        Set api url (demo or real) and call _set_token_into_default_headers.
        Keyword arguments:
            user (str) -- darwinex user name.
            password (str) -- darwinex user password.
            demo (bool) -- indicate to use demo account or real account (default: True).
            device_id (str) -- android device id (default: '').
            apikey (str) -- darwinex apikey (default: current mobile app apikey ('a5fe75ff729a251cbbd4f11ee541c2c4')).
        """
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
        """
        Process API response after receiving.
        Keyword arguments:
            response (Response <Response> object) -- darwinex api response.
        """
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
        """
        Process API request before sending.
        Keyword arguments:
            url (str) -- relative URL to send.
            method (str) -- HTTP method to use.
                Values can take: ['get', 'post', 'put', 'patch', 'delete']
            params (dict) -- params to be sent in the query string for the.
            data (dict) -- data to send in the body of the HTTP request.
            headers (dict) -- HTTP Headers to send in the HTTP request.
        """
        if not headers:
            headers = self.default_headers
        if data:
            headers = headers.copy()
            headers.update({'Content-Type': 'application/json; charset=UTF-8',
                            'Content-Length': str(len(str(data).replace(" ", ""))),
                            })
            data = json.dumps(data)
        response = requests.request(method=method, url='{}{}'.format(self.base_url, url), params=params, data=data,
                                    headers=headers)
        return self._process_response(response)

    ############
    # AUTH API #
    ############

    def login(self):
        """
        Get authentication token for given user
        Response:
            {"authtoken":"eyJjdHk..."}
        Errors:
            If user or password is not correct it returns an error:
                {"errors": [{"code": "USER_INVALID_CREDENTIALS", "message": "Invalid credentials.", "value": ""}]}
            Also, if API Key is not correct it returns an error:
                {"errors": [{"code": "AUTH_INVALID_API_KEY", "message": "Invalid api key.", "value": ""}]}
        """
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
        """
        Register new user.
        Keyword arguments:
            email (str) -- email for the new user.
            username (str) -- username for the new user.
            password (str) -- password for the new user. Should be between 8 and 20 characters.
            device_id (str) -- android uuid (example: dc714af6-6acb-2bb1-284b-5cee8c4cac7e).
            locale (str) -- language for the user (default: 'en').
                Values can take: ['en', 'es'...]
        Response:
            {"authtoken":"eyJjdHk..."}
        Errors:
            If device id is not valid returns an error:
                {"errors":[{"code":"INVALID_DEVICE_ID","message":"A device id is required.","value":""}]}
            If user already exists returns an error:
                {"errors":[{"code":"EXISTING_USERNAME","message":"Username XXXXX already in use","value":""}]}
            If email already exists returns an error:
                {"errors":[{"code":"EXISTING_EMAIL","message":"Email XXXXX already in use.","value":""}]}
            If password is too short or too long returns an error:
                {"errors":[{"code":"INVALID_PASSWORD","message":"Password must be between 8 and 20 characters long.","value":""}]}
            If email has not the correct format, it returns an error:
                {"errors":[{"code":"INVALID_EMAIL","message":"Malformed email address.","value":""}]}
        """
        url = '/auth/register'

        data = {"deviceId": device_id,
                "email": email,
                "locale": locale,
                "username": username,
                "password": password}

        headers = self.default_headers.copy()
        headers.update({'Authorization': 'ApiKey {}'.format(self._apikey)})
        response = self._process_request(url=url, method='post', data=data, headers=headers)
        return response

    # TODO: recover_password method requires a solved captcha (recaptcha) and don't know exactly where it comes from and
    # how to generate it.

    # def recover_password(self, email):
    #     """
    #     Recover password for a given email.
    #
    #     Keyword arguments:
    #         email(str) -- email to recover password from
    #
    #     Response:
    #         ¿?
    #
    #     Errors:
    #         If captcha is not correct returns an error:
    #           {"errors":[{"code":"AUTH_UNVERIFIED_CAPTCHA","message":"Captcha not verified.","value":""}]}
    #
    #         If email is not registered returns an error:
    #           ...
    #
    #     """
    #     url = '/auth/recover_password'
    #
    #     data = {'email': email}
    #
    #     headers = self.default_headers.copy()
    #     headers.update({'Authorization': 'ApiKey {}'.format(self._apikey)})
    #     response = self._process_request(url=url, method='post', data=data, headers=headers)
    #     return response

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
        """
        Search darwins from a given string.
        Keyword arguments:
            query (str) -- string to search for.
            position (int) -- ¿? (default 0).
            limit (int)-- limit the number of results. Should be an integer in 0 < limit < 51. (default 10).
        Response:
            [{'dscore': 36.97275847626485, 'product_name': 'AVT.4.5'},
             {'dscore': 31.815479827061864, 'product_name': 'UVT.4.4'},
             {'dscore': 16.966121229717753, 'product_name': 'VTE.4.20'},
             {'dscore': 31.126748486658247, 'product_name': 'VTF.4.7'},
             {'dscore': 36.844157237422365, 'product_name': 'VTG.4.3'},
             {'dscore': 72.18839778980019, 'product_name': 'VTJ.4.2'},
             {'dscore': 62.82766752349335, 'product_name': 'VTS.4.6'}]
        Errors:
            If limit is minor than 1, it returns an error:
                {"errors":[{"code":"BAD_REQUEST","message":"Page size must not be less than one!","value":""}]}
            If limit is bigger than 50, it returns an error:
                {"errors":[{"code":"BAD_REQUEST","message":"The number of elements should be greater than 0 and less than 51","value":""}]}
        """
        url = '/search/darwin'
        params = {'query': query, 'position': position, 'limit': limit}

        response = self._process_request(url=url, method='get', params=params)
        return response

    ##############
    # DARWIN API #
    ##############

    def darwin_quote(self, name, zoom=''):
        """
        Get a darwin quote.
        Keyword arguments:
            name (str) -- darwin name.
            zoom (str) -- search period (default '' - current moment).
                Values can take: ['', '1D', '5D', '7D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '3Y', '5Y', '10Y', 'ALL']
        Response:
            If no Zoom is provided:
                {'quote': 200.70834726686027, 'timestamp': 1520632740074}
            If Zoom is provided:
                {'chart': {'max': 1.8,
                           'min': -1.81,
                           'values': [0.0,
                                      0.0,
                                      ...
                                      0.0,
                                      -0.0008267883769645966]
                           },
                 'quote': 200.70834726686027,
                 'return': -0.0008267883769645966,
                 'zoom': '1D'}
        Errors:
            If darwin name is not correct it returns an error:
                {"errors":[{"code":"BAD_REQUEST","message":"No handler found for GET /api-demo/darwin/VTJ.2/statistics/1D","value":"/api-demo/darwin/VTJ.2/statistics/1D"}]}
            If zoom is not a valid it returns an error:
                {"errors":[{"code":"BAD_REQUEST","message":"No enum constant com.tradeslide.domain.Zoom._XX","value":""}]}
        """
        url = '/darwin/{}/quote/{}'.format(name, zoom)

        response = self._process_request(url=url, method='get')
        return response

    def darwin_statistics(self, name, zoom='', locale='en'):
        """
        Get statistics from a darwin.
        Keyword arguments:
            name (str) -- darwin name.
            zoom (str) -- search period (default '' - current moment).
                Values can take: ['', '1D', '5D', '7D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '3Y', '5Y', '10Y', 'ALL']
            locale (str) -- language to show the descriptions (default: 'en').
                Values can take: ['en', 'es'...]
        Response:
            {'americanSession': 34.52012383900929,
             'asianSession': 30.14705882352941,
             'behaviour': {'max': 0.07391640866873066,
                           'min': 0.01586687306501548,
                           'values': [0.06424148606811146,
                                      0.03831269349845201,
                                      ...
                                      0.04063467492260062]
                           },
             'currency': 'EUR',
             'divergence': 0.04990866984930786,
             'drawdown': -16.128227904713277,
             'europeanSession': 35.3328173374613,
             'filters': [{'avatar': 'aguila',
                          'description': 'En racha con experiencia (ideal para estrategia a días)',
                          'id': 6433,
                          'name': 'Racha + exp',
                          'type': 'USER'},
                         {'avatar': 'TOP_INVESTORS',
                          'description': 'Éstos son los 20 DARWINS que están siendo replicados por un mayor número de inversores en la actualidad.',
                          'id': 1,
                          'name': 'Más inversores',
                          'type': 'PREDEFINED'},
                          ...
                          {'avatar': 'HIGH_RETURN',
                           'description': 'Incluye DARWINs que han ganado más de un 50% desde que fueron creados y que, por tanto, tienen una cotización superior a 150.',
                           'id': 3,
                           'name': 'Retorno > 50%',
                           'type': 'PREDEFINED'}],
             'investorsCapital': 1728641.241194325,
             'losingPosition': 23.555829971129267,
             'numberOfTrades': 1292,
             'return': 100.70834726686027,
             'timeOpenTrades': None,
             'tradeAllocation': [{'allocation': 0.016253869969040248, 'asset': 'EURGBP'},
                                 {'allocation': 0.025541795665634675, 'asset': 'USDCAD'},
                                 ...
                                 {'allocation': 0.206656346749226, 'asset': 'GBPUSD'}],
             'tradeDuration': 27238087.82120743,
             'traderEquity': 4999.08,
             'tradingScore': 72.18839778980019,
             'winningDays': 38.66877971473851,
             'winningPosition': 18.217251342850307,
             'winningTrades': 61.45510835913313,
             'winningWeeks': 53.72424722662441,
             'zoom': 'ALL'}
        Errors:
            If zoom is not a valid it returns an error:
                {"errors":[{"code":"BAD_REQUEST","message":"No enum constant com.tradeslide.domain.Zoom._XX","value":""}]}
            If darwin name is not correct it returns an error:
                {"errors":[{"code":"BAD_REQUEST","message":"No handler found for GET /api-demo/darwin/VTJ.2/statistics/1D","value":"/api-demo/darwin/VTJ.2/statistics/1D"}]}
        """
        url = '/darwin/{}/statistics/{}'.format(name, zoom)
        params = {'locale': locale}

        response = self._process_request(url=url, method='get', params=params)
        return response

    ################
    # INVESTOR API #
    ################

    @property
    def balance(self):
        """
        Get investor balance.
        Response:
            {'available': 0.02,
             'availableToInvest': 0.04,
             'equity': 603.45,
             'wallet': 0.0}
        """
        url = '/investor/balance'

        response = self._process_request(url=url, method='get')
        return response

    @property
    def portfolio_summary(self):
        """
        Get investor portfolio summary.
        Response:
            {'available': 0.02,
             'availableToInvest': 0.04,
             'closedPnL': -83.54,
             'equity': 603.43,
             'equityAtRisk': 64.97,
             'invested': 1232.64,
             'maxRisk': 80.0,
             'openPnL': -12.9,
             'pfees': 2.7795284357312826,
             'rebates': 2.66}
        """
        url = '/investor/portfolio/summary'

        response = self._process_request(url=url, method='get')
        return response

    @property
    def portfolio_distribution(self):
        """
        Get investor portfolio distribution.
        Response:
            [{'averageQuote': 254.79,
              'currentQuote': 256.69,
              'invested': 342.69,
              'openPnL': 2.71,
              'product': 'DLF.4.7'},
             {'averageQuote': 206.3,
              'currentQuote': 200.71,
              'invested': 221.14,
              'openPnL': -6.98,
              'product': 'VTJ.4.2'},
             {'averageQuote': 216.13,
              'currentQuote': 219.4,
              'invested': 281.75,
              'openPnL': 4.68,
              'product': 'JZH.4.13'},
             {'averageQuote': 285.8,
              'currentQuote': 276.14,
              'invested': 387.07,
              'openPnL': -13.36,
              'product': 'QUA.4.3'}]
        """
        url = '/investor/portfolio/distribution'

        response = self._process_request(url=url, method='get')
        return response

    def portfolio(self, zoom='1D'):
        """
        Get investor portfolio.
        Keyword arguments:
            zoom (str) -- search period (default '1D' - last day).
                Values can take: ['', '1D', '5D', '7D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '3Y', '5Y', '10Y', 'ALL']
        Response:
            {'chart':
                {'max': 1.42,
                 'min': -1.77,
                 'values': [0.0,
                            -0.0011999832106611736,
                            0.0017999747228675566,
                            0.06129905210644457,
                            ...
                            -0.3468211062916695]
                },
             'return': -0.3468211062916695,
             'zoom': '6M'}
        """
        url = '/investor/portfolio/{}'.format(zoom)

        response = self._process_request(url=url, method='get')
        return response

    @property
    def account(self):
        """
        Get investor account.
        Response:
            {'available': 0.02,
             'currency': 'EUR',
             'equity': 603.36,
             'risk': 80.0}
        """
        url = '/investor/account'

        response = self._process_request(url=url, method='get')
        return response

    def change_risk(self, amount):
        """
        Change risk.
        Keyword arguments:
            amount (num) -- new risk value.
        Response:
            {'risk': 79.0}
        Errors:
            If risk value is too high it returns an error:
                ...
        """
        url = '/investor/account'
        params = {'operation': 'change_risk'}
        data = {'amount': amount}

        response = self._process_request(url=url, method='put', params=params, data=data)
        return response

    def add_funds(self, amount):
        """
        Add funds.
        Keyword arguments:
            amount (num) -- funds to add.
        Response:
            {'available': 8775.23,
             'availableToInvest': 17550.46,
            'equity': 9995.07,
            'wallet': 0.0}
        Errors:
            If amount is bigger than available to invest it returns an error:
                {"errors":[{"code":"ACCOUNT_FUNDS_INVALID_VOLUME","message":"Bad Request","value":""}]}
        """
        url = '/investor/account'
        params = {'operation': 'add_funds'}
        data = {'amount': amount}

        response = self._process_request(url=url, method='put', params=params, data=data)
        return response

    def withdraw_funds(self, amount):
        """
        Withdraw funds.
        Keyword arguments:
            amount (num) -- funds to withdraw.
        Response:
            [WAITING TO TEST IN REAL ACCOUNT]
        If you try to withdraw funds in demo account it returns an error:
            {"errors":[{"code":"BAD_REQUEST","message":"Funds withdrawal operation is not available for DEMO accounts.","value":""}]}
        If amount is bigger than available to withdraw it returns an error:
            [WAITING TO TEST IN REAL ACCOUNT]
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
        Response:
            {'darwin_availability':
                {'availableOperations': ['BUY', 'SELL'],
                'status': 'ACTIVE'},
             'investment':
                {'averageQuote': 285.8,
                 'closedPnL': -39.43,
                 'divergence': -0.07,
                 'highwatermark': 0.0,
                 'invested': 387.07,
                 'openPnL': -13.36,
                 'paidPfees': 0.0,
                 'pendingPfees': 0.0,
                 'quarterEnds': 18},
             'investor_balance':
                {'available': 0.02,
                 'availableToInvest': 0.04,
                 'equity': 603.37,
                 'wallet': 0.0}
            }
        """
        url = '/investor/investment/{}'.format(name)

        response = self._process_request(url=url, method='get')
        return response

    def buy(self, name, amount):
        """
        Buy a darwin.
        Keyword arguments:
            name (str) -- darwin name.
            amount (num) -- quantity to invest in the darwin.
        Response:
            {'price': 200.71}
        Errors:
            If the amount is bigger than the available equity it returns the next error:
                {"errors":[{"code":"INVESTMENT_NOT_ENOUGH_FUNDS","message":"NOT_ENOUGH_FUNDS","value":""}]}
            If amount is less than the minimum order volume it returns the next error:
                {"errors":[{"code":"INVESTMENT_INVALID_ORDER_VOLUME","message":"INVALID_AMOUNT","value":"25.00"}]}
            If market is closed:
                {"errors":[{"code":"INVESTMENT_MARKET_CLOSED","message":"MARKET_CLOSED","value":""}]}
        """
        url = '/investor/investment/{}'.format(name)
        params = {'operation': 'buy'}
        data = {'amount': amount}

        response = self._process_request(url=url, method='post', params=params, data=data)
        return response

    def sell(self, name, amount):
        """
        Sell a darwin.
        Note: If the amount is bigger than the invested volume, it works, and all darwin is sold.
        Keyword arguments:
            name (str) -- darwin name.
            amount (num) -- quantity to sell from the darwin.
        Response:
            {'price': 200.71}
        Errors:
            If the amount is less than the minimum order volume it returns an error:
                {"errors":[{"code":"INVESTMENT_INVALID_ORDER_VOLUME","message":"INVALID_AMOUNT","value":"25.00"}]}
            If market is closed:
                {"errors":[{"code":"INVESTMENT_MARKET_CLOSED","message":"MARKET_CLOSED","value":""}]}
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
        """
        Get filters.
        Keyword arguments:
            locale (str) -- language to show the descriptions (default: 'en').
                Values can take: ['en', 'es'...]
        Response:
            [{'avatar': 'TOP_INVESTORS',
              'description': 'Éstos son los 20 DARWINS que están siendo replicados por un mayor número de inversores en la actualidad.',
              'id': 1,
              'name': 'Más inversores',
              'type': 'PREDEFINED'},
             {'avatar': 'GOOD_SCORES',
              'description': 'Incluye los DARWINS que tienen buenas puntuaciones en todos los atributos (no se tiene en cuenta la escalabilidad).',
              'id': 2,
              'name': 'Buenas Notas',
              'type': 'PREDEFINED'},
              ...
             {'avatar': 'COMMUNITY',
              'description': 'Este filtro incluye DARWINS cuya operativa depende de los datos internos generados por todas las estrategias de trading de Darwinex.',
              'id': 9,
              'name': 'DARWINS de la Comunidad',
              'type': 'PREDEFINED'}]
        """
        url = '/filter'
        params = {'locale': locale}

        response = self._process_request(url=url, method='get', params=params)
        return response

    def darwins_in_filter(self, filter_id, filter_type, order='RETURN', position=1, limit=5, zoom='2Y', locale='en'):
        """
        Get darwins in filter
        Keyword arguments:
            filter_id (str) -- filter id.
            filter_type -- filter type.
                Values can take: ['USER', 'PREDEFINED']
            order (str) -- result order (default 'RETURN').
                Values can take: ['RETURN', 'DRAWDOWN', 'RETURN_DRAWDOWN', 'TS_SCORE', 'CURRENT_INVESTMENT']
            position (int) -- ¿? (default 1).
            limit (int)-- limit the number of results. Should be an integer in 0 < limit < 51. (default 10)
            zoom (str) -- search period (default '2Y' - 2 last years).
                Values can take: ['', '1D', '5D', '7D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '3Y', '5Y', '10Y', 'ALL']
            locale (str) -- language to show the descriptions (default: 'en').
                Values can take: ['en', 'es'...]
       Response:
            [{'chart': {'max': 117.32,
              'min': -1.02,
              'values': [0.0,
                0.04825200819188967,
                0.012065302830358069,
                ...
                120.13659647844703]},
            'drawdown': -12.30174117147821,
            'investment': 133544.554951325,
            'product': 'BDR.4.4',
            'rank': 0,
            'return': 120.13659647844706,
            'traderScore': 59.351300770969715}]
        """
        url = '/filter/darwin'
        params = {'filter': filter_id,
                  'type': filter_type,
                  'order': order,
                  'locale': locale,
                  'zoom': zoom,
                  'position': position,
                  'limit': limit,
                  }

        response = self._process_request(url=url, method='get', params=params)
return response
