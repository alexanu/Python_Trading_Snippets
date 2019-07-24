class QuandlApi(ExternalDataApi):

    @staticmethod
    def retrieve_data(data_category=None,
                      start_date=dt.datetime.today(),
                      end_date=dt.datetime.today(),
                      options_dict=None):

        # Any external data API should implement a generic retrieve_data
        # method that suppors all relevant data retrieval
        raise NotImplementedError

    @staticmethod
    def get_equity_index_universe():
        return ['DOW', 'SPX', 'NDX', 'NDX100', 'UKX']

    @staticmethod
    def get_futures_universe():
        quandl_futures = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Futures/meta.csv'

    @staticmethod
    def get_currency_universe():
        quandl_currencies = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/currencies.csv'

    @staticmethod
    def get_equity_universe(index_ticker):

        quandl_universe = {'DOW': 'https://s3.amazonaws.com/static.quandl.com/tickers/dowjonesA.csv',
                           'SPX': 'https://s3.amazonaws.com/static.quandl.com/tickers/SP500.csv',
                           'NDX': 'https://s3.amazonaws.com/static.quandl.com/tickers/NASDAQComposite.csv',
                           'NDX100': 'https://s3.amazonaws.com/static.quandl.com/tickers/nasdaq100.csv',
                           'UKX': 'https://s3.amazonaws.com/static.quandl.com/tickers/FTSE100.csv'}

        quandl_filenames = {'DOW': 'dowjonesA.csv',
                            'SPX': 'SP500.csv',
                            'NDX': 'NASDAQComposite.csv',
                            'NDX100': 'nasdaq100.csv',
                            'UKX': 'FTSE100.csv'}

        tickers = QuandlApi.retrieve_universe(path=quandl_universe[index_ticker],
                                              filename=quandl_filenames[index_ticker])

        return tickers

    @staticmethod
    def retrieve_universe(path, filename):
        opener = urllib.URLopener()
        target = path
        opener.retrieve(target, filename)
        tickers_file = pd.read_csv(filename)
        tickers = tickers_file['ticker'].values
        return tickers
