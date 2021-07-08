from alpha_vantage import *
from pandas import DataFrame as df
import unittest


class TestAlphaVantage(unittest.TestCase):
    _API_KEY_TEST = '84WDI082Z0HOREL6'
    _EQ_NAME_TEST = 'MSFT'
    _CRYPTO_NAME_TEST = 'XRP'
    _FIAT_NAME_TEST = "EUR"

    # Errors definitions
    _ERROR_MUST_BE_DICTIONNARY = "Result data must be a dictionnary"
    _ERROR_MUST_BE_PANDAS = "Result data must be Pandas data frame to be used"
    _ERROR_MUST_BE_JSON = "Result data must be in JSON format"

    def test_key_none(self):
        """Raise an error when a key has not been given
        """
        try:
            AlphaVantage()
            self.fail(msg='A None api key must raise an error')
        except ValueError:
            self.assertTrue(True)

    def test_handle_api_call(self):
        """ Test that api call returns a json file as requested
        """
        av = AlphaVantage(key=TestAlphaVantage._API_KEY_TEST)
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=1min&apikey=test"
        data = av._handle_api_call(url)
        self.assertIsInstance(data, dict, TestAlphaVantage._ERROR_MUST_BE_DICTIONNARY)

    def test_time_series_intraday(self):
        """ Test that api call returns a json file as requested
        """
        ts = TimeSeries(key=TestAlphaVantage._API_KEY_TEST)
        data, _ = ts.get_intraday(TestAlphaVantage._EQ_NAME_TEST, interval='1min', outputsize='full')
        self.assertIsInstance(data, dict, TestAlphaVantage._ERROR_MUST_BE_DICTIONNARY)

    def test_time_series_intraday_pandas(self):
        """ Test that api call returns a json file as requested
        """
        ts = TimeSeries(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas')
        data, _ = ts.get_intraday(TestAlphaVantage._EQ_NAME_TEST, interval='1min', outputsize='full')
        self.assertIsInstance(data, df, TestAlphaVantage._ERROR_MUST_BE_PANDAS)

    def test_time_series_intraday_date_indexing(self):
        """ Test that api call returns a pandas data frame with a date as index
        """
        ts = TimeSeries(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas', indexing_type='date')
        data, _ = ts.get_intraday(TestAlphaVantage._EQ_NAME_TEST, interval='1min', outputsize='full')
        assert isinstance(data.index[0], str)

    def test_time_series_intraday_date_integer(self):
        """ Test that api call returns a pandas data frame with an integer as index
        """
        ts = TimeSeries(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas', indexing_type='integer')
        data, _ = ts.get_intraday(TestAlphaVantage._EQ_NAME_TEST, interval='1min', outputsize='full')
        assert type(data.index[0]) == int

    def test_technical_indicator_sma_python3(self):
        """ Test that api call returns a json file as requested
        """
        ti = TechIndicators(key=TestAlphaVantage._API_KEY_TEST)
        data, _ = ti.get_sma(TestAlphaVantage._EQ_NAME_TEST, interval='15min', time_period=10, series_type='close')
        self.assertIsInstance(data, dict, TestAlphaVantage._ERROR_MUST_BE_DICTIONNARY)

    def test_technical_indicator_sma_pandas(self):
        """ Test that api call returns a json file as requested
        """
        ti = TechIndicators(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas')
        data, _ = ti.get_sma(TestAlphaVantage._EQ_NAME_TEST, interval='15min', time_period=10, series_type='close')
        self.assertIsInstance(data, df, TestAlphaVantage._ERROR_MUST_BE_PANDAS)

    def test_sector_perfomance(self):
        """ Test that api call returns a json file as requested
        """
        sp = SectorPerformances(key=TestAlphaVantage._API_KEY_TEST)
        data, _ = sp.get_sector()
        self.assertIsInstance(data, dict, TestAlphaVantage._ERROR_MUST_BE_DICTIONNARY)

    def test_sector_perfomance_pandas(self):
        """ Test that api call returns a json file as requested
        """
        sp = SectorPerformances(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas')
        data, _ = sp.get_sector()
        self.assertIsInstance(data, df, TestAlphaVantage._ERROR_MUST_BE_PANDAS)

    def test_foreign_exchange(self):
        """ Test that api call returns a json file as requested
        """
        fe = ForeignExchange(key=TestAlphaVantage._API_KEY_TEST)
        data, _ = fe.get_currency_exchange_rate(from_currency=TestAlphaVantage._CRYPTO_NAME_TEST,
                                                to_currency=TestAlphaVantage._FIAT_NAME_TEST)
        self.assertIsInstance(data, dict, TestAlphaVantage._ERROR_MUST_BE_DICTIONNARY)

    def test_crypto_currencies_pandas(self):
        """ Test that api call returns a json file as requested
        """
        cc = CryptoCurrencies(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas')
        data, _ = cc.get_digital_currency_daily(symbol=TestAlphaVantage._CRYPTO_NAME_TEST,
                                                market=TestAlphaVantage._FIAT_NAME_TEST)
        self.assertIsInstance(data, df, TestAlphaVantage._ERROR_MUST_BE_PANDAS)

    def test_batch_quotes(self):
        """ Test that api call returns a json file as requested
        """
        ts = TimeSeries(key=TestAlphaVantage._API_KEY_TEST)
        data, _ = ts.get_batch_stock_quotes(symbols=('MSFT', 'FB', 'AAPL'))
        self.assertIsInstance(data[0], dict, TestAlphaVantage._ERROR_MUST_BE_JSON)

    def test_batch_quotes_pandas(self):
        """ Test that api call returns a json file as requested
        """
        ts = TimeSeries(key=TestAlphaVantage._API_KEY_TEST, output_format='pandas')
        data, _ = ts.get_batch_stock_quotes(symbols=('MSFT', 'FB', 'AAPL'))
        self.assertIsInstance(data, df, TestAlphaVantage._ERROR_MUST_BE_PANDAS)


if __name__ == '__main__':
    unittest.main()
