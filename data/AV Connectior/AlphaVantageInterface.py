import os
import sys
import inspect
import logging
import traceback
from enum import Enum
import datetime as dt
import time
import functools
import threading
from alpha_vantage.timeseries import TimeSeries

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from Utils.Utils import Markets


class AVInterval(Enum):
    """
    AlphaVantage interval types: '1min', '5min', '15min', '30min', '60min', 'daily', 'weekly and 'monthly'
    """

    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_15 = "15min"
    MIN_30 = "30min"
    MIN_60 = "60min"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


# Mutex used for thread synchronisation
lock = threading.Lock()


def synchronised(lock):
    """ Thread synchronization decorator """

    def wrapper(f):
        @functools.wraps(f)
        def inner_wrapper(*args, **kw):
            with lock:
                return f(*args, **kw)

        return inner_wrapper

    return wrapper


class Singleton(type):
    """Metaclass to implement the Singleton desing pattern"""

    _instances = {}

    @synchronised(lock)
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AlphaVantageInterface(metaclass=Singleton):
    """class providing interfaces to request data from AlphaVantage"""

    def __init__(self, config):
        self._config = config
        self._last_call_ts = dt.datetime.now()
        self._TS = TimeSeries(
            key=config.get_alpha_vantage_api_key(),
            output_format="json",
            treat_info_as_error=True,
        )
        logging.info("AlphaVantageInterface initialised")

    def _daily(self, market_id):
        """
        Calls AlphaVantage API and return the Daily time series for the given market

            - **market_id**: string representing a market ticker
            - Returns **None** if an error occurs otherwise the pandas dataframe
        """
        try:
            market_id = self._format_market_id(market_id)
            data, meta_data = self._TS.get_daily(symbol=market_id, outputsize="compact")
            return data
        except Exception as e:
            logging.error("AlphaVantage wrong api call for {}".format(market_id))
            logging.debug(e)
            logging.debug(traceback.format_exc())
            logging.debug(sys.exc_info()[0])
        return None

    def _format_market_id(self, market_id):
        """
        Convert a standard market id to be compatible with AlphaVantage API.
        Adds the market exchange prefix (i.e. London is LON:)
        """
        if "LSE:" in market_id:
            subs = market_id.split(":")
            assert len(subs) == 2
            return "{}:{}".format(Markets[subs[0]].value, subs[1])
        return market_id

    def _wait_before_call(self):
        """
        Wait between API calls to not overload the server
        """
        while (dt.datetime.now() - self._last_call_ts) <= dt.timedelta(
            seconds=self._config.get_alpha_vantage_polling_period()
        ):
            time.sleep(0.2)
        self._last_call_ts = dt.datetime.now()

    def get_prices(self, market_id, interval=AVInterval.DAILY):
        """
        Return the price time series of the requested market with the interval
        granularity. Return None if the interval is invalid
        """
        self._wait_before_call()
        if interval == AVInterval.DAILY:
            return self._daily(market_id)
        else:
            logging.warning(
                "AlphaVantageInterface supports only DAILY interval. Requested interval: {}".format(
                    interval.value
                )
            )
            return None
