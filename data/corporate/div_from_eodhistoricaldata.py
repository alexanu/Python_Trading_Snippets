import os
import requests
import requests_cache
import pandas as pd
from pandas.io.common import urlencode
from pandas.api.types import is_number
from pandas.compat import StringIO
from ._utils import (_init_session, _format_date,
                     _sanitize_dates, _url, RemoteDataError)

EOD_HISTORICAL_DATA_API_KEY_ENV_VAR = "EOD_HISTORICAL_API_KEY"
EOD_HISTORICAL_DATA_API_KEY_DEFAULT = "OeAFFmMliFG5orCUuwAKQ8l4WWFQ67YX"
EOD_HISTORICAL_DATA_API_URL = "https://eodhistoricaldata.com/api"


def _init_session(session):
    """
    Returns a requests.Session (or CachedSession)
    """
    if session is None:
        return requests.Session()
    return session


def _url(url, params):
    """
    Returns long url with parameters
    http://mydomain.com?param1=...&param2=...
    """
    if params is not None and len(params) > 0:
        return url + "?" + urlencode(params)
    else:
        return url

class RemoteDataError(IOError):
    """
    Remote data exception
    """
    pass


def _format_date(dt):
    """
    Returns formated date
    """
    if dt is None:
        return dt
    return dt.strftime("%Y-%m-%d")


def _sanitize_dates(start, end):
    """
    Return (datetime_start, datetime_end) tuple
    """
    if is_number(start):
        # regard int as year
        start = datetime.datetime(start, 1, 1)
    start = pd.to_datetime(start)

    if is_number(end):
        # regard int as year
        end = datetime.datetime(end, 1, 1)
    end = pd.to_datetime(end)

    if start is not None and end is not None:
        if start > end:
            raise Exception("end must be after start")

    return start, end


def get_api_key(env_var=EOD_HISTORICAL_DATA_API_KEY_ENV_VAR):
    """
    Returns API key from environment variable
    API key must have been set previously
    bash> export EOD_HISTORICAL_API_KEY="YOURAPI"
    Returns default API key, if environment variable is not found
    """
    return os.environ.get(env_var, EOD_HISTORICAL_DATA_API_KEY_DEFAULT)


def get_dividends(symbol, exchange, start=None, end=None,
                  api_key=EOD_HISTORICAL_DATA_API_KEY_DEFAULT,
                  session=None):
    """
    Returns dividends
    """
    symbol_exchange = symbol + "." + exchange
    session = _init_session(session)
    start, end = _sanitize_dates(start, end)
    endpoint = "/div/{symbol_exchange}".format(symbol_exchange=symbol_exchange)
    url = EOD_HISTORICAL_DATA_API_URL + endpoint
    params = {
        "api_token": api_key,
        "from": _format_date(start),
        "to": _format_date(end)
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(StringIO(r.text), skipfooter=1,
                         parse_dates=[0], index_col=0)
        assert len(df.columns) == 1
        ts = df["Dividends"]
        return ts
    else:
        params["api_token"] = "YOUR_HIDDEN_API"
        raise RemoteDataError(r.status_code, r.reason, _url(url, params))


pd.set_option("max_rows", 10)

# Cache session (to avoid too much data consumption)
expire_after = datetime.timedelta(days=1)
session = requests_cache.CachedSession(cache_name='cache', backend='sqlite',
                                       expire_after=expire_after)

# Get API key
#  from environment variable
#  bash> export EOD_HISTORICAL_API_KEY="YOURAPI"
api_key = get_api_key()
# api_key = "YOURAPI"



def test_get_dividends():
    df = get_dividends("AAPL", "US", start="2016-02-01", end="2016-02-10",
                       api_key=api_key, session=session)
    print(df)
    assert df.index.name == "Date"

