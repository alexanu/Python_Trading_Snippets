
from functools import wraps
import logging
import time
import pandas as pd

log = logging.getLogger(__name__)


def retry(tries, exceptions=None, delay=0):
    """
    Decorator for retrying a function if exception occurs
    Source: https://gist.github.com/treo/728327

    tries -- num tries
    exceptions -- exceptions to catch
    delay -- wait between retries
    """
    exceptions = exceptions or (Exception, )

    def _retry(fn):
        @wraps(fn)
        def __retry(*args, **kwargs):
            for _ in xrange(tries+1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    log.warning("Exception, retrying...", exc_info=e)
                    time.sleep(delay)
            raise  # If no success after tries raise last exception
        return __retry

    return _retry



def bars_to_dateframe(bars, tz):
    """Creates dataframe from list of Bar instances"""

    rows = [{'DateTime':  bar.datetime,
             'Open':      bar.open,
             'High':      bar.high,
             'Low':       bar.low,
             'Close':     bar.close,
             'Volume':    bar.volume,
             } for bar in bars]
    return pd.DataFrame.from_records(rows).set_index(['DateTime']).sort_index()


def tick_bars_to_dateframe(bars):
    rows = [{
        'DateTime': bar.datetime,
        'Last':     bar.last,
        'LastSize': bar.last_size,
        'Volume':   bar.volume,
        'Bid':      bar.bid,
        'Ask':      bar.ask,
        'TicketID': bar.ticket_id,
        } for bar in bars]
    return pd.DataFrame.from_records(rows).set_index(['DateTime']).sort_index()


def get_instruments_from_file(filename):
    """Load index from txt file"""
    instruments = []
    with open(filename, 'r') as f:
        for instrument in f:
            instruments.append(instrument.rstrip())
    if len(instruments) > 0:
        instruments = instruments[1:]
    return instruments