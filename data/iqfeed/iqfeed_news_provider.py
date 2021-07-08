import datetime
import threading
from datetime import timedelta
from typing import List

import pandas as pd
from dateutil import tz

import atpy.data.iqfeed.util as iqfeedutil
import pyiqfeed as iq
from atpy.data.iqfeed.filters import *


class NewsFilter(NamedTuple):
    """
    News filter parameters
    """
    sources: List[str]
    symbols: List[str]
    date: datetime.date
    timeout: int
    limit: int


NewsFilter.__new__.__defaults__ = (None, None, None, None, 100000)


class DefaultNewsFilterProvider(DefaultFilterProvider):
    """Default news filter provider, which contains a list of filters"""

    def _default_filter(self):
        return NewsFilter()


class IQFeedNewsProvider(object):
    """
    IQFeed news provider (not streaming). See the unit test on how to use
    """

    def __init__(self, attach_text=False, key_suffix=''):
        """
        :param attach_text: attach news text (separate request for each news item)
        :param key_suffix: suffix in the output dictionary
        """
        self.attach_text = attach_text
        self.conn = None
        self.key_suffix = key_suffix

    def __enter__(self):
        iqfeedutil.launch_service()

        self.conn = iq.NewsConn()
        self.conn.connect()
        self.cfg = self.conn.request_news_config()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.disconnect()
        self.quote_conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.cfg = None

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def request_news(self, f: NewsFilter):
        _headlines = self.conn.request_news_headlines(sources=f.sources, symbols=f.symbols, date=f.date, limit=f.limit, timeout=f.timeout)
        headlines = [h._asdict() for h in _headlines]

        processed_data = None

        for h in headlines:
            if self.attach_text:
                h['text'] = self.conn.request_news_story(h['story_id']).story

            h['timestamp' + self.key_suffix] = (datetime.datetime.combine(h.pop('story_date').astype(datetime.date), datetime.datetime.min.time()) + timedelta(microseconds=h.pop('story_time'))).replace(
                tzinfo=tz.gettz('US/Eastern')).astimezone(tz.gettz('UTC'))

            if processed_data is None:
                processed_data = {f + self.key_suffix: list() for f in h.keys()}

            for key, value in h.items():
                processed_data[key + self.key_suffix].append(value)

        result = pd.DataFrame(processed_data)

        result = result.set_index(['timestamp', 'story_id'], drop=False, append=False).iloc[::-1]

        return result


class IQFeedNewsListener(IQFeedNewsProvider):
    """
    IQFeed news listener (not streaming). See the unit test on how to use
    """

    def __init__(self, listeners, attach_text=False, key_suffix='', filter_provider=DefaultNewsFilterProvider()):
        """
        :param listeners: event listeners
        :param attach_text: attach news text (separate request for each news item)
        :param key_suffix: suffix in the output dictionary
        :param filter_provider: iterator for filters
        """
        super().__init__(attach_text=attach_text, key_suffix=key_suffix)
        self.listeners = listeners
        self.conn = None
        self.filter_provider = filter_provider

    def __enter__(self):
        super().__enter__()
        iqfeedutil.launch_service()

        self.conn = iq.NewsConn()
        self.conn.connect()
        self.cfg = self.conn.request_news_config()

        self.is_running = True
        self.producer_thread = threading.Thread(target=self.produce, daemon=True)
        self.producer_thread.start()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.disconnect()
        self.quote_conn = None
        self.is_running = False

    def __del__(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.cfg = None

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def produce(self):
        for f in self.filter_provider:
            result = super().request_news(f)

            self.listeners({'type': 'news_batch', 'data': result})

            if not self.is_running:
                break

    def batch_provider(self):
        return iqfeedutil.IQFeedDataProvider(self.listeners, accept_event=lambda e: True if e['type'] == 'news_batch' else False)
