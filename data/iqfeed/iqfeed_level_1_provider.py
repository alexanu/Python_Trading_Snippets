import logging
import queue
import threading
import typing
from collections import Iterable

import numpy as np
import pandas as pd

import pyiqfeed as iq
from atpy.data.iqfeed.util import launch_service, iqfeed_to_dict, iqfeed_to_deque
from pyevents.events import SyncListeners, EventFilter


class IQFeedLevel1Listener(iq.SilentQuoteListener):

    def __init__(self, listeners, mkt_snapshot_depth=0, conn: iq.QuoteConn = None):
        super().__init__(name="Level 1 listener")

        self.listeners = listeners
        self.listeners += self.on_event

        self.mkt_snapshot_depth = mkt_snapshot_depth

        self.conn = conn
        self._own_conn = conn is None

        self.watched_symbols = dict()

        self.total_updates = 0

    def __enter__(self):
        if self._own_conn:
            launch_service()
            self.conn = iq.QuoteConn()
            self.conn.add_listener(self)
            self.conn.connect()
        else:
            self.conn.add_listener(self)

        self.queue = queue.Queue()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.remove_listener(self)

        if self._own_conn:
            self.conn.disconnect()

        self.conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.remove_listener(self)
            self.conn.disconnect()

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def on_event(self, event):
        if event['type'] == 'watch_ticks':
            self.watch(event['data'])

    def watch(self, symbol: typing.Union[str, Iterable]):
        """Watch symbol for both trades and quotes"""
        if isinstance(symbol, str) and symbol not in self.watched_symbols:
            self.watched_symbols[symbol] = None
            self.conn.watch(symbol)
        elif isinstance(symbol, Iterable):
            for s in [s for s in symbol if s not in self.watched_symbols]:
                self.watched_symbols[s] = None
                self.conn.watch(s)

    def watch_trades(self, symbol: typing.Union[str, Iterable]):
        """Watch symbol for both trades only"""
        if isinstance(symbol, str) and symbol not in self.watched_symbols:
            self.watched_symbols[symbol] = None
            self.conn.trades_watch(symbol)
        elif isinstance(symbol, Iterable):
            for s in [s for s in symbol if s not in self.watched_symbols]:
                self.watched_symbols[s] = None
                self.conn.trades_watch(s)

    def unwatch(self, symbol: typing.Union[str, Iterable]):
        if isinstance(symbol, str) and symbol in self.watched_symbols:
            del self.watched_symbols[symbol]
            self.conn.unwatch(symbol)
        elif isinstance(symbol, list):
            for s in [s for s in symbol if s in self.watched_symbols]:
                del self.watched_symbols[s]
                self.conn.unwatch(s)

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol
        :param bad_symbol: The bad symbol
        """

        logging.getLogger(__name__).warning("Invalid symbol request: " + str(bad_symbol))

        if bad_symbol in self.watched_symbols:
            del self.watched_symbols[bad_symbol]

    def process_news(self, news_item: iq.QuoteConn.NewsMsg):
        self.listeners({'type': 'level_1_news_item', 'data': news_item._asdict()})

    def process_regional_quote(self, quote: np.array):
        self.listeners({'type': 'level_1_regional_quote', 'data': iqfeed_to_dict(quote)})

    def process_summary(self, summary: np.array):
        if self.mkt_snapshot_depth > 0:
            s = iqfeed_to_deque([summary], maxlen=self.mkt_snapshot_depth)

            # no need for deque for the symbol
            symbol = s['symbol'][0]
            s['symbol'] = symbol

            self.watched_symbols[symbol] = s

        self.listeners({'type': 'level_1_summary', 'data': iqfeed_to_dict(summary)})

    def process_update(self, update: np.array):
        if self.mkt_snapshot_depth > 0:
            update = update[0] if len(update) == 1 else update
            symbol = update[0].decode("ascii")

            data = self.watched_symbols[symbol]

            for key, v in zip(data, update):  # skip symbol
                if key != 'symbol':
                    if isinstance(v, bytes):
                        v = v.decode('ascii')

                    data[key].append(v)
        else:
            data = iqfeed_to_dict(update)

        self.total_updates = (self.total_updates + 1) % 1000000007

        if self.total_updates % 1000 == 0:
            logging.getLogger(__name__).debug("%d total updates" % self.total_updates)

        self.listeners({'type': 'level_1_update', 'data': data})

    def news_filter(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if e['type'] == 'level_1_news_item' else False,
                           event_transformer=lambda e: (e['data'],))

    def regional_quote_filter(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if e['type'] == 'level_1_regional_quote' else False,
                           event_transformer=lambda e: (e['data'],))

    def all_level_1_filter(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if 'type' in e and e['type'] in ('level_1_summary', 'level_1_update') else False,
                           event_transformer=lambda e: (e['data'],))

    def level_1_summary_filter(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if 'type' in e and e['type'] == 'level_1_summary' else False,
                           event_transformer=lambda e: (e['data'],))

    def level_1_update_filter(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if 'type' in e and e['type'] == 'level_1_update' else False,
                           event_transformer=lambda e: (e['data'],))

    def process_fundamentals(self, fund: np.array):
        f = iqfeed_to_dict(fund)
        self.listeners({'type': 'level_1_fundamentals', 'data': f})

    def fundamentals_filter(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if e['type'] == 'level_1_fundamentals' else False,
                           event_transformer=lambda e: (e['data'],))


def get_fundamentals(symbol: typing.Union[str, Iterable], conn: iq.QuoteConn = None):
    result = dict()
    invalid = set()

    if isinstance(symbol, str):
        symbol = {symbol}
    elif not isinstance(symbol, set) and isinstance(symbol, Iterable):
        symbol = set(symbol)

    e = threading.Event()

    class TmpIQFeedLevel1Listener(IQFeedLevel1Listener):
        def process_invalid_symbol(self, bad_symbol: str) -> None:
            super().process_invalid_symbol(bad_symbol)
            invalid.add(bad_symbol)
            if (result.keys() | invalid) - symbol == {}:
                e.set()

        def process_fundamentals(self, fund: np.array):
            fundamentals = iqfeed_to_dict(fund)
            s = fundamentals['symbol']
            result[s] = fundamentals
            if len(symbol - (result.keys() | invalid)) == 0:
                e.set()

    with TmpIQFeedLevel1Listener(listeners=SyncListeners(), conn=conn) as listener:
        listener.watch(symbol)
        e.wait()

    return result


def get_splits_dividends(symbol: typing.Union[set, str], conn: iq.QuoteConn = None):
    funds = get_fundamentals(symbol=symbol, conn=conn)

    points = {'timestamp': list(), 'symbol': list(), 'type': list(), 'value': list()}
    for f in funds.values():
        if f['split_factor_1_date'] is not None and f['split_factor_1'] is not None:
            points['timestamp'].append(f['split_factor_1_date'])
            points['symbol'].append(f['symbol'])
            points['type'].append('split')
            points['value'].append(f['split_factor_1'])

        if f['split_factor_2_date'] is not None and f['split_factor_2'] is not None:
            points['timestamp'].append(f['split_factor_2_date'])
            points['symbol'].append(f['symbol'])
            points['type'].append('split')
            points['value'].append(f['split_factor_2'])

        if f['ex-dividend_date'] is not None and f['dividend_amount'] is not None:
            points['timestamp'].append(f['ex-dividend_date'])
            points['symbol'].append(f['symbol'])
            points['type'].append('dividend')
            points['value'].append(f['dividend_amount'])

    result = pd.DataFrame(points)

    if not result.empty:
        result['provider'] = 'iqfeed'

        result['timestamp'] = pd.to_datetime(result['timestamp'])
        result = result.set_index(['timestamp', 'symbol', 'type', 'provider'])
        result = result.tz_localize('US/Eastern', level=0).tz_convert('UTC', level=0)
        result = result[~result.index.duplicated(keep='last')]
        result.sort_index(inplace=True)

    return result
