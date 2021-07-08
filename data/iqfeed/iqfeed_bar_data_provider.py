import datetime
from collections import Iterable

from dateutil import tz

from atpy.data.iqfeed.iqfeed_level_1_provider import get_splits_dividends
from atpy.data.iqfeed.util import *
from atpy.data.splits_dividends import adjust_df
from pyevents.events import EventFilter


class IQFeedBarDataListener(iq.SilentBarListener):
    """Real-time bar data"""

    def __init__(self, listeners, interval_len, interval_type='s', mkt_snapshot_depth=0, adjust_history=True, update_interval=0):
        """
        :param listeners: listeners to notify for incombing bars
        :param interval_len: interval length
        :param interval_type: interval type
        :param mkt_snapshot_depth: construct and maintain dataframe representing the current market snapshot with depth. If 0, then don't construct, otherwise construct for the past periods
        :param adjust_history: adjust historical bars for splits and dividends
        :param update_interval: how often to update each bar
        """
        super().__init__(name="Bar data listener %d%s" % (interval_len, interval_type))

        self.listeners = listeners
        self.listeners += self.on_event

        self.conn = None
        self.streaming_conn = None
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.mkt_snapshot_depth = mkt_snapshot_depth
        self.adjust_history = adjust_history
        self.update_interval = update_interval
        self.watched_symbols = dict()
        self.bar_updates = 0

    def __enter__(self):
        launch_service()

        self.conn = iq.BarConn()
        self.conn.add_listener(self)
        self.conn.connect()

        # streaming conn for fundamental data
        if self.adjust_history:
            self.streaming_conn = iq.QuoteConn()
            self.streaming_conn.connect()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.remove_listener(self)

        self.conn.disconnect()

        self.conn = None

        if self.streaming_conn is not None:
            self.streaming_conn.disconnect()
            self.streaming_conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.remove_listener(self)
            if self.own_conn:
                self.conn.disconnect()

        if self.streaming_conn is not None:
            self.streaming_conn.disconnect()

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if bad_symbol in self.watched_symbols and bad_symbol in self.watched_symbols:
            del self.watched_symbols[bad_symbol]

    def _process_bar_update(self, bar_data: np.array) -> pd.DataFrame:
        bar_data = bar_data[0] if len(bar_data) == 1 else bar_data

        symbol = bar_data[0].decode("ascii")

        df = self.watched_symbols[symbol]

        if df is None:
            self.watched_symbols[symbol] = self._bar_to_df(bar_data)
        else:
            bar_timestamp = (bar_data[1] + np.timedelta64(bar_data[2], 'us')) \
                .astype(datetime.datetime) \
                .replace(tzinfo=tz.gettz('US/Eastern')) \
                .astimezone(tz.gettz('UTC'))

            timestamp_ind = df.index.names.index('timestamp')
            df_timestamp = df.index[-1][timestamp_ind]

            if df_timestamp != bar_timestamp:
                data = self._bar_to_df(bar_data)
                df = df.append(data) if self.mkt_snapshot_depth > 0 else data
                if df.shape[0] > self.mkt_snapshot_depth:
                    df = df.iloc[df.shape[0] - self.mkt_snapshot_depth:]

                df.index = df.index.set_levels(pd.to_datetime(df.index.levels[timestamp_ind], utc=True), level='timestamp')

                self.watched_symbols[symbol] = df
            else:
                df.iloc[-1] = bar_data['open_p'], \
                              bar_data['high_p'], \
                              bar_data['low_p'], \
                              bar_data['close_p'], \
                              bar_data['tot_vlm'], \
                              bar_data['prd_vlm'], \
                              bar_data['num_trds']

        self.bar_updates = (self.bar_updates + 1) % 1000000007

        if self.bar_updates % 100 == 0:
            logging.getLogger(__name__).debug("%d bar updates" % self.bar_updates)

        return df

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        df = self._process_bar_update(bar_data)
        symbol = bar_data[0].decode("ascii")

        self.listeners({'type': 'latest_bar_update',
                        'data': df,
                        'symbol': symbol,
                        'interval_type': self.interval_type,
                        'interval_len': self.interval_len})

    def process_live_bar(self, bar_data: np.array) -> None:
        df = self._process_bar_update(bar_data)
        symbol = bar_data[0].decode("ascii")

        self.listeners({'type': 'live_bar',
                        'data': df,
                        'symbol': symbol,
                        'interval_type': self.interval_type,
                        'interval_len': self.interval_len})

    def process_history_bar(self, bar_data: np.array) -> None:
        bar_data = (bar_data[0] if len(bar_data) == 1 else bar_data).copy()

        symbol = bar_data[0].decode("ascii")

        if self.watched_symbols[symbol] is None:
            self.watched_symbols[symbol] = list()

        self.watched_symbols[symbol].append(bar_data)

        if len(self.watched_symbols[symbol]) == self.mkt_snapshot_depth:
            df = self._bars_to_df(self.watched_symbols[symbol])

            if self.adjust_history:
                adjust_df(df, get_splits_dividends(symbol, self.streaming_conn))

            self.watched_symbols[symbol] = df

            self.listeners({'type': 'history_bars',
                            'data': df,
                            'symbol': symbol,
                            'interval_type': self.interval_type,
                            'interval_len': self.interval_len})

    def bar_updates_event_stream(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=
                           lambda e: True if 'type' in e
                                             and e['type'] == 'latest_bar_update'
                                             and e['interval_type'] == self.interval_type
                                             and e['interval_len'] == self.interval_len
                           else False,
                           event_transformer=lambda e: (e['data'], e['symbol']))

    def all_full_bars_event_stream(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=
                           lambda e: True if 'type' in e and e['type'] in ('history_bars', 'live_bar') and e['interval_type'] == self.interval_type and e['interval_len'] == self.interval_len else False,
                           event_transformer=lambda e: (e['data'], e['symbol']))

    def on_event(self, event):
        if event['type'] == 'watch_bars':
            self.watch_bars(event['data']['symbol'] if isinstance(event['data'], dict) else event['data'])

    def watch_bars(self, symbol: typing.Union[str, Iterable]):
        data_copy = {'symbol': symbol,
                     'interval_type': self.interval_type,
                     'interval_len': self.interval_len,
                     'update': self.update_interval,
                     'lookback_bars': self.mkt_snapshot_depth}

        if isinstance(symbol, str) and symbol not in self.watched_symbols:
            self.watched_symbols[symbol] = None
            self.conn.watch(**data_copy)
        elif isinstance(symbol, Iterable):
            for s in [s for s in data_copy['symbol'] if s not in self.watched_symbols]:
                data_copy['symbol'] = s
                self.watched_symbols[s] = None
                self.conn.watch(**data_copy)

    @staticmethod
    def _bars_to_df(bars: list) -> pd.DataFrame:
        if len(bars) == 0:
            return pd.DataFrame()

        df = iqfeed_to_df(bars)

        df['timestamp'] = pd.Index(df['date'] + pd.to_timedelta(df['time'], unit='us')) \
            .tz_localize('US/Eastern') \
            .tz_convert('UTC')

        df = df.rename(index=str,
                       columns={'open_p': 'open',
                                'high_p': 'high',
                                'low_p': 'low',
                                'close_p': 'close',
                                'tot_vlm': 'total_volume',
                                'prd_vlm': 'volume',
                                'num_trds': 'number_of_trades'}) \
            .set_index('timestamp', drop=True, append=False) \
            .drop(['date', 'time'], axis=1)

        return df

    @staticmethod
    def _bar_to_df(bar_data) -> pd.DataFrame:
        result = dict()

        bar_data = (bar_data[0] if len(bar_data) == 1 else bar_data).copy()

        result['timestamp'] = (datetime.datetime.combine(bar_data['date'].astype(datetime.datetime), datetime.datetime.min.time())
                               + datetime.timedelta(microseconds=bar_data['time'].astype(np.uint64) / 1)) \
            .replace(tzinfo=tz.gettz('US/Eastern')).astimezone(tz.gettz('UTC'))

        result['open'] = bar_data['open_p']
        result['high'] = bar_data['high_p']
        result['low'] = bar_data['low_p']
        result['close'] = bar_data['close_p']
        result['total_volume'] = bar_data['tot_vlm']
        result['volume'] = bar_data['prd_vlm']
        result['number_of_trades'] = bar_data['num_trds']

        result = pd.DataFrame(result, index=result['timestamp']).drop('timestamp', axis=1)

        return result
