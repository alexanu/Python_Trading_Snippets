

import requests as request
from bs4 import BeautifulSoup as bs
import re
from pprint import pprint
import sys
from datetime import datetime
import time
import json
import pandas as pd
import numpy as np


class Dl:
    trades = None
    positions = None
    history = None
    broker_stats = None

    exchanges = {
        'O': {'exchange': 'O', 'name': 'NASDAQ', 'active': False, 'premarket_open': '', 'open': '', 'close': '',
              'postmarket_close': '', 'trades': True, 'positions': True, 'history': True},
        'N': {'exchange': 'N', 'name': 'Nyse', 'active': True},
        'A': {'exchange': 'A', 'name': 'Amex', 'active': True},
        'FXSX': {'name': 'currency'},
        'GTIS': {'name': 'commodities'}}

    nicknames = {
        'WTI': {'name': 'West Texas Intermediate Crude Oil', 'instrument': 'C-EWTIUSDBR-SP', 'exchange': 'GTIS'},
        }

    def __init__(self, instrument, exchange="OSE", day="today", download=False, exclude_derivate=True):
        """Init Netfonds downloader
        Date: current date, accepts today, yesterday, monday-friday and datetime obj
        @TODO: verify date, verify not weekend, verify open exchange that day"""

        if day == "today":
            self.date = datetime.datetime.now()
        elif isinstance(day, datetime.datetime) or isinstance(day, datetime.date):
            self.date = day
        else:
            self.date = datetime.datetime.strptime(day, '%Y%m%d')

        self.date = self.date.strftime('%Y%m%d')

        self.exchange_pre = '%s 08:15:00' % self.date
        self.exchange_open = '%s 09:00:00' % self.date
        self.exchange_closed = '%s 16:25:59' % self.date
        self.exchange_post = '%s 17:00:00' % self.date

        self.instrument = instrument
        self.exchange = exchange

        self.exclude_derivative = exclude_derivate

        self.pos_url = 'http://hopey.netfonds.no/posdump.php?date=%s&paper=%s.%s&csv_format=csv'
        self.trade_url = 'http://hopey.netfonds.no/tradedump.php?date=%s&paper=%s.%s&csv_format=csv'
        self.history_url = 'http://www.netfonds.no/quotes/paperhistory.php?paper=%s.%s&csv_format=csv'

        if download:
            self._update()

    def _update(self):
        self._get_trades()
        self._get_positions()

    def new_date(self, date):

        self.date = date
        self._update()

    def get_date(self):

        return self.date

    def _get_trades(self):
        """Get all trades for given date"""

        trade_url = self.trade_url % (self.date, self.instrument, self.exchange)
        self.trades = pd.read_csv(trade_url, parse_dates=[0],
                                  date_parser=lambda t: pd.to_datetime(str(t), format='%Y%m%dT%H%M%S'))

        self.trades.fillna(np.nan)
        self.trades.index = pd.to_datetime(self.trades.time, unit='s')
        self.trades.time = pd.to_datetime(self.trades.time, unit='s')
        self.trades.columns = ['time', 'price', 'volume', 'source', 'buyer', 'seller', 'initiator']
        # del self.trades['time']

        if self.exclude_derivative:
            self.trades = self.trades[(self.trades.source != 'Derivatives trade') & (self.trades.source != 'Official')]

    def _get_positions(self):
        """Get position summary for date
        """
        pos_url = self.pos_url % (self.date, self.instrument, self.exchange)
        self.positions = pd.read_csv(pos_url, parse_dates=[0],
                                     date_parser=lambda t: pd.to_datetime(str(t), format='%Y%m%dT%H%M%S'))
        self.positions.fillna(np.nan)
        self.positions.index = pd.to_datetime(self.positions.time, unit='s')
        self.positions.columns = ['time', 'bid', 'bid_depth', 'bid_depth_total', 'ask', 'ask_depth', 'ask_depth_total']
        self.positions = self.positions[self.exchange_pre:self.exchange_post]

    def _get_broker_stats(self):
        stock_seller = pd.concat([self.trades.groupby(by=['seller'])['volume'].sum(),
                                  self.trades.groupby(by=['seller'])['price'].count()],
                                 axis=1, keys=['sold', 'sold_trades'])

        stock_buyer = pd.concat([self.trades.groupby(by=['buyer'])['volume'].sum(),
                                 self.trades.groupby(by=['buyer'])['price'].count()],
                                axis=1, keys=['bought', 'bought_trades'])

        self.broker_stats = pd.concat([stock_seller, stock_buyer], axis=1).fillna(np.nan)
        self.broker_stats.fillna(0, inplace=True)
        self.broker_stats['total'] = self.broker_stats.bought - self.broker_stats.sold

        self.broker_stats.sort_values(by='total', axis='index', ascending=False, inplace=True)

        self.broker_stats.reset_index(inplace=True)
        self.broker_stats.rename(columns={'index': 'broker'}, inplace=True)

        self.broker_stats.reset_index(inplace=True)

        self.broker_stats['positive'] = self.broker_stats['total'] > 0

    def _get_history(self):
        """"""

        self.history_url = self.history_url % (self.instrument, self.exchange)
        self.history = pd.read_csv(self.history_url, encoding="ISO-8859-1", parse_dates=[0])

        del self.history['paper']
        del self.history['exch']
        self.history.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        self.history.index = pd.to_datetime(self.history.date, unit='s', format="%Y%m%d")
        self.history.fillna(np.nan)
        self.history = self.history.reindex(index=self.history.index[::-1])

        # del self.history['date']

    def get_trades(self, vwap=False, fix_nmbr=False):

        if self.trades is None:
            self._get_trades()

        if vwap and 'vwap' not in self.trades.columns:
            self.vwap()

        if fix_nmbr:
            self.trades[(self.trades.buyer == 'NMBR') & (self.trades.seller != 'NMBR')].buyer = self.trades[
                (self.trades.buyer == 'NMBR') & (self.trades.seller != 'NMBR')].seller
            self.trades[(self.trades.seller == 'NMBR') & (self.trades.buyer != 'NMBR')].seller = self.trades[
                (self.trades.seller == 'NMBR') & (self.trades.buyer != 'NMBR')].buyer

        return self.trades

    def vwap(self):
        self.trades['vwap'] = (self.trades.volume * self.trades.price).cumsum() / self.trades.volume.cumsum()

    def get_broker_stats(self):
        """
        Columns: ['index', 'broker', 'sold', 'sold_trades', 'bought', 'bought_trades', 'total', 'positive']
        :return:
        """

        if self.broker_stats is None:
            self._get_broker_stats()

        return self.broker_stats

    def get_positions(self):
        """
        Columns: ['time', 'bid', 'bid_depth', 'bid_depth_total', 'ask', 'ask_depth', 'ask_depth_total']
        :return:
        """

        if self.positions is None:
            self._get_positions()

        return self.positions

    def get_history(self, mas=[], value='close'):
        """Returns ohlc and volume per trading day.
        Optional moving averages defined in the mas list will be applied and added as 'ma<window>' columns
        Columns: ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover' (,'ma<window>', ...)]"""

        if self.history is None:
            self._get_history()

        if len(mas) > 0:
            for ma in mas:
                self.history['ma%i' % ma] = self.history[value].rolling(center=False, window=ma).mean()

        return self.history

    def get_ohlcv(self, interval='5min', vwap=False):
        """Returns a resampled dataframe for date with or without vwap
        Columns: ['time', 'open', 'high', 'low', 'close', 'volume' (,'vwap')]
        New resampler:
        ticks.Price.resample('1min').ohlc())
        """
        if self.trades is None:
            self._get_trades()

        if self.trades.shape[0] == 0:
            return None
            # tmp = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume']).set_index('time', inplace=True)
            # print(tmp)
            # return tmp

        vol = self.trades.volume.resample(interval).sum()  # resample(interval, how={'volume': 'sum'})
        vol.columns = pd.MultiIndex.from_tuples([('vol', 'volume')])

        price = self.trades.price.resample(interval).ohlc()  # resample(interval, how={'price': 'ohlc'})

        if vwap:
            if 'vwap' not in self.trades.columns:
                self.vwap()

            vwap_rs = self.trades.wvap.resample(interval).mean()  # .resample(interval, how={'vwap': 'mean'})
            vwap_rs.columns = pd.MultiIndex.from_tuples([('vwap_rs', 'vwap')])
            ohlcv = pd.concat([price, vol, vwap_rs], axis=1)

        else:
            ohlcv = pd.concat([price, vol], axis=1)

        # ohlcv.columns = ohlcv.columns.droplevel()

        # Fix nan's forward fill last close
        ohlcv = ohlcv.assign(close=ohlcv['close'].ffill()).bfill(axis=1)
        # Fix nan's volume to 0
        ohlcv.volume.fillna(0, inplace=True)

        ohlcv['time'] = pd.to_datetime(ohlcv.index.to_series())
        ohlcv = ohlcv.reindex_axis(['time', 'open', 'high', 'low', 'close', 'volume'], axis=1)

        return ohlcv
    
    
    


class Info(object):
    def __init__(self, instrument, exchange='OSE'):

        html = request.get('http://www.netfonds.no/quotes/about.php?paper=%s.%s' % (instrument, exchange))
        s = bs(html.text, "lxml")

        table = s.find(id='updatetable1').find_all('tr')

        structure = {1: 'instrument',
                     2: 'name',
                     3: 'exchange',
                     4: 'type',
                     5: 'active',
                     6: 'tradable',
                     7: 'index1',
                     8: 'index2',
                     9: 'index3',
                     10: 'prev_tradeday',
                     11: 'prev_quote',
                     12: 'quantity',
                     13: 'mcap',
                     14: 'lot_size',
                     15: 'min_amount',
                     16: 'ISIN',
                     17: 'currency',
                     18: 'about'}

        self.info = {}
        index3 = False

        for key, row in enumerate(table):

            if key == 0:
                continue

            if key == 9:
                for cell in row.find_all('th'):
                    if cell.get_text().strip() == 'Indeks':
                        index3 = True
            skey = key
            for cell in row.find_all('td'):

                if skey >= 9 and not index3:
                    skey = skey + 1

                if skey in [5, 6]:
                    value = cell.get_text().strip()
                    value = True if value == 'Ja' else False
                elif skey == 10:
                    value = datetime.strptime(cell.get_text().strip(), '%d/%m-%Y').date()
                elif skey == 11:
                    value = float(cell.get_text().strip())
                elif skey in [12, 13, 14]:
                    value = int(cell.get_text().replace(' ', ''))
                elif skey == 16:
                    value = cell.get_text().replace(' ', '')
                else:
                    value = cell.get_text().strip()

                self.info.update({structure[skey]: value})

        # Assign dictionary to properties
        for k, v in self.info.items():
            setattr(self, k, v)

    def get_info(self):
        return self.info

    def is_active(self):
        return self.info.get('active', False)
    
    def is_tradable(self):
        return self.info.get('tradable', False)

    def get_indexes(self):

        return {1: self.info.get('index1', None),
                2: self.info.get('index2', None),
                3: self.info.get('index3', None)}

    
    

class Multi():
    def __init__(self, instrument, exchange='OSE'):

        self.instrument = instrument
        self.exchange = exchange

    def _daterange(self, start_date, end_date):
        """Date generator"""
        if start_date <= end_date:
            for n in range((end_date - start_date).days + 1):
                yield start_date + datetime.timedelta(n)
        else:
            for n in range((start_date - end_date).days + 1):
                yield start_date - datetime.timedelta(n)

    def _dates(self, start, end):

        if isinstance(start, datetime.datetime) and isinstance(end, datetime.datetime):
            pass
        else:
            start = datetime.datetime.strptime(start, '%Y%m%d')
            end = datetime.datetime.strptime(end, '%Y%m%d')

        return start.date(), end.date()

    def get_trades(self, start, end):
        """Uses dl to download and appends each day with data"""

        start, end = self._dates(start, end)

        for date in self._daterange(start, end):

            if date.isoweekday() not in [6, 7]:
                # print(date.__format__("%Y%m%d"))
                i = Dl(self.instrument, self.exchange, day=date)

                data = i.get_trades()

                if isinstance(data, pd.DataFrame):
                    if 'trades' in locals() and isinstance(trades, pd.DataFrame):
                        trades = trades.append(data)
                    else:
                        trades = data

        return trades

    def get_ohlcv(self, start, end, interval='5min'):
        """Uses dl to download and appends each day with data"""

        start, end = self._dates(start, end)

        for date in self._daterange(start, end):

            if date.isoweekday() not in [6, 7]:
                # print(date.__format__("%Y%m%d"))
                i = Dl(self.instrument, self.exchange, day=date, download=True)

                data = i.get_ohlcv(interval)

                if isinstance(data, pd.DataFrame):
                    if 'ohlcv' in locals() and isinstance(ohlcv, pd.DataFrame):
                        ohlcv = ohlcv.append(data)
                    else:
                        ohlcv = data

        return ohlcv

    def get_positions(self, start, end):
        """Uses dl to download and appends each day with data"""

        start, end = self._dates(start, end)

        for date in self._daterange(start, end):

            if date.isoweekday() not in [6, 7]:
                # print(date.__format__("%Y%m%d"))
                i = Dl(self.instrument, self.exchange, day=date)

                data = i.get_positions()

                if isinstance(data, pd.DataFrame):
                    if 'positions' in locals() and isinstance(positions, pd.DataFrame):
                        positions = positions.append(data)
                    else:
                        positions = data

        return positions
    
    
# EXAMPLES ##############################################################################################

stl = multi(instrument='STL', exchange='OSE') # Statoil ASA
ohlcv = stl.get_ohlcv('20170801', '20171020', '15min') # intraday data
# ohlcv = stl.get_ohlcv(interval='5min')
ohlcv.to_csv('stl.csv')


stl = Dl('STL', exchange='OSE', download=False)
history = stl.get_history()
history_ma = stl.get_history(mas=[10,20,50,100,200])

stl_2 = Dl('STL', exchange='OSE', day='today', download=True)
stl_2.trades.tail(5)
# If download=False then you need to call dl.get_trades()
#trades = stl_2.get_trades()
stl_2.vwap()

positions = stl.get_positions() 
fig, ax = plt.subplots()
ax.tick_params(labeltop=False, labelright=True)
positions.bid.plot(drawstyle='steps', color='b')
positions.ask.plot(drawstyle='steps', color='g')
plt.title("Intraday best bid vs best ask")
plt.grid()


brokers = stl.get_broker_stats()



 
