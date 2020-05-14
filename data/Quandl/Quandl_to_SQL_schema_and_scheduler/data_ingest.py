"""
This module should ideally be ONLY tasked with the automation and triggering of
ETL tasks and should rely on other modules to actually implement those tasks?
EG, we shouldn't need references here to YahooApi...
"""

import pandas as pd
import datetime as dt
import numpy as np

from qfl.core.data_interfaces import QuandlApi, FigiApi
from qfl.core.data_interfaces import DatabaseInterface as db

default_start_date = dt.datetime(1990, 1, 1) # Default start date for historical data ingests

from pandas.tseries.offsets import BDay
def closest_business_day_in_past(date=None):
    if date is None:
        date = dt.datetime.today()
return date + BDay(1) - BDay(1)


def initialize_data_environment():
    db.initialize()

def daily_equity_price_ingest():
    initialize_data_environment()
    data_source = 'yahoo'
    update_equity_prices(ids=None,
                         data_source=data_source,
                         _db=db)

def historical_equity_price_ingest():
    initialize_data_environment()
    date = utils.closest_business_day_in_past()
    data_source = 'yahoo'
    load_historical_equity_prices(ids=None,
                                  start_date=default_start_date,
                                  end_date=date,
                                  data_source=data_source,
                                  _db=db)

def _prep_equity_price_load(ids=None, _db=None):
    # Grab full universe
    equities = _db.get_data(table_name='equities', index_table=True)
    equity_prices_table = _db.get_table(table_name='equity_prices')

    if ids is None:    # Default is everything
        ids = equities.index.tolist()

    rows = equities.loc[ids].reset_index()
    equity_tickers = rows['ticker'].tolist()
    equity_tickers = [str(ticker) for ticker in equity_tickers] # handle potential unicode weirdness

    return equities, equity_prices_table, ids, equity_tickers, rows

def update_equity_prices(ids=None,
                         data_source='yahoo',
                         _db=None):
    date = utils.closest_business_day_in_past(dt.datetime.today())
    load_historical_equity_prices(ids=ids,
                                  start_date=date,
                                  end_date=date,
                                  data_source=data_source,
                                  _db=_db,
                                  batch_ones=False)

def _load_historical_equity_prices(ids=None,
                                   start_date=default_start_date,
                                   end_date=dt.datetime.today(),
                                   data_source='yahoo',
                                   _db=None):
    # Prep
    equities, equity_prices_table, ids, equity_tickers, rows = \
        _prep_equity_price_load(ids, _db)

    if data_source == 'yahoo':
        prices = YahooApi.retrieve_prices(equity_tickers, start_date, end_date)
        prices_df = prices.to_frame()
        yahoo_fields = ['id', 'date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        db_fields = ['id', 'date', 'open_price', 'high_price', 'low_price','last_price', 'volume', 'adj_close']
        prices_df.index.names = ['date', 'ticker'] # Remove indices to prepare for database
        prices_df = prices_df.reset_index()
        mapped_prices = pd.merge(left=prices_df, # Merge with ID's
                                 right=rows,
                                 on='ticker',
                                 how='inner')
    else:
        raise NotImplementedError

    # Map to database column structure
    equity_prices_data = pd.DataFrame(index=mapped_prices.index,
                                      columns=equity_prices_table.columns.keys())
    for i in range(0, len(yahoo_fields)):
        equity_prices_data[db_fields[i]] = mapped_prices[yahoo_fields[i]]

    _db.execute_db_save(equity_prices_data, equity_prices_table)


def load_historical_equity_prices(ids=None,
                                  start_date=default_start_date,
                                  end_date=dt.datetime.today(),
                                  data_source='yahoo',
                                  _db=None,
                                  batch_ones=True):

    if batch_ones:
        for _id in ids:
            _load_historical_equity_prices([_id],
                                           start_date,
                                           end_date,
                                           data_source,
                                           _db)
    else:
        _load_historical_equity_prices(ids,
                                       start_date,
                                       end_date,
                                       data_source,
                                       _db)

def add_equities_from_index(ticker=None, method='quandl', _db=None):

    # right now this uses quandl
    tickers = list()
    if method == 'quandl':
        if ticker not in QuandlApi.get_equity_index_universe():
            raise NotImplementedError
        tickers = QuandlApi.get_equity_universe(ticker)
    else:
        raise NotImplementedError
    
    add_equities_from_list(tickers=tickers) # Add the equities
    tickers = [str(ticker) for ticker in tickers] # Make sure string format is normal
    equities_table = db.get_table(table_name='equities') # Get the equities we just created
    equities_table_data = _db.get_data(table_name='equities')

    # Find the index mapping
    indices = _db.get_table(table_name='equity_indices')
    index_id = indices[indices['ticker'] == ticker]['index_id'].values[0]

    # Get index members table
    index_members_table = _db.get_table(table_name='equity_index_members')
    index_membership_data = pd.DataFrame(
        columns=index_members_table.columns.keys())
    index_membership_data['equity_id'] = equities_table_data['id']
    index_membership_data['valid_date'] = dt.date.today()
    index_membership_data['index_id'] = index_id

    db.execute_db_save(df=index_membership_data, table=index_members_table) # Update equity index membership table


def add_equities_from_list(tickers=None, _db=None):
    tickers_df = pd.DataFrame(data=tickers, columns=['ticker'])
    equities_table = _db.get_table(table_name='equities')
    db.execute_db_save(df=tickers_df, table=equities_table)

