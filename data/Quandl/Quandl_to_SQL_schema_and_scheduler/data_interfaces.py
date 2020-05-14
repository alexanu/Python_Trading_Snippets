import pandas as pd
import datetime as dt
import pandas_datareader.data as pdata
import numpy as np
import sqlalchemy as sa
import urllib
import dateutil
from sqlalchemy.sql.expression import bindparam
from bs4 import BeautifulSoup
import requests

class DatabaseInterface(object):

    # SqlAlchemy metadata
    connection_string = None
    engine = None
    conn = None
    metadata = None
    tables = None

    @classmethod
    def read_sql(cls, query, parse_dates=None):
        output = pd.read_sql(sql=query,
                             con=cls.engine,
                             parse_dates=parse_dates)
        return output


    @classmethod
    def initialize(cls):
        cls.connection_string = "postgresql://postgres:Thirdgen1@localhost:5432/postgres"
        cls.engine = sa.create_engine(cls.connection_string, echo=False)
        cls.conn = cls.engine.connect() # Connect to the database
        cls.metadata = sa.MetaData() # Metadata object
        cls.define_qfl_tables() # Create tables

    @classmethod
    def define_qfl_tables(cls):
        cls.tables = dict() # Tables

        # TIME SERIES
        time_series_table = sa.Table(
            'time_series', cls.metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('date', sa.Date, primary_key=True),
            sa.Column('field', sa.String(128), primary_key=True),
            sa.Column('value', sa.Float, primary_key=False))

        cls.tables['time_series'] = time_series_table

        # EQUITIES
        equities_table = sa.Table(
            'equities', cls.metadata,
            sa.Column('id', sa.Integer, primary_key=True, unique=True),
            sa.Column('ticker', sa.String(64), primary_key=False, unique=True))

        cls.tables['equities'] = equities_table

        # EQUITY PRICES
        equity_prices_table = sa.Table(
            'equity_prices', cls.metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('date', sa.Date, primary_key=True),
            sa.Column('adj_close', sa.Float, primary_key=False),
            sa.Column('last_price', sa.Float, primary_key=False),
            sa.Column('bid_price', sa.Float, primary_key=False),
            sa.Column('ask_price', sa.Float, primary_key=False),
            sa.Column('open_price', sa.Float, primary_key=False),
            sa.Column('high_price', sa.Float, primary_key=False),
            sa.Column('low_price', sa.Float, primary_key=False),
            sa.Column('volume', sa.Float, primary_key=False),
            sa.ForeignKeyConstraint(['id'], ['equities.id']))
        cls.tables['equity_prices'] = equity_prices_table

        # EQUITY SCHEDULES
        equity_schedules_table = sa.Table(
            'equity_schedules',
            cls.metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('date', sa.Date, primary_key=True),
            sa.Column('schedule_type', sa.String(16), primary_key=True),
            sa.Column('value', sa.Float, primary_key=False),
            sa.ForeignKeyConstraint(['id'], ['equities.id']))
        cls.tables['equity_schedules'] = equity_schedules_table

        # EQUITY INDICES
        equity_index_table = sa.Table(
            'equity_indices',
            cls.metadata,
            sa.Column('index_id', sa.Integer, primary_key=True, unique=True),
            sa.Column('ticker', sa.String(32), primary_key=False))
        cls.tables['equity_indices'] = equity_index_table

        # EQUITY INDEX MEMBERS
        equity_index_members_table = sa.Table(
            'equity_index_members',
            cls.metadata,
            sa.Column('index_id', sa.Integer, primary_key=True),
            sa.Column('equity_id', sa.Integer, primary_key=True),
            sa.Column('valid_date', sa.Date, primary_key=True),
            sa.ForeignKeyConstraint(['index_id'], ['equity_indices.index_id']))
        cls.tables['equity_index_members'] = equity_index_members_table

    @classmethod
    def create_tables(cls):
        cls.metadata.create_all(cls.engine)

    @classmethod
    def get_table(cls, table_name=None):
        if cls.tables is not None:
            if table_name in cls.tables:
                return cls.tables.get(table_name)
            else:
                return None

    @classmethod
    def execute_bulk_insert(cls, df, table):
        list_to_write = df.to_dict(orient='records')
        result = cls.engine.execute(sa.insert(table=table,
                                              values=list_to_write))
        return result

    @classmethod
    def format_as_tuple_for_query(cls, item):
        if item is None:
            return
        if not isinstance(item, (tuple, list)):
            item = [item]
        item = tuple(item)
        return item

    @classmethod
    def parenthetical_string_list_with_quotes(cls, items):
        out = "("
        if not isinstance(items, (tuple, list)):
            items = [items]
        counter = 0
        for item in items:
            out += "'" + item + "'"
            counter += 1
            if counter == len(items):
                out += ')'
            else:
                out += ','
        return out

    @classmethod
    def build_pk_where_str(cls, df, table, use_column_as_key):
        where_str = str()
        if use_column_as_key is None:
            key_column_names = table.primary_key.columns.keys()
        else:
            key_column_names = [use_column_as_key]
        for key in key_column_names:
            if key in df:
                unique_keys = np.unique(df[key])
                if isinstance(table.columns[key].type, sa.String):
                    unique_keys = [str(k) for k in unique_keys]
                t = tuple(unique_keys)
                if isinstance(table.columns[key].type, sa.Date):
                    unique_keys = [str(k) for k in pd.to_datetime(unique_keys)]
                    t = cls.parenthetical_string_list_with_quotes(unique_keys)
                if where_str != '':
                    where_str += ' and '
                if len(unique_keys) == 1:
                    try:
                        a = iter(unique_keys)
                        unique_keys = unique_keys[0]
                    except:
                        a = 1
                where_str += key + " in {0}".format(t)

        return where_str, key_column_names

    @classmethod
    def execute_db_save(cls, df, table, use_column_as_key=None):

        """
        This is a generic fast method to insert-if-exists-update a database
        table using SqlAlchemy expression language framework.
        :param df: DataFrame containing data to be written to table
        :param table: SqlAlchemy table object
        :param use_column_as_key string
        :return: none
        """

        # Get where string to identify existing rows
        where_str, key_column_names = cls.build_pk_where_str(df,
                                                             table,
                                                             use_column_as_key)

        # Grab column names
        column_names = table.columns.keys()
        column_list = [table.columns[column_name]
                       for column_name in column_names]

        # Grab the existing data in table corresponding to the new data
        s = sa.select(columns=column_list).where(where_str)
        existing_data = pd.read_sql(sql=s,
                                    con=cls.engine,
                                    index_col=key_column_names)

        # Add index to df so that we can identify existing data
        key_column_names_in_df = list()
        for key_column in key_column_names:
            if key_column in df.columns:
                key_column_names_in_df.append(key_column)
        df_key_cols = [df[key_column] for key_column in key_column_names_in_df]

        if use_column_as_key is None:
            df.index = df_key_cols
        else:
            orig_key_cols = table.primary_key.columns.keys()
            df.index = df[use_column_as_key]
            existing_data = existing_data.reset_index()
            existing_data.index = existing_data[use_column_as_key]

        # New df structures for insert and update
        insert_df = pd.DataFrame(columns=df.columns)
        update_df = pd.DataFrame(columns=df.columns)

        # Now we figure out what part of the df is already in existing_data
        for ind in df.index.values:
            if ind in existing_data.index:
                update_df = update_df.append(df.loc[ind])
            else:
                insert_df = insert_df.append(df.loc[ind])

        # Insert part is easy
        if len(insert_df) > 0:
            insert_df.reset_index()
            cls.execute_bulk_insert(insert_df, table)

        # Generic version of update using bindparams
        if len(update_df) > 0:

            update_df_mod = update_df

            # If we used an alternative column for the key (e.g. if the table's
            # primary key is an autoincrement integer) we need to join to get it
            if use_column_as_key is not None:
                join_cols = [use_column_as_key] + orig_key_cols
                try:
                    existing_data = existing_data.reset_index()
                except:
                    existing_data.index = existing_data[orig_key_cols]
                update_df_mod = pd.merge(left=existing_data[join_cols],
                                         right=update_df,
                                         on=key_column_names)

            # Cannot use database column names as bindparams (odd)
            for col in key_column_names:
                update_df_mod = update_df_mod.rename(columns={col: col + "_"})

            # Dictionary format for records
            list_to_write = update_df_mod.to_dict(orient='records')

            # Build the update command
            s = table.update()
            for col in key_column_names:
                s = s.where(table.c[col] == bindparam(col + "_"))
            values_dict = dict()
            for col in key_column_names:
                values_dict[col] = bindparam(col)
            s.values(values_dict)

            # Execute the update command
            cls.conn.execute(s, list_to_write)

    @classmethod
    def get_data(cls, table_name=None, index_table=False, parse_dates=False):
        if table_name in cls.tables:
            table = cls.tables[table_name]
            pk_columns = None
            if index_table:
                pk_columns = table.primary_key.columns.keys()
            columns = table.columns.keys()
            parse_dates_columns = None
            if parse_dates:
                for column in columns:
                    if table.columns[column].type == sa.sql.sqltypes.Date:
                        parse_dates_columns.append(table.columns[column].name)
            output = pd.read_sql(sql=table_name,
                                 con=cls.engine,
                                 index_col=pk_columns,
                                 parse_dates=parse_dates_columns)
            return output

    @classmethod
    def get_equity_ids(cls, equity_tickers=None):
        ids = list()
        equities = cls.get_data(table_name='equities')
        if equity_tickers is None:
            ids = equities['id'].tolist()
        else:
            equities.index = equities['ticker']
            for ticker in equity_tickers:
                if ticker in equities.index:
                    ids.append(equities.loc[ticker, 'id'])
        return ids

    @classmethod
    def get_equity_tickers(cls, ids=None):
        tickers = list()
        equities = cls.get_data(table_name='equities')
        if ids is None:
            tickers = equities['ticker'].tolist()
        else:
            equities.index = equities['id']
            for id in ids:
                if id in equities.index:
                    tickers.append(equities.loc[id, 'ticker'])
        return tickers


    @classmethod
    def get_equities(cls):
        x = 1



class FigiApi(object):

    base_url = 'https://api.openfigi.com/v1/mapping'
    api_key = "471197f1-50fe-429b-9e11-a6828980e213"

    @classmethod
    def retrieve_mapping(cls, id_type=None, ids=None, exch_codes=None):
        req_data = list()
        for i in range(0, len(ids)):
            req_data.append({"idType": id_type,
                             "idValue": ids[i],
                             "exchCode": exch_codes[i]})
        r = requests.post(cls.base_url,
                          headers={"Content-Type": "text/json",
                                   "X-OPENFIGI-APIKEY": cls.api_key},
                          json=req_data)
        return r


class QuandlApi(ExternalDataApi):

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

