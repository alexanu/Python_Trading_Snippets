# Source: https://github.com/thoriuchi0531/intraday_data
# Schema is here:
# https://bubbl.us/NDc3NDc4NC8zODMxOTMwLzAxOWMyZmJiYThkYmNmYzM0OTMyYzczNTI4MTA3ZGVk@X?utm_source=shared-link&utm_medium=link&s=10033179


import requests
import zipfile
import io
from os.path import expanduser, join
from os import remove
from datetime import datetime, timedelta

import pandas as pd
from arctic.date import DateRange

from adagio.utils.date import date_shift


ccy_pairs = ['AUDJPY', 'AUDNZD', 'AUDUSD', 'CADJPY', 'CHFJPY', 'EURCHF',
             'EURGBP', 'EURJPY', 'EURUSD', 'GBPJPY', 'GBPUSD', 'NZDUSD',
             'USDCAD', 'USDCHF', 'USDJPY']

start_date = datetime(2009, 5, 1)
url_change_date = datetime(2017, 4, 1)
root_url = r'https://www.truefx.com/dev/data'
download_to = expanduser('~')
data_frequency = '1min'


from arctic import Arctic
arctic_store = Arctic(arctic_host = 'localhost')

def get_library(library_name):
    libraries = arctic_store.list_libraries()
    if library_name not in libraries:
        arctic_store.initialize_library(library_name)
    return arctic_store[library_name]
  
library = get_library('truefx_fx_rates')


def get_last_update(symbol):
    """ Return last updated time for a given symbol. If data is not found in
    the database, this returns the start date - 1 day. """
    if library.has_symbol(symbol):
        # check last_datetime and work out which zip to download
        return library.read(symbol).metadata['last_datetime']
    else:
        return start_date - timedelta(days=1)


def get_download_url(ccy_pair, download_date):
    filename = '{}-{}.zip'.format(ccy_pair, download_date.strftime('%Y-%m'))
    if download_date < url_change_date:
        dir = download_date.strftime('%B-%Y').upper()
    else:
        dir = download_date.strftime('%Y-%m')

    url = ('{root_url}/{year}/{dir}/{filename}'
           .format(root_url=root_url, year=download_date.strftime('%Y'),
                   dir=dir, filename=filename))
    return url


def download_zip_from_url(from_url, to_directory=expanduser('~')):
    request = requests.get(from_url, verify=False)
    zip = zipfile.ZipFile(io.BytesIO(request.content))
    zip.extractall(to_directory)


def get_df_from_csv(csv_filepath):
    data = pd.read_csv(csv_filepath, header=None, parse_dates=True,
                       index_col=0, usecols=[1, 2, 3])
    data.columns = ['bid', 'ask']
    data.index.name = 'timestamp'
    data.index = data.index.tz_localize('UTC')
    return data


def data_to_mongo(symbol, data):
    metadata = {'last_datetime': data.index[-1]}

    if library.has_symbol(symbol):
        # only append non-overlapping part
        existing_up_to = library.read(symbol).metadata['last_datetime']
        last_existing_data = library.read(symbol,
                                          date_range=DateRange(existing_up_to))
        if len(last_existing_data.data) != 1:
            raise ValueError('metadata and database are not consistent.')

        save_from = date_shift(existing_up_to, data_frequency)
        save_data = data.loc[save_from:, :]
        library.append(symbol, save_data, metadata=metadata)
    else:
        library.write(symbol, data, metadata=metadata)

        
if __name__ == '__main__':
    for ccy_pair in ccy_pairs:
        last_update = get_last_update(ccy_pair)
        download_date = date_shift(last_update, '+MonthBegin')
        last_month = date_shift(datetime.today(), '-MonthEnd')

        while download_date < last_month:
            download_url = get_download_url(ccy_pair, download_date)
            filename = download_url.split('/')[-1].replace('zip', 'csv')
            download_zip_from_url(download_url, download_to)
            csv_filepath = join(download_to, filename)
            data = get_df_from_csv(csv_filepath)
            data = data[~data.index.duplicated(keep='last')].dropna() # remove duplicate rows and NaNs
            data_1min = data.sort_index().resample(data_frequency).last()

            is_unique = data_1min.index.is_unique
            is_monotonic_increasing = data_1min.index.is_monotonic_increasing
            if not is_unique or not is_monotonic_increasing:
                raise ValueError('Data appears to be corrupt. is_unique={}, '
                                 'is_monotonic_increasing={}'
                                 .format(is_unique, is_monotonic_increasing))

            data_to_mongo(ccy_pair, data_1min)
            download_date = date_shift(download_date, '+1m')
            remove(csv_filepath)
