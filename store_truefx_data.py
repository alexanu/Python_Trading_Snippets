import requests
import zipfile
import io
from os.path import expanduser, join
from os import remove
from datetime import datetime, timedelta

import pandas as pd
from arctic.date import DateRange

from adagio.utils.date import date_shift
from adagio.utils.logging import get_logger
from adagio.utils.mongo import get_library

logger = get_logger(__name__)
library = get_library('truefx_fx_rates')

ccy_pairs = ['AUDJPY', 'AUDNZD', 'AUDUSD', 'CADJPY', 'CHFJPY', 'EURCHF',
             'EURGBP', 'EURJPY', 'EURUSD', 'GBPJPY', 'GBPUSD', 'NZDUSD',
             'USDCAD', 'USDCHF', 'USDJPY']

start_date = datetime(2009, 5, 1)
url_change_date = datetime(2017, 4, 1)
root_url = r'https://www.truefx.com/dev/data'
download_to = expanduser('~')
data_frequency = '1min'


def get_last_update(symbol):
    """ Return last updated time for a given symbol. If data is not found in
    the database, this returns the start date - 1 day. """
    if library.has_symbol(symbol):
        # check last_datetime and work out which zip to download
        return library.read(symbol).metadata['last_datetime']
    else:
        return start_date - timedelta(days=1)


def get_download_url(ccy_pair, download_date):
    """ Return download url """
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
    metadata = {
        'last_datetime': data.index[-1]
    }

    if library.has_symbol(symbol):
        # only append non-overlapping part
        existing_up_to = library.read(symbol).metadata['last_datetime']
        last_existing_data = library.read(symbol,
                                          date_range=DateRange(existing_up_to))
        if len(last_existing_data.data) != 1:
            raise ValueError('metadata and database are not consistent.')

        save_from = date_shift(existing_up_to, data_frequency)
        save_data = data.loc[save_from:, :]
        logger.debug('Append {} rows'.format(len(save_data)))
        library.append(symbol, save_data, metadata=metadata)
    else:
        logger.debug('Write {} rows'.format(len(data)))
        library.write(symbol, data, metadata=metadata)


if __name__ == '__main__':
    for ccy_pair in ccy_pairs:
        logger.info('Updating {}'.format(ccy_pair))
        last_update = get_last_update(ccy_pair)
        download_date = date_shift(last_update, '+MonthBegin')
        last_month = date_shift(datetime.today(), '-MonthEnd')

        while download_date < last_month:
            logger.info('Downloading {}-{}'.format(download_date.year,
                                                   download_date.month))
            download_url = get_download_url(ccy_pair, download_date)
            filename = download_url.split('/')[-1].replace('zip', 'csv')

            logger.debug('Try download: {}'.format(download_url))
            download_zip_from_url(download_url, download_to)

            logger.debug('Loading csv: {}'.format(filename))
            csv_filepath = join(download_to, filename)
            data = get_df_from_csv(csv_filepath)
            # remove duplicate rows, also sometimes csv contains NaNs.
            data = data[~data.index.duplicated(keep='last')].dropna()
            data_1min = data.sort_index().resample(data_frequency).last()

            is_unique = data_1min.index.is_unique
            is_monotonic_increasing = data_1min.index.is_monotonic_increasing
            if not is_unique or not is_monotonic_increasing:
                raise ValueError('Data appears to be corrupt. is_unique={}, '
                                 'is_monotonic_increasing={}'
                                 .format(is_unique, is_monotonic_increasing))

            logger.debug('Pushing to MongoDB')
            data_to_mongo(ccy_pair, data_1min)
            download_date = date_shift(download_date, '+1m')
            remove(csv_filepath)