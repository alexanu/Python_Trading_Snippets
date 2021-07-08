import glob
import logging
import os
import queue
import tempfile
import threading
import typing
import zipfile
from enum import Enum
from multiprocessing.pool import ThreadPool

import pandas as pd
import quandl


def get_time_series(filters: typing.List[dict], threads=1, async=False, processor: typing.Callable = None):
    """
    Get async data for a list of filters. Works only for the historical API
    :param filters: a list of filters
    :param threads: number of threads for data retrieval
    :param async: if True, return queue. Otherwise, wait for the results
    :param processor: process each result
    :return Queue or pd.DataFrame with identifier, date set as multi index
    """

    return __get_data(filters=filters, api_type=__APIType.TIME_SERIES, threads=threads, async=async, processor=processor)


def get_table(filters: typing.List[dict], threads=1, async=False, processor: typing.Callable = None):
    """
    Get async data for a list of filters. Works only for the historical API
    :param filters: a list of filters
    :param threads: number of threads for data retrieval
    :param async: if True, return queue. Otherwise, wait for the results
    :param processor: process each result
    :return Queue or pd.DataFrame with identifier, date set as multi index
    """

    return __get_data(filters=filters, api_type=__APIType.TABLES, threads=threads, async=async, processor=processor)


class __APIType(Enum):
    TIME_SERIES = 1
    TABLES = 2


def __get_data(filters: typing.List[dict], api_type: __APIType, threads=1, async=False, processor: typing.Callable = None):
    """
    Get async data for a list of filters using the tables or time series api
    :param filters: a list of filters
    :param api_type: whether to use time series or tables
    :param threads: number of threads for data retrieval
    :param async: if True, return queue. Otherwise, wait for the results
    :param processor: process each result
    :return Queue or pd.DataFrame with identifier, date set as multi index
    """
    api_k = os.environ['QUANDL_API_KEY'] if 'QUANDL_API_KEY' in os.environ else None
    q = queue.Queue(100)
    global_counter = {'c': 0}
    lock = threading.Lock()
    no_data = set()

    def mp_worker(f):
        try:
            data = None
            if api_type == __APIType.TIME_SERIES:
                data = quandl.get(**f, paginate=True, api_key=api_k)
                if data is not None:
                    data = data.tz_localize('UTC', copy=False)
                    q.put((f['dataset'], processor(data, **f) if processor is not None else data))
            elif api_type == __APIType.TABLES:
                data = quandl.get_table(**f, paginate=True, api_key=api_k)
                if data is not None:
                    q.put((f['datatable_code'], processor(data, **f) if processor is not None else data))

        except Exception as err:
            data = None
            logging.getLogger(__name__).exception(err)

        if data is None:
            no_data.add(f)

        with lock:
            global_counter['c'] += 1
            cnt = global_counter['c']
            if cnt == len(filters):
                q.put(None)

            if cnt % 20 == 0 or cnt == len(filters):
                logging.getLogger(__name__).info("Loaded " + str(cnt) + " queries")
                if len(no_data) > 0:
                    no_data_list = list(no_data)
                    no_data_list.sort()
                    logging.getLogger(__name__).info("No data found for " + str(len(no_data_list)) + " datasets: " + str(no_data_list))
                    no_data.clear()

    if threads > 1 and len(filters) > 1:
        pool = ThreadPool(threads)
        pool.map(mp_worker, (f for f in filters))
        pool.close()
    else:
        for f in filters:
            mp_worker(f)

    if not async:
        result = dict()
        while True:
            job = q.get()
            if job is None:
                break

            if job[0] in result:
                current = result[job[0]]
                if isinstance(current, list):
                    current.append(job[1])
                else:
                    result[job[0]] = [result[job[0]], job[1]]
            else:
                result[job[0]] = job[1]

        return result
    else:
        return q


def bulkdownload(dataset: str, chunksize=None):
    with tempfile.TemporaryDirectory() as td:
        filename = os.path.join(td, dataset + '.zip')
        logging.getLogger(__name__).info("Downloading dataset " + dataset + " to " + filename)
        quandl.bulkdownload(dataset, filename=filename, api_key=os.environ['QUANDL_API_KEY'] if 'QUANDL_API_KEY' in os.environ else None)
        zipfile.ZipFile(filename).extractall(td)

        logging.getLogger(__name__).info("Done... Start yielding dataframes")

        return pd.read_csv(glob.glob(os.path.join(td, '*.csv'))[0], header=None, chunksize=chunksize, parse_dates=[1])


def get_sf1(filters: typing.List[dict], threads=1, async=False):
    """
    return core us fundamental data
    :param filters: list of filters
    :param threads: number of request threads
    :param async: wait for the result or return a queue
    :return:
    """

    def _sf1_processor(df, dataset):
        df.rename(columns={'Value': 'value'}, inplace=True)
        df.index.rename('date', inplace=True)
        df = df.tz_localize('UTC', copy=False)
        df['symbol'], df['indicator'], df['dimension'] = dataset.split('/')[1].split('_')
        df.set_index(['symbol', 'indicator', 'dimension'], drop=True, inplace=True, append=True)

        return df

    result = get_time_series(filters,
                             threads=threads,
                             async=async,
                             processor=_sf1_processor)

    if not async and isinstance(result, list):
        result = pd.concat(result)
        result.sort_index(inplace=True, ascending=True)

    return result


def bulkdownload_sf0():
    df = bulkdownload(dataset='SF0', chunksize=None)
    sid = df[0]
    df.drop(0, axis=1, inplace=True)
    df = pd.concat([df, sid.str.split('_', expand=True)], axis=1, copy=False)
    df.columns = ['date', 'value', 'symbol', 'indicator', 'dimension']
    df.set_index(['date', 'symbol', 'indicator', 'dimension'], drop=True, inplace=True, append=False)

    return df


class QuandlEvents(object):
    """
    Quandl requests via events
    """

    def __init__(self, listeners):
        self.listeners = listeners
        self.listeners += self.listener

    def listener(self, event):
        if event['type'] == 'quandl_timeseries_request':
            result = get_time_series(event['data'] if isinstance(event['data'], list) else event['data'],
                                     threads=event['threads'] if 'threads' in event else 1,
                                     async=event['async'] if 'async' in event else False)

            self.listeners({'type': 'quandl_timeseries_result', 'data': result})
        elif event['type'] == 'quandl_table_request':
            result = get_table(event['data'] if isinstance(event['data'], list) else event['data'],
                               threads=event['threads'] if 'threads' in event else 1,
                               async=event['async'] if 'async' in event else False)

            self.listeners({'type': 'quandl_table_result', 'data': result})
