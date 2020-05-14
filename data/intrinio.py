import itertools
import json
import logging
import os
import queue
import threading
import typing
from io import StringIO
from multiprocessing.pool import ThreadPool

import pandas as pd
import requests.sessions as sessions


def to_dataframe(csv_str: str):
    """
    Convert csv result to DataFrame
    :param csv_str: csv string
    :return: pd.DataFrame
    """
    dates = [d for d in csv_str.split('\n', 1)[0].split(',') if 'date' in d or 'period' in d]
    return pd.read_csv(StringIO(csv_str), parse_dates=dates)


def get_csv(sess: sessions.Session, endpoint: str, **parameters):
    """
    get csv data from the Intrinio API
    :param sess: session
    :param endpoint: endpoint
    :param parameters: query parameters
    :return: csv result
    """
    auth = os.getenv('INTRINIO_USERNAME'), os.getenv('INTRINIO_PASSWORD')

    url = '{}/{}'.format('https://api.intrinio.com', endpoint + ('' if endpoint.endswith('.csv') else '.csv'))

    if 'page_size' not in parameters:
        parameters['page_size'] = 10000

    pages = list()

    for page_number in itertools.count():
        parameters['page_number'] = page_number + 1

        response = sess.request('GET', url, params=parameters, auth=auth, verify=True)
        if not response.ok:
            try:
                response.raise_for_status()
            except Exception as err:
                logging.getLogger(__name__).error(err)

        new_lines = response.content.decode('utf-8').count('\n')

        if new_lines == 1:
            break

        info, columns, page = response.content.decode('utf-8').split('\n', 2)

        if page_number == 0:
            info = {s.split(':')[0]: s.split(':')[1] for s in info.split(',')}
            total_pages = int(info['TOTAL_PAGES'])
            pages.append(columns.lower() + '\n')

        pages.append(page)

        if len(page) == 0 or page_number + 1 == total_pages:
            break

    return ''.join(pages) if len(pages) > 0 else None


def get_data(filters: typing.List[dict], threads=1, async=False, processor: typing.Callable = None):
    """
    Get async data for a list of filters. Works only for the historical API
    :param filters: a list of filters
    :param threads: number of threads for data retrieval
    :param async: if True, return queue. Otherwise, wait for the results
    :param processor: process the results

        For full list of available parameters check http://docs.intrinio.com/#historical-data

    :return Queue or pd.DataFrame with identifier, date set as multi index
    """
    q = queue.Queue(100)
    pool = ThreadPool(threads)
    global_counter = {'c': 0}
    lock = threading.Lock()
    no_data = set()

    with sessions.Session() as sess:
        def mp_worker(f):
            try:
                data = get_csv(sess, **f)
            except Exception as err:
                data = None
                logging.getLogger(__name__).exception(err)

            if data is not None:
                q.put(processor(data, **f) if processor is not None else (json.dumps(f), data))
            else:
                no_data.add(json.dumps(f))

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
                        logging.getLogger(__name__).info("No data found for " + str(len(no_data_list)) + " queries: " + str(no_data_list))
                        no_data.clear()

        if threads > 1 and len(filters) > 1:
            pool.map(mp_worker, (f for f in filters))
            pool.close()
        else:
            for f in filters:
                mp_worker(f)

        if not async:
            result = dict()
            for job in iter(q.get, None):
                if job is None:
                    break

                result[job[0]] = job[1]

            return result
        else:
            return q


def get_historical_data(filters: typing.List[dict], threads=1, async=False):
    for f in filters:
        if 'endpoint' not in f:
            f['endpoint'] = 'historical_data'
        elif f['endpoint'] != 'historical_data':
            raise Exception("Only historical data is allowed with this request")

    result = get_data(filters,
                      threads=threads,
                      async=async,
                      processor=_historical_data_processor)

    if not async and isinstance(result, dict):
        result = pd.concat(result)
        result.index.set_names('symbol', level=0, inplace=True)
        result = result.tz_localize('UTC', level=1, copy=False)

    return result


def _historical_data_processor(csv_str: str, **parameters):
    """
    Get historical data for given item and identifier
    :param csv_str: csv string
    :return pd.DataFrame with date set as index
    """

    result = to_dataframe(csv_str)
    tag = result.columns[1]
    result['tag'] = tag
    result.rename(columns={tag: 'value'}, inplace=True)
    result.set_index(['date', 'tag'], drop=True, inplace=True, append=True)
    result.reset_index(level=0, inplace=True, drop=True)

    return parameters['identifier'], result


class IntrinioEvents(object):
    """
    Intrinio requests via events
    """

    def __init__(self, listeners):
        self.listeners = listeners
        self.listeners += self.listener

    def listener(self, event):
        if event['type'] == 'intrinio_request':
            with sessions.Session() as sess:
                endpoint = event['endpoint'] if event['endpoint'].endswith('.csv') else event['endpoint'] + '.csv'
                if 'parameters' in event:
                    result = get_csv(sess, endpoint=endpoint, **event['parameters'])
                else:
                    result = get_csv(sess, endpoint=endpoint)

            if 'dataframe' in event:
                result = to_dataframe(result)

            self.listeners({'type': 'intrinio_request_result', 'data': result})
        elif event['type'] == 'intrinio_historical_data':
            data = event['data'] if isinstance(event['data'], list) else event['data']
            result = get_historical_data(data,
                                         threads=event['threads'] if 'threads' in event else 1,
                                         async=event['async'] if 'async' in event else False)

            self.listeners({'type': 'intrinio_historical_data_result', 'data': result})
