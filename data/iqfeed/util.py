import logging
import os
import queue
import tempfile
import typing
import zipfile
from collections import OrderedDict, deque

import numpy as np
import pandas as pd
import requests

import pyiqfeed as iq


def dtn_credentials():
    return os.environ['DTN_PRODUCT_ID'], os.environ['DTN_LOGIN'], os.environ['DTN_PASSWORD'], 'Debugging'


def launch_service():
    """Check if IQFeed.exe is running and start if not"""
    dtn_product_id, dtn_login, dtn_password, version = dtn_credentials()

    svc = iq.FeedService(product=dtn_product_id,
                         version=version,
                         login=dtn_login,
                         password=dtn_password)

    headless = bool(os.environ["DTN_HEADLESS"]) if "DTN_HEADLESS" in os.environ else "DISPLAY" not in os.environ
    logging.getLogger(__name__).info("Launching IQFeed service in " + ("headless mode" if headless else "non headless mode"))

    svc.launch(headless=headless)


def iqfeed_to_df(data: typing.Collection):
    """
    Create minibatch-type data frame based on the pyiqfeed data format
    :param data: data list
    :return:
    """
    result = None

    for i, datum in enumerate(data):
        datum = datum[0] if len(datum) == 1 else datum

        if result is None:
            result = OrderedDict(
                [(n.replace(" ", "_").lower(),
                  np.empty((len(data),), d.dtype if str(d.dtype) not in ('|S4', '|S2', '|S3') else object))
                 for n, d in zip(datum.dtype.names, datum)])

        for j, f in enumerate(datum.dtype.names):
            d = datum[j]
            if isinstance(d, bytes):
                d = d.decode('ascii')

            result[f.replace(" ", "_").lower()][i] = d

    return pd.DataFrame(result)


def iqfeed_to_deque(data: typing.Iterable, maxlen: int = None):
    """
    Create minibatch-type dict of deques based on the pyiqfeed data format
    :param data: data list
    :param maxlen: maximum deque length
    :return:
    """
    result = None

    for i, datum in enumerate(data):
        datum = datum[0] if len(datum) == 1 else datum

        if result is None:
            result = OrderedDict(
                [(n.replace(" ", "_").lower(),
                  deque(maxlen=maxlen))
                 for n, d in zip(datum.dtype.names, datum)])

        for j, f in enumerate(datum.dtype.names):
            d = datum[j]
            if isinstance(datum[j], bytes):
                d = datum[j].decode('ascii')

            result[f.replace(" ", "_").lower()].append(d)

    return result


def get_last_value(data: dict) -> dict:
    """
    If the data is a result is a time-serires (dict of collections), return the last one
    :param data: data list
    :return:
    """
    return OrderedDict([(k, v[-1] if isinstance(v, typing.Collection) else v) for k, v in data.items()])


def iqfeed_to_dict(data):
    """
    Turn one iqfeed data item to dict
    :param data: data list
    :return:
    """
    data = data[0] if len(data) == 1 else data

    result = OrderedDict([(n.replace(" ", "_").lower(), d) for n, d in zip(data.dtype.names, data)])

    for k, v in result.items():
        if isinstance(v, bytes):
            result[k] = v.decode('ascii')
        elif pd.isnull(v):
            result[k] = None

    return result


def get_symbols(symbols_file: str = None, flt: dict = None):
    """
    Get available symbols and information about them

    :param symbols_file: location of the symbols file (if None, the file is downloaded)
    :param flt: filter for the symbols
    """

    with tempfile.TemporaryDirectory() as td:
        if symbols_file is not None:
            logging.getLogger(__name__).info("Symbols: " + symbols_file)
            zipfile.ZipFile(symbols_file).extractall(td)
        else:
            with tempfile.TemporaryFile() as tf:
                logging.getLogger(__name__).info("Downloading symbol list... ")
                tf.write(requests.get('http://www.dtniq.com/product/mktsymbols_v2.zip', allow_redirects=True).content)
                zipfile.ZipFile(tf).extractall(td)

        with open(os.path.join(td, 'mktsymbols_v2.txt')) as f:
            content = f.readlines()

    logging.getLogger(__name__).debug("Filtering companies...")

    flt = {'SECURITY TYPE': 'EQUITY', 'EXCHANGE': {'NYSE', 'NASDAQ'}} if flt is None else flt

    cols = content[0].split('\t')
    positions = {cols.index(k): v if isinstance(v, set) else {v} for k, v in flt.items()}

    result = dict()
    for c in content[1:]:
        split = c.split('\t')
        if all([split[col] in positions[col] for col in positions]):
            result[split[0]] = {cols[i]: split[i] for i in range(1, len(cols))}

    logging.getLogger(__name__).debug("Done")

    return result


class IQFeedDataProvider(object):
    """Streaming data provider generator/iterator interface"""

    def __init__(self, listeners, accept_event):
        self._queue = queue.Queue()
        self.listeners = listeners
        self.accept_event = accept_event

    def _populate_queue(self, event):
        if self.accept_event(event):
            self._queue.put(event['data'])

    def __iter__(self):
        return self

    def __next__(self) -> map:
        return self._queue.get()

    def __enter__(self):
        self.listeners += self._populate_queue

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.listeners -= self._populate_queue
