# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import socket
from collections import namedtuple
from enum import Enum
import logging
from functools import wraps
import time
import pandas as pd
from datetime import datetime
from consts import *


log = logging.getLogger(__name__)

DATE_INPUT_FORMAT = '%Y-%m-%d %H:%M:%S'


def parse_minute(data):
    """
    :param data: Iqfeed data
    :return: Array JSON objects representing minute history data
    """
    result = []

    if len(data) is 0:
        return result

    for line in data.split('\n'):
        try:
            (datetime_str, high, low, open_, close, volume, _, _) = line.split(',')
            line_data = {
                IQ_TIME_COL: datetime.strptime(datetime_str, DATE_INPUT_FORMAT),
                IQ_HIGH_COL: float(high),
                IQ_LOW_COL: float(low),
                IQ_OPEN_COL: float(open_),
                IQ_CLOSE_COL: float(close),
            }
            result.append(line_data)
        except ValueError:
            if 'NO_DATA' in line:
                log.info('No data received.')
            else:
                log.info(data)
            return []

    return result


def parse_day(data):
    return parse_minute(data)


def parse_tick():
    pass



def retry(tries, exceptions=None, delay=0):
    """
    Decorator for retrying a function if exception occurs
    Source: https://gist.github.com/treo/728327

    tries -- num tries
    exceptions -- exceptions to catch
    delay -- wait between retries
    """
    exceptions = exceptions or (Exception, )

    def _retry(fn):
        @wraps(fn)
        def __retry(*args, **kwargs):
            for _ in xrange(tries+1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    log.warning("Exception, retrying...", exc_info=e)
                    time.sleep(delay)
            raise  # If no success after tries raise last exception
        return __retry

    return _retry



def bars_to_dateframe(bars, tz):
    """Creates dataframe from list of Bar instances"""

    rows = [{'DateTime':  bar.datetime,
             'Open':      bar.open,
             'High':      bar.high,
             'Low':       bar.low,
             'Close':     bar.close,
             'Volume':    bar.volume,
             } for bar in bars]
    return pd.DataFrame.from_records(rows).set_index(['DateTime']).sort_index()


def tick_bars_to_dateframe(bars):
    rows = [{
        'DateTime': bar.datetime,
        'Last':     bar.last,
        'LastSize': bar.last_size,
        'Volume':   bar.volume,
        'Bid':      bar.bid,
        'Ask':      bar.ask,
        'TicketID': bar.ticket_id,
        } for bar in bars]
    return pd.DataFrame.from_records(rows).set_index(['DateTime']).sort_index()


def get_instruments_from_file(filename):
    """Load index from txt file"""
    instruments = []
    with open(filename, 'r') as f:
        for instrument in f:
            instruments.append(instrument.rstrip())
    if len(instruments) > 0:
        instruments = instruments[1:]
    return instruments





log = logging.getLogger(__name__)

Bar = namedtuple('IQFeedBar', ['datetime', 'open', 'high', 'low', 'close', 'volume'])
TickBar = namedtuple('IQFeedTickBar', ['datetime', 'last', 'last_size', 'volume', 'bid', 'ask', 'ticket_id'])


class DataType(Enum):
    DAY = 0
    MINUTE = 1
    TICK = 2

PARSERS = {
    DataType.DAY: parser_table.parse_day,
    DataType.MINUTE: parser_table.parse_minute,
    DataType.TICK: parser_table.parse_tick,
}

BEGIN_TIME_FILTER = '093000'
END_TIME_FILTER = '160000'
BARS_PER_MINUTE = 60
HOST = 'localhost'
PORT = 9100
TIMEOUT = 10.0
DATE_FORMAT = '%Y%m%d %H%M%S'


@retry(5, delay=2)
def get_data(instrument, start_time, end_time, data_type):
    if data_type is DataType.TICK:
        raise NotImplementedError('Not implemented')
    try:
        start_time = start_time.strftime(DATE_FORMAT)
        end_time = end_time.strftime(DATE_FORMAT)

        socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_.connect((HOST, PORT))

        socket_.settimeout(TIMEOUT)
        socket_.sendall(_get_request_string(data_type, instrument, start_time, end_time))

        data = _read_historical_data_socket(socket_)
    finally:
        socket_.close()

    return PARSERS[data_type](data)


def _read_historical_data_socket(sock, receive_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    data_buffer = ""
    while True:
        data = sock.recv(receive_buffer)
        data_buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in data_buffer:
            break

    # Remove the end message string
    data_buffer = data_buffer[:-12]
    return data_buffer


def _get_request_string(data_type, instrument, start_date, end_date):
    if data_type is DataType.DAY:
        return 'HDT,{0},{1},{2},,,,1\n'.format(instrument, start_date, end_date)
    elif data_type is DataType.MINUTE:
        return 'HIT,{0},{1},{2},{3},,{4},{5},1\n'\
            .format(instrument, BARS_PER_MINUTE, start_date, end_date, BEGIN_TIME_FILTER, END_TIME_FILTER)
    elif data_type is DataType.TICK:
        return 'HTT,{0},{1},{2},,{3},{4},1,,\n'\
            .format(instrument, start_date, end_date, BEGIN_TIME_FILTER, END_TIME_FILTER)

