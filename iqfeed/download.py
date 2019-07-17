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
import logging

from tools import retry
from collections import namedtuple
from minutedatahandler import MinuteDataHandler

log = logging.getLogger(__name__)

Bar = namedtuple('IQFeedBar', ['datetime', 'open', 'high', 'low', 'close', 'volume'])


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


@retry(5, delay=2)
def get_data(ticker, process_date, iqfeed_host='localhost', iqfeed_port=9100, timeout=10.0):
    handler = MinuteDataHandler(instrument=ticker, process_date=process_date)
    try:
        socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_.connect((iqfeed_host, iqfeed_port))

        socket_.settimeout(timeout)
        socket_.sendall(handler.get_request_string())

        data = _read_historical_data_socket(socket_)
    finally:
        socket_.close()

    return handler.get_json_array(data)
