import os

import json
import logging
import socket
import ssl

from client.constants import Period
from client.models import RateInfoRecord

logger = logging.getLogger('XTBClient')
logger.setLevel(os.environ.get('XTB_LOG_LEVEL') or logging.DEBUG)


class XTBClient:
    def __init__(self, host='xapia.x-station.eu', port=5124, stream_port=5125):
        """
        X-Trades Broker client
        :param host: host of the demo or real servers (by default demo: 'xapia.x-station.eu')
        :param port: port of the demo or real servers (by default demo: 5124)
        :param stream_port: streaming port of the demo or real servers (by default demo: 5125)
        """
        # replace host name with IP, this should fail connection attempt,
        # but it doesn't in Python 2.x
        host = socket.getaddrinfo(host, port)[0][4][0]

        # create socket and connect to server
        # server address is specified later in connect() method
        self.sock = socket.socket()
        self.sock.connect((host, port))

        # wrap socket to add SSL support
        self.sock = ssl.wrap_socket(self.sock)

        self.stream = socket.socket()
        self.stream.connect((host, stream_port))

        self.stream = ssl.wrap_socket(self.stream)

        self.stream_session_id = None

    @staticmethod
    def _to_camel_case(string):
        first, *rest = string.split('_')
        if rest:
            return first.lower() + ''.join(word.capitalize() for word in rest)
        return first

    def _receive_socket_data(self, sock, buffer_size=4096):
        """Getting ALL the data pending to be received on the socket"""
        return b''.join(self._receive_socket_chucked_data(sock, buffer_size))

    @staticmethod
    def _receive_socket_chucked_data(sock, buffer_size=4096):
        """
        Yielding the data of the buffer in chucks of 4KiB
        :param buffer_size: size of the buffer to ask data from, by default 4KiB
        :return: received data
        """
        end_of_data = b'\n\n'  # the API uses this constant to determinate the end of the transmission
        data = sock.recv(buffer_size)
        while data:
            eod_idx = data.find(end_of_data)
            if eod_idx != -1:
                yield data[:eod_idx]
                break

            yield data
            if len(data) < buffer_size:
                # There is no more data left
                # break
                pass

            data = sock.recv(buffer_size)
        yield b''

    def _send_action(self, command, ret_format='json', arguments=None, sock='sock', **kwargs):
        """
        Sending supported commands to the API using the sock of the XTBClient object
        :param command: string defining the command to be sent
        :param kwargs: dictionary with the different options to the API
              note: the keys of the object will be transform from underscore_case to camelCase if required
        :param ret_format: can be either json or raw to get the answer back
        :return: response from the XTB server
        """
        payload = {"command": command, "prettyPrint": True}
        if kwargs:
            payload.update({self._to_camel_case(k): v for k, v in kwargs.items()})
        if arguments:
            payload["arguments"] = {self._to_camel_case(k): v for k, v in arguments.items()}

        payload_request = json.dumps(payload, indent=4)
        logger.debug(payload_request)

        sending_sock = self.sock if sock == 'sock' else self.stream
        sending_sock.send(payload_request.encode('utf-8'))

        received_data = self._receive_socket_data(sending_sock)
        if ret_format == 'json':
            return json.loads(received_data)
        elif ret_format == 'raw':
            return received_data
        else:
            raise ValueError('Invalid format {} specified'.format(ret_format))

    def login(self, user_id, password, app_id=None, app_name=None):
        """Login method to connect the client to XTB API
        :param user_id: user_id or string that identifies the user of the platform
        :param password: password to access
        :param app_id: app_id to get registered on the API
        :param app_name: app_name to get registered on the API
        :return: response from the API
        """
        params = {"user_id": user_id, "password": password}
        if app_id:
            params['app_id'] = app_id
        if app_name:
            params['app_name'] = app_name

        login_info = self._send_action('login', arguments=params)

        # Saving session id to be able to use it later
        self.stream_session_id = login_info.get('streamSessionId')
        return login_info

    def logout(self):
        """Logout method to disconnect the client from the API"""
        return self._send_action("logout")

    def ping(self):
        """Regularly calling this function is enough to refresh the internal
        state of all the components in the system.
        It is recommended that any application that does not execute other
        commands, should call this command at least once every 10 minutes.
        Please note that the streaming counterpart of this function is
        combination of ping and getKeepAlive.
        """
        return self._send_action("ping")

    def get_balance(self):
        """Allows to get actual account indicators values in real-time,
        as soon as they are available in the system."""
        if not self.stream_session_id:
            raise ValueError('Client not logged in, please log in before try to perform this action')
        return self._send_action("getBalance", sock='stream', **{"stream_session_id": self.stream_session_id}).get(
            'data')

    def get_all_symbols(self):
        """Returns array of all symbols available for the user"""
        return self._send_action("getAllSymbols").get('returnData')

    def get_chart_range_request(self, symbol, start, end, period=Period.h4, ticks=0):
        """Returns chart info with data between given start and end dates.
        :param symbol {string} Symbol
        :param start {timestamp} Start of chart block (rounded down to the nearest interval and excluding)
        :param end {timestamp} Time	End of chart block (rounded down to the nearest interval and excluding)
        :param period {int}	Number of minutes for the period, please use Period class on constants
        :param ticks {int} Number of ticks needed, this field is optional
                Ticks field - if ticks is not set or value is 0, getChartRangeRequest works as before (you must send valid start and end time fields).
                If ticks value is not equal to 0, field end is ignored.
                If ticks >0 (e.g. N) then API returns N candles from time start.
                If ticks <0 then API returns N candles to time start.
                It is possible for API to return fewer chart candles than set in tick field.
        :return [RateInfoRecord]
        """
        res = self._send_action("getChartRangeRequest", arguments={
            'info': {
                "end": end,
                "period": period,
                "start": start,
                "symbol": symbol,
                "ticks": ticks
            }})
        if res.get('status') is not True:
            raise ValueError('Some error getting the response {}'.format(res))
        return_data = res.get('returnData')
        digits = res.get('digits')
        return [RateInfoRecord(digits=digits, **info) for info in return_data.get('rateInfos')]
