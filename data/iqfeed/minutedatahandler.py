from abc import ABCMeta, abstractmethod
from datetime import timedelta, datetime, time
import logging

log = logging.getLogger(__name__)
DATE_FORMAT = '%Y%m%d %H%M%S'


class RequestHandler:

    __metaclass__ = ABCMeta

    def __init__(self, debug):
        self.begin_time_filter = '093000'
        self.end_time_filter = '160000'
        self.debug = debug

    @abstractmethod
    def get_request_string(self):
        pass

    @abstractmethod
    def get_json_array(self, data):
        pass

    @abstractmethod
    def get_collection_name(self):
        pass


class MinuteDataHandler(RequestHandler):

    def __init__(self, instrument, process_date, bars_per_minute=60, debug=False):
        super(MinuteDataHandler, self).__init__(debug)
        _process_datetime = datetime.combine(process_date, time.min)

        self.instrument = instrument
        self.start_date = _process_datetime.strftime(DATE_FORMAT)
        self.end_date = (_process_datetime + timedelta(days=1) - timedelta(seconds=1)).strftime(DATE_FORMAT)
        self.bars_per_minute = bars_per_minute

    def get_request_string(self):
        """
        :return: Request string sent to iqfeed client
        """
        return 'HIT,{0},{1},{2},{3},,{4},{5},1\n'\
            .format(self.instrument, self.bars_per_minute, self.start_date, self.end_date, self.begin_time_filter,
                    self.end_time_filter)

    def get_json_array(self, data):
        """
        :param data: Iqfeed data
        :return: Array JSON objects representing minute history data
        """
        result = []

        if len(data) is 0:
            return result

        for line in data.split('\n'):
            try:
                # Returned fields in data are:
                # datetime, high, low, open, close, volume, period volume, number of trades
                if self.debug:
                    log.debug(line)
                (datetime_str, high, low, open_, close, volume, _, _) = line.split(',')
                line_data = {
                    'datetime': datetime_str,
                    'high': high,
                    'low': low,
                    'open': open_,
                    'close': close,
                    'volume': volume
                }
                result.append(line_data)
            except ValueError:
                if 'NO_DATA' in line:
                    log.info('No data received.')
                    return []
                else:
                    raise
        return result

    def get_collection_name(self):
        return 'minute-data'
