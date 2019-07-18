from abc import ABCMeta, abstractmethod


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
