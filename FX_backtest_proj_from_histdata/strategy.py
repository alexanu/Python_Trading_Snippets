from abc import ABC, abstractmethod


class AbstractStrategy(ABC):

    @abstractmethod
    def execute(self, account, timestamp, current_prices):
        pass
