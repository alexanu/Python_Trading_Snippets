from abc import *
from typing import NamedTuple


class FilterProvider(metaclass=ABCMeta):
    """Base namedtuple filter provider generator/iterator interface"""

    @abstractmethod
    def __iter__(self):
        return

    @abstractmethod
    def __next__(self) -> NamedTuple:
        return


class DefaultFilterProvider(FilterProvider):
    """Default filter provider, which contains a list of filters"""

    def __init__(self, repeat=True):
        self._filters = list()
        self._repeat = repeat

    def __iadd__(self, fn):
        self._filters.append(fn)
        return self

    def __isub__(self, fn):
        self._filters.remove(fn)
        return self

    def __iter__(self):
        self.__counter = 0
        return self

    def __next__(self) -> FilterProvider:
        if not self._repeat and self.__counter >= len(self._filters):
            raise StopIteration
        else:
            self.__counter += 1
            return self._filters[self.__counter % len(self._filters)]
