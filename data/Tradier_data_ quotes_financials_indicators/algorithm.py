from typing import Any
from streamz import Stream
import numpy as np
import libkloudtrader.stocks as stocks
from .exceptions import InvalidAlgorithmMode, EmptySymbolBucket

source = Stream()


def run(mode: str,
        strategy_name: str,
        symbol_bucket: list,
        states: list = ['open'],
        start_date: Any = None,
        end_date: Any = None,
        initial_cash: float = None):
    try:
        if mode not in ('backtest', 'live'):
            raise InvalidAlgorithmMode(
                "{} is an invalid mode for running your algorithm. Please select from either 'backtest' or 'live' modes."
                .format(mode))
        if not symbol_bucket:
            raise EmptySymbolBucket(
                'Symbol Bucket is empty. It must containe at least one equity or crypto symbol to trade.'
            )
        symbol_bucket = np.array(symbol_bucket)
        if mode == "live":
            print("{} is now entering the live markets. All the Best!".format(
                strategy_name.__name__))
            while stocks.intraday_status()['state'] in states:
                source.map(strategy_name).sink(print)
                for i in symbol_bucket:
                    source.emit(i)
        elif mode == "backtest":
            source.map(strategy_name).sink(backtest)
            for symbol in symbol_bucket:
                print("\nBacktesting {} on symbol {}\n".format(
                    strategy_name.__name__, symbol))
                source.emit(symbol)

    except Exception as exception:
        raise exception
