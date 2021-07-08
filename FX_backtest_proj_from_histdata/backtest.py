import logging
import forex.config as cfg
from forex.account import Account


class Backtest:

    def __init__(self, prices_file, balance, strategy):
        """
        :param prices_file: The file that has the pair prices.
        :param balance: The initial balance to start with.
        :param strategy: The instance of the strategy to backtest (subclass of AbstractStrategy)
        """
        self.prices_file = prices_file
        self.account = Account(balance)
        self.strategy = strategy

    @staticmethod
    def parse_entry(entry):
        parts = entry.strip().split(';')
        timestamp = parts[0]
        values = [0] * (len(parts) - 1)
        for i in range(1, len(parts)):
            values[i - 1] = float(parts[i])
        return timestamp, values

    def start(self):
        """Starts the replay of historical data against the user-defined strategy"""
        
        pairs = cfg.pairs()
        with open(self.prices_file) as file:
            while True:
                current_line = file.readline()
                if not current_line:
                    break

                timestamp, prices = self.__class__.parse_entry(current_line)
                if len(prices) != len(pairs):
                    raise RuntimeError('The number of prices in this entry ({}) does not match the number of pairs'
                                       .format(timestamp))

                prices_dict = {pairs[i]: prices[i] for i in range(len(pairs))}

                results = self.account.auto_close_trades(prices_dict)
                for trade_id, outcome in results:
                    logging.debug('{} Closed t{} with {}'.format(timestamp, trade_id, outcome))

                self.strategy.execute(self.account, timestamp, prices_dict)

        logging.info('Result: %s' % self.account.net_balance_summary(prices_dict))
