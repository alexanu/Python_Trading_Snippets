from forex.trade import Trade


class Account:

    OUTCOME_PROFIT = 'profit'
    OUTCOME_LOSS = 'loss'

    def __init__(self, balance):
        self._balance = balance
        self._trades = {}
        self._trade_count = 0
        self._trade_wins = 0
        self._trade_losses = 0

    @property
    def balance(self):
        return self._balance

    @property
    def trades_list(self):
        return self._trades.copy().values()

    def open_trade(self, pair, price, trade_type, amount, take_profit_price=None, stop_loss_price=None):
        """Creates a trade with the specified params, and updates the account's balance accordingly."""

        if self._balance - amount < 0:
            raise RuntimeError('Cannot proceed, balance is negative!')

        trade = Trade(pair, price, trade_type, amount)
        if take_profit_price:
            trade.take_profit_price = take_profit_price
        if stop_loss_price:
            trade.stop_loss_price = stop_loss_price

        self._trade_count += 1
        trade_id = self._trade_count
        self._trades[trade_id] = trade
        self._balance -= amount
        return trade_id, trade

    def close_trade(self, trade_id, current_price):
        """
        Closes a trade, given it's id and current price, and updates the account's balance and statistics accordingly.
        """

        trade = self._trades[trade_id]
        restored_amount = trade.current_value(current_price)
        del self._trades[trade_id]
        self._balance += restored_amount
        if restored_amount >= trade.amount:
            outcome = self.__class__.OUTCOME_PROFIT
            self._trade_wins += 1
        else:
            outcome = self.__class__.OUTCOME_LOSS
            self._trade_losses += 1
        return outcome

    def auto_close_trades(self, current_prices):
        """
        Closes the open trades in case their take profit/stop loss price thresholds are met based on
        the specified current prices.
        """

        result = []
        for trade_id, trade in self._trades.copy().items():
            price = current_prices[trade.pair]
            close_trade = (trade.is_sell and trade.take_profit_price is not None and price <= trade.take_profit_price)
            close_trade |= (trade.is_sell and trade.stop_loss_price is not None and price >= trade.stop_loss_price)
            close_trade |= (trade.is_buy and trade.take_profit_price is not None and price >= trade.take_profit_price)
            close_trade |= (trade.is_buy and trade.stop_loss_price is not None and price <= trade.stop_loss_price)
            if close_trade:
                outcome = self.close_trade(trade_id, price)
                result.append((trade_id, outcome))
        return result

    def net_balance_summary(self, current_prices):
        """
        Returns the net worth of the account, composed of the remaining balance and the open trades' value based on the
        specified current prices. The returned summary also includes trade statistics like number of wins and losses.
        """

        net_balance = self._balance
        for trade in self._trades.values():
            net_balance += trade.current_value(current_prices[trade.pair])
        summary = '{} units - trades [open {}, {} total, {} wins, {} losses]'.format(
            int(net_balance), list(self._trades.keys()), self._trade_count, self._trade_wins, self._trade_losses)
        return summary
