

class Trade:

    TYPE_BUY = 'BUY'
    TYPE_SELL = 'SELL'

    def __init__(self, pair, price, trade_type, amount):
        """
        :param pair: The currency tuple, e.g. ('EUR','USD')
        :param price: The price the trade was opened at.
        :param trade_type: Either 'SELL' or 'BUY'.
        :param amount: The amount of money invested in this trade.
        """

        self._pair = pair
        self._price = price
        self._trade_type = trade_type
        self._amount = amount
        self._take_profit_price = None
        self._stop_loss_price = None

    @property
    def pair(self):
        return self._pair

    @property
    def price(self):
        return self._price

    @property
    def is_buy(self):
        return self._trade_type == self.__class__.TYPE_BUY

    @property
    def is_sell(self):
        return self._trade_type == self.__class__.TYPE_SELL

    @property
    def amount(self):
        return self._amount

    @property
    def take_profit_price(self):
        return self._take_profit_price

    @take_profit_price.setter
    def take_profit_price(self, take_profit_price):
        if self.is_buy and take_profit_price <= self._price:
            raise ValueError('Take profit price ({}) must be bigger than a BUY trade '
                             'with price ({})!'.format(take_profit_price, self._price))

        if self.is_sell and take_profit_price >= self._price:
            raise ValueError('Take profit price ({}) must be smaller than a SELL trade '
                             'with price ({})!'.format(take_profit_price, self._price))

        self._take_profit_price = take_profit_price

    @property
    def stop_loss_price(self):
        return self._stop_loss_price

    @stop_loss_price.setter
    def stop_loss_price(self, stop_loss_price):
        if self.is_buy and stop_loss_price >= self._price:
            raise ValueError('Stop loss price ({}) must be smaller than a BUY trade\'s '
                             'price ({})!'.format(stop_loss_price, self._price))

        if self.is_sell and stop_loss_price <= self._price:
            raise ValueError('Stop loss price ({}) must be bigger than a SELL trade\'s '
                             'price ({})!'.format(stop_loss_price, self._price))

        self._stop_loss_price = stop_loss_price

    def current_value(self, current_price):
        """ Returns the amount the trade is worth, given the current price of the forex pair."""

        if self.is_buy:
            percentage_change = (current_price - self._price) / self._price
        else:
            percentage_change = (self._price - current_price) / current_price
        return (1 + percentage_change) * self._amount

    def __str__(self):
        return '-> {} {} {{price={}, take_profit_price={}, stop_loss_price={}}}'\
            .format(self._trade_type, self._pair, self._price, self._take_profit_price, self._stop_loss_price)
