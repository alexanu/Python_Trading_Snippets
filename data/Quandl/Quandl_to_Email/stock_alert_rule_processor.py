import quandl
import json
import datetime
import os

class StockAlertRuleProcessor(object):
    def __init__(self, alert_rule_file):
        if os.environ["QUANDL_API_KEY"] is None:
            raise EnvironmentError("Missing env - QUANDL_API_KEY")

        quandl.ApiConfig.api_key = os.environ["QUANDL_API_KEY"]

        with open(alert_rule_file) as fp:
            self.stock_alert_rules = json.load(fp)

        self.symbol_list = list(
            map(lambda alert_rule: alert_rule["symbol"], self.stock_alert_rules))

    def get_stock_prices(self, date):
        symbol_close_df = quandl.get(
            self.symbol_list, start_date=date, column_index="5")  # Close price
        print(symbol_close_df)
        symbol_close_prices = map(lambda symbol: (
            symbol, symbol_close_df[symbol + " - Close"][date] if date in symbol_close_df[symbol + " - Close"] else 0.0), self.symbol_list)
        return symbol_close_prices

    def get_latest_date_with_data(self):
        return quandl.get("NSE/TCS", rows=1).index[0].strftime("%Y-%m-%d")

    def get_stock_alerts(self, date):
        symbol_close_prices = dict(self.get_stock_prices(date))
        rules_by_symbol = dict(
            map(lambda rule: (rule["symbol"], rule), self.stock_alert_rules))

        # Alert Object -> symbol, rule, close price
        symbols_to_alert = filter(lambda symbol: self.__raise_alert(
            rules_by_symbol[symbol], symbol_close_prices[symbol]), self.symbol_list)

        return map(lambda symbol: {"symbol": symbol,
                                   "alert_rule": rules_by_symbol[symbol],
                                   "price": symbol_close_prices[symbol]
                                   },
                   symbols_to_alert)

    def __raise_alert(self, rule, price):
        return price <= rule["alert_when_less_than"] or price >= rule["alert_when_greater_than"]

if __name__ == '__main__':
    prev_day = str(datetime.date.today() - datetime.timedelta(days=1))

    alerter = StockAlertRuleProcessor("./rules.json")
    date = alerter.get_latest_date_with_data()
    print(date)
    print(list(alerter.get_stock_alerts(date)))
    
