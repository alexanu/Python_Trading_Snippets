class StockAlertFormatter(object):
    def __convertAlertToHtmlRow(self, stock_alert):
        return '''<tr>
                    <td>{0}</td>
                    <td>{1}</td>
                    <td>{2}</td>
                    <td>{3}</td>
                </tr>'''.format(stock_alert["symbol"],
                                stock_alert["alert_rule"]["alert_when_less_than"],
                                stock_alert["alert_rule"]["alert_when_greater_than"],
                                stock_alert["price"])

    def get_alerts_in_html(self, stock_alerts):
        return '''<table border="1">
                    <thead>
                        <th>SYMBOL</th>
                        <th>Alert Rule Floor Price</th>
                        <th>Alert Rule Ceil Price</th>
                        <th>Stock Price</th>
                    </thead>
                    <tbody>{0}</tbody>
                </table>'''.format(''.join(map(lambda alert: self.__convertAlertToHtmlRow(alert), stock_alerts)))
