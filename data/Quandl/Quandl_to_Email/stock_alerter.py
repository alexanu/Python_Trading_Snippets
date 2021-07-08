from stock_alert_formatter import StockAlertFormatter
from stock_alert_rule_processor import StockAlertRuleProcessor
from mailer import Mailer
import datetime
import argparse
import os

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--to_address", required=True, help="Mail address to which stock alerts are to be sent")
    parser.add_argument("--alert_rule_file", required=True, help="JSON file path containing stock alert rules")
    return parser.parse_args()

if __name__ == '__main__':
    args = process_args()

    stock_alert_rule_processor = StockAlertRuleProcessor(args.alert_rule_file)
    prev_day = stock_alert_rule_processor.get_latest_date_with_data()
    stock_alerts = list(stock_alert_rule_processor.get_stock_alerts(prev_day))
    mailer = Mailer()
    if len(stock_alerts) > 0:
        mailer.send_mail(from_address="stock_alerter@mmmtrading.com",
                        to_address=args.to_address,
                        subject="MMM Trading Stock Alerts!!",
                        body=StockAlertFormatter().get_alerts_in_html(stock_alerts),
                        content_type="text/html")
