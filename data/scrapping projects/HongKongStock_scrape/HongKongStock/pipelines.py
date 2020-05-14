import logging
from SQLManager import SQLManager


class Pipeline(object):

    def __init__(self):
        self.sql_manager = SQLManager()

    def process_item(self, item, spider):
        if spider.name == 'SpecificStockSpider':
            self.store_stock_seats(item)
        elif spider.name == 'ShortSellingSpider':
            self.store_short_selling(item)
        elif spider.name == 'CashStkSpider':
            self.store_cash_stk(item)

    def store_stock_seats(self, item):
        param = dict(zip(item.keys(), map(item.get, item.keys())))
        sql = u'INSERT INTO HongKongStock.SpecificStock(status, ' \
              u'stock_id, create_date, BrokerNo, turnover, shares,' \
              u'percent, AV) VALUES ("{status}", "{stock_id}",' \
              u'"{create_date}", "{BrokerNo}", "{turnover}", "{shares}",' \
              u'{percent}, "{AV}")'.format(**param)
        self.sql_manager.execute(sql)
        logging.info(u'{create_date}-{stock_id}-{BrokerNo} '
                     u'stored in db.'.format(**param))

    def store_short_selling(self, item):
        param = dict(zip(item.keys(), map(item.get, item.keys())))
        sql = u'INSERT INTO HongKongStock.ShortSelling(stock_id,BrokerNo,' \
              u'create_date, short_selling_amount, short_selling_amount_extent,' \
              u'short_selling_amount_accounting, short_selling_turnover, ' \
              u'short_selling_turnover_extent, short_selling_turnover_accounting,' \
              u'price, increase, increase_amount, volumn, turnover) VALUES ' \
              u'("{stock_id}", "{BrokerNo}", "{create_date}", "{short_selling_amount}", ' \
              u'{short_selling_amount_extent}, {short_selling_amount_accounting}, ' \
              u'"{short_selling_turnover}", {short_selling_turnover_extent}, ' \
              u'{short_selling_turnover_accounting}, {price}, {increase}, ' \
              u'{increase_amount}, "{volumn}", "{turnover}")'.format(**param)
        self.sql_manager.execute(sql)
        logging.info(u'{create_date}-{stock_id} stored in ShortSelling.'
                     .format(**param))

    def store_cash_stk(self, item):
        param = dict(zip(item.keys(), map(item.get, item.keys())))
        sql = u'INSERT INTO HongKongStock.CashStk(stock_id, BrokerNo,' \
              u'create_date, increase, price, inflow_funds, ' \
              u'outflow_funds, net_inflow, net_inflow_rate,' \
              u'small_net_inflow, small_accounting, medium_net_inflow,' \
              u'medium_net_accounting, big_net_inflow, big_accounting,' \
              u'large_net_inflow, large_accounting) VALUES ' \
              u'("{stock_id}", "{BrokerNo}",' \
              u'"{create_date}", "{increase}", {price}, "{inflow_funds}", ' \
              u'"{outflow_funds}", "{net_inflow}", "{net_inflow_rate}",' \
              u'"{small_net_inflow}", "{small_accounting}", "{medium_net_inflow}",' \
              u'"{medium_net_accounting}", "{big_net_inflow}", "{big_accounting}",' \
              u'"{large_net_inflow}", "{large_accounting}")'.format(**param)
        self.sql_manager.execute(sql)
        logging.info(u'{create_date}-{stock_id} stored in CashStk'.format(**param))
