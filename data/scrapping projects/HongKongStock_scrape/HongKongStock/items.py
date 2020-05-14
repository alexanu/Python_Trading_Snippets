# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class StockSeat(scrapy.Item):
    stock_id = scrapy.Field()
    create_date = scrapy.Field()
    status = scrapy.Field()
    BrokerNo = scrapy.Field()
    turnover = scrapy.Field()
    shares = scrapy.Field()
    percent = scrapy.Field()
    AV = scrapy.Field()


class ShortSellingItem(scrapy.Item):
    BrokerNo = scrapy.Field()
    stock_id = scrapy.Field()
    create_date = scrapy.Field()
    short_selling_amount = scrapy.Field()
    short_selling_amount_extent = scrapy.Field()
    short_selling_amount_accounting = scrapy.Field()

    short_selling_turnover = scrapy.Field()
    short_selling_turnover_extent = scrapy.Field()
    short_selling_turnover_accounting = scrapy.Field()

    price = scrapy.Field()
    increase = scrapy.Field()
    increase_amount = scrapy.Field()
    volumn = scrapy.Field()
    turnover = scrapy.Field()


class CashStkItem(scrapy.Item):
    BrokerNo = scrapy.Field()
    stock_id = scrapy.Field()
    create_date = scrapy.Field()
    increase = scrapy.Field()
    price = scrapy.Field()
    inflow_funds = scrapy.Field()
    outflow_funds = scrapy.Field()
    net_inflow = scrapy.Field()
    net_inflow_rate = scrapy.Field()

    small_net_inflow = scrapy.Field()
    small_accounting = scrapy.Field()
    medium_net_inflow = scrapy.Field()
    medium_net_accounting = scrapy.Field()
    big_net_inflow = scrapy.Field()
    big_accounting = scrapy.Field()
    large_net_inflow = scrapy.Field()
    large_accounting = scrapy.Field()
