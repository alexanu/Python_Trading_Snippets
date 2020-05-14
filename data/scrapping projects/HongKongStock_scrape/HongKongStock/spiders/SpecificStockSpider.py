# -*- coding: utf-8 -*-

import time
import json
from scrapy.spiders import CrawlSpider
from HongKongStock.settings import STOCK_CODE, SPECIFIC_STOCK_API
from HongKongStock.items import StockSeat


class SpecificStockSpider(CrawlSpider):
    name = "SpecificStockSpider"
    allowed_domains = ["http://data.tsci.com.cn"]
    start_urls = [SPECIFIC_STOCK_API.format(code=code)
                  for code in STOCK_CODE]

    def parse(self, response):
        item = StockSeat()
        res = json.loads(response.body_as_unicode())
        res.pop('Total')
        for key in res.keys():
            item['status'] = key
            code = response.request.url.split('&')[-1].split('=')[-1]
            item['stock_id'] = code
            item['create_date'] = time.strftime('%Y-%m-%d',
                                                time.localtime(time.time()))
            for seat in res.get(key):
                item['BrokerNo'] = seat.get('BrokerNo')
                item['turnover'] = seat.get('turnover')
                item['shares'] = seat.get('shares')
                item['percent'] = seat.get('percent')
                item['AV'] = seat.get('AV')
                yield item
