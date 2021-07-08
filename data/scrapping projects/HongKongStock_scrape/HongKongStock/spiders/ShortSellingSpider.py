# -*- coding: utf-8 -*-

import time
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from HongKongStock.items import ShortSellingItem


class ShortSellingSpider(CrawlSpider):
    name = "ShortSellingSpider"
    allowed_domains = ["http://data.tsci.com.cn"]
    start_urls = ['http://data.tsci.com.cn/indices'
                  '/ShortSelling.aspx?T=0&Order=%d' % i
                  for i in range(1, 11)]
    rules = (
        Rule(
            LinkExtractor(
                restrict_xpaths='//div[@class="Main"]'
                                '//form[@method="post"]'
                                '//table//tbody//tr'
            ),
            callback='parse'
        ),
    )

    def parse(self, response):
        # first seven response.xpath('//tr') is not we want
        for res in response.xpath('//tr').extract()[8:]:
            item = ShortSellingItem()
            th_pattern = re.compile(r'<th><a href="\S*">'
                                    r'(?P<stock_id>\S*\s\S*)'
                                    r'</a></th>')
            if len(th_pattern.findall(res)) == 1:
                stock_id = th_pattern.findall(res)[0]
                item['BrokerNo'] = stock_id
                item['stock_id'] = 'E'+stock_id.split()[0]
            td_pattern = re.compile(r'<td\s*\S*>(?P<val>\S*)</td>')
            tds = td_pattern.findall(res)
            item['create_date'] = time.strftime('%Y-%m-%d',
                                                time.localtime(time.time()))
            item['short_selling_amount'] = tds[0]
            item['short_selling_amount_extent'] = tds[1]
            item['short_selling_amount_accounting'] = tds[2]
            item['short_selling_turnover'] = tds[3]
            item['short_selling_turnover_extent'] = tds[4]
            item['short_selling_turnover_accounting'] = tds[5]
            item['price'] = tds[6]
            item['increase'] = tds[7]
            item['increase_amount'] = tds[8]
            item['volumn'] = tds[9]
            item['turnover'] = tds[10]
            yield item
