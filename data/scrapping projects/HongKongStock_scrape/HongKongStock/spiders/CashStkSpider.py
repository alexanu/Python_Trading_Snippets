# -*- coding: utf-8 -*-

import time
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from HongKongStock.items import CashStkItem


class CashStkSpider(CrawlSpider):
    name = "CashStkSpider"
    allowed_domains = ["http://data.tsci.com.cn"]
    start_urls = ['http://data.tsci.com.cn/CashFlow'
                  '/CashStk.aspx?P=%d' % i
                  for i in range(0, 32)]
    rules = (
        Rule(
            LinkExtractor(
                restrict_xpaths='//div[@class="Main shadow"]'
                                '//table//tbody'
            ),
            callback='parse'
        ),
    )

    def parse(self, response):
        # first three response.xpath('//tr') is not we want
        for res in response.xpath('//tr').extract()[3:]:
            item = CashStkItem()
            th_pattern = re.compile(r'<th><a\s*\S*\s'
                                    r'target=\S*>'
                                    r'(?P<stock_id>\S*\s\S*)'
                                    r'</a></th>')
            if len(th_pattern.findall(res)) == 1:
                stock_id = th_pattern.findall(res)[0]
                item['BrokerNo'] = stock_id
                item['stock_id'] = 'E'+stock_id.split()[0]
            item['create_date'] = time.strftime('%Y-%m-%d',
                                                time.localtime(time.time()))
            # the format of the page is sick, i have to parse it in this way
            param = [
                data.replace('<td>', '').\
                replace('</th>', '').\
                replace('</font>', '').\
                replace('\r\n', '').split('</td>')
                for data in re.findall('>(?P<val>\s*\S*)</',res)[1:]]
            param = reduce(lambda x, y: x+y, param)[1:]
            item['increase'] = param[0]
            item['price'] = param[1]
            item['inflow_funds'] = param[2]
            item['outflow_funds'] = param[3]
            item['net_inflow'] = param[4]
            item['net_inflow_rate'] = param[5]
            item['small_net_inflow'] = param[6]
            item['small_accounting'] = param[7]
            item['medium_net_inflow'] = param[8]
            item['medium_net_accounting'] = param[9]
            item['big_net_inflow'] = param[10]
            item['big_accounting'] = param[11]
            item['large_net_inflow'] = param[12]
            item['large_accounting'] = param[13]
            yield item
