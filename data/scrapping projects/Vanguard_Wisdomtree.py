
# extract ETFs NAV
# source: https://github.com/scrapinghub/navscraper

from scrapy.contrib.loader import XPathItemLoader
from scrapy.contrib.loader.processor import TakeFirst, Compose
from scrapy.item import Item, Field


class FundItem(Item):
    id = Field()
    name = Field()
    symbol = Field()


class NavItem(Item):
    fund_id = Field()
    dates = Field()
    values = Field()


class FundLoader(XPathItemLoader):
    default_item_class = FundItem
    default_output_processor = TakeFirst()
    name_out = Compose(TakeFirst(), lambda s: s.strip())
    
    

from datetime import date, datetime

from scrapy import log
from scrapy.http import FormRequest, Request
from scrapy.selector import HtmlXPathSelector
from scrapy.spider import BaseSpider


class VanguardSpider(BaseSpider):
    """Vanguard.com ETF data scraper.

    Arguments
    ---------
    fund_id : str
        Fund's ID in Vanguard site.
    date_start : str (default: first day of this year)
        Start date in format: mm/dd/YY.
    date_end : str (default: today)
        End date in format: mm/dd/YY.
    """
    name = "vanguard"
    allowed_domains = ["vanguard.com"]

    # spider arguments
    fund_id = None
    date_start = None
    date_end = None

    date_format = '%m/%d/%Y'
    search_url = (
        'https://personal.vanguard.com/us/funds/tools/pricehistorysearch'
        '?FundId=null&FundType=ExchangeTradedShares'
    )

    def start_requests(self):
        if self.fund_id:
            date_start, date_end = self._get_dates_period()
            params = {
                # support comma separated values
                'fund_ids': self.fund_id.split(','),
                'date_start': date_start,
                'date_end': date_end,
            }
            yield Request(self.search_url, meta={'params': params},
                          callback=self.parse_form)
        else:
            self.log("Argument 'fund_id' missing.", level=log.ERROR)

    def parse_form(self, response):
        params = response.meta['params']
        for fund_id in params['fund_ids']:
            meta = {
                'item': NavItem(fund_id=fund_id),
            }
            data = {
                'FundId': fund_id,  # yes, first char is uppercase.
                'fundName': fund_id,
                'radiobutton2': '1',
                'radio': '1',
                'beginDate': params['date_start'],
                'endDate': params['date_end'],
                'results': 'get',
            }

            yield FormRequest.from_response(
                response, formname='FormNavigate', formdata=data, meta=meta,
                callback=self.parse_results)

    def parse_results(self, response):
        hxs = HtmlXPathSelector(response)
        results = hxs.select(
            '//tr[count(th) = 2 and th[1][text()="Date"]][1]'
            '/following-sibling::tr/td/text()'
        ).extract()

        if results:
            results = iter(results)
            dates, values = zip(*zip(results, results))
            item = response.meta['item']
            item.update({
                'dates': map(self._parse_date, dates),
                'values': map(self._parse_value, values),
            })
            return item
        else:
            self.log("No results found %r" % response, level=log.ERROR)

    def _parse_date(self, date_str):
        dt = datetime.strptime(date_str, self.date_format).date()
        return dt.isoformat()

    def _parse_value(self, value_str):
        # TODO: this is specific for vangard. Better to have a generic value
        # parser to handle other sites possible formats.
        return float(value_str.replace('$', ''))

    def _get_dates_period(self):
        if not self.date_start:
            # default start: first day of this year
            self.date_start = date.today().replace(month=1, day=1) \
                                  .strftime(self.date_format)
        if not self.date_end:
            # default end: today
            self.date_end = date.today().strftime(self.date_format)

        return self.date_start, self.date_end
    

class VanguardFundsSpider(BaseSpider):
    name = 'vanguard_funds'
    allowed_domains = ['vanguard.com']
    start_urls = ['https://personal.vanguard.com/us/funds/etf/all']

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        rows = hxs.select(
            '//tbody[@id="tboxForm:upperTB:perfTB:avgAnnTBLtbody0"]'
            '/tr[count(./th)=0]'
        )
        for row in rows:
            fl = FundLoader(response=response, selector=row)
            fl.add_xpath('id', './td[2]/a/@href', re=r'FundId=(\d+)')
            fl.add_xpath('name', './td[2]/a/text()')
            fl.add_xpath('symbol', './td[3]/text()')
            yield fl.load_item()

            

class WisdomtreeSpider(BaseSpider):
    """WisdomTree.com ETF data scraper.

    Arguments
    ---------
    fund_id : str
        Fund's ID in WisdomTree website.
    """
    name = "wisdomtree"
    allowed_domains = ["wisdomtree.com"]

    fund_id = None

    date_format = '%m/%d/%Y'
    history_url = 'http://www.wisdomtree.com/etfs/nav-history.aspx?etfid=%(fund_id)s'

    def start_requests(self):
        if self.fund_id:
            for fund_id in self.fund_id.split(','):
                # The site does not support query by start/end date as already
                # display all the history values.
                meta = {'fund_id': fund_id}
                url = self.history_url % meta
                yield Request(url, meta=meta, callback=self.parse_history)
        else:
            self.log("Argument 'fund_id' missing.", level=log.ERROR)

    def parse_history(self, response):
        fund_id = response.meta['fund_id']
        hxs = HtmlXPathSelector(response)
        results = hxs.select('//table[@title="NAV History"]'
                             '/tbody/tr/td/text()').extract()
        if results:
            results = iter(results)
            dates, values = zip(*zip(results, results))
            dates = map(self._parse_date, dates)
            values = map(float, values)
            # values are extracted in a reversed chronological order
            dates.reverse()
            values.reverse()

            return NavItem(fund_id=fund_id, dates=dates, values=values)
        else:
            self.log("No results found %r" % response, level=log.ERROR)

    # TODO: refactor this method
    def _parse_date(self, date_str):
        dt = datetime.strptime(date_str, self.date_format).date()
        return dt.isoformat()
    
    
import json

class WisdomtreeFundsSpider(BaseSpider):
    name = 'wisdomtree_funds'
    allowed_domains = ['wisdomtree.com']
    start_urls = ['http://www.wisdomtree.com/etfs/']

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        try:
            jsdata = hxs.select('//script[contains(.,"var results")]') \
                        .re('var results = (.+?);')[0]
        except IndexError:
            self.log('JS funds data not found %r' % response, level=log.ERROR)
            return

        try:
            data = json.loads(jsdata)['data']
        except ValueError:
            self.log('Could not load JS data %r' % jsdata, level=log.ERROR)
        except KeyError:
            self.log('Could not find data field %r' % jsdata, level=log.ERROR)

        for etf in data:
            yield FundItem(
                id=etf['ETFID'],
                name=etf['fund'],
                symbol=etf['ticker'],
            )