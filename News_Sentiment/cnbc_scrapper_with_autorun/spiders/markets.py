import scrapy
import re
from cnbc.items import CnbcItem

class cnbc_markets_scraper(scrapy.Spider):
    # I guess I could've done it with the home page, but oh well rearrange another time
    name = 'cnbc_2'
    # markets page and business news
    
    start_urls=[
        'https://www.cnbc.com/markets/',
        'https://www.cnbc.com/business/',
        'https://www.cnbc.com/economy/',
        'https://www.cnbc.com/finanace/',
        'https://www.cnbc.com/health/',
        'https://www.cnbc.com/real-estate/',
        'https://www.cnbc.com/energy/',
        'https://www.cnbc.com/transportation/',
        'https://www.cnbc.com/industrials/',
        'https://www.cnbc.com/retail/',
        'https://www.cnbc.com/wealth/',
        'https://www.cnbc.com/business/',
        'https://www.cnbc.com/small-business/',
        'https://www.cnbc.com/investing/',
        'https://www.cnbc.com/business/',
        'https://www.cnbc.com/personal-finance/',
        'https://www.cnbc.com/careers/',
        'https://www.cnbc.com/college/',
        'https://www.cnbc.com/debt/',
        'https://www.cnbc.com/tax-planning/',
        'https://www.cnbc.com/savings/',
        'https://www.cnbc.com/financial-advisors/',
        'https://www.cnbc.com/advisor-council/',
        'https://www.cnbc.com/advisor-insight/',
        'https://www.cnbc.com/fa-playbook/',
        'https://www.cnbc.com/fixed-income-strategies/',
        'https://www.cnbc.com/straight-talk/',
        'https://www.cnbc.com/earnings/',
        'https://www.cnbc.com/futures-now/',
        'https://www.cnbc.com/options-action/',
        'https://www.cnbc.com/etf-street/',
        'https://www.cnbc.com/technology/',
        'https://www.cnbc.com/cybersecurity/',
        'https://www.cnbc.com/enterprise/',
        'https://www.cnbc.com/social-media/',
        'https://www.cnbc.com/mobile/',
        'https://www.cnbc.com/tech-guide/',
        'https://www.cnbc.com/internet/',
        'https://www.cnbc.com/venture-capital/',
        'https://www.cnbc.com/insurance/',
        'https://www.cnbc.com/hedge-funds/',
        'https://www.cnbc.com/deals-and-ipos/',
        'https://www.cnbc.com/banks/',
        'https://www.cnbc.com/wall-street/',
        'https://www.cnbc.com/politics/',
        'https://www.cnbc.com/white-house/',
        'https://www.cnbc.com/elections/',
        'https://www.cnbc.com/congress/',
        'https://www.cnbc.com/law/',
        'https://www.cnbc.com/taxes/',
        'https://www.cnbc.com/trading-nation/'
    ]

    allowed_domains = ['cnbc.com']
    

    def parse(self, response):
        date_regex = re.compile('/\d{4}/\d{2}/\d{2}/')
        
        for i in self.start_urls:
            if i == response.request.url:
                for i in response.xpath('//div[@id="cnbc-contents"]').css('a::attr(href)').extract():
                    if date_regex.match(i):
                        yield response.follow(url=i, callback=self.article_parse)

        if 'https://www.cnbc.com/markets/' == response.request.url:
            # there is only small portions of page wanted, reason for tight search
            for i in response.xpath('//div[@id="featuredNews_0"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)
            for i in response.xpath('//div[@id="featuredNews_1"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)
            for i in response.xpath('//div[@id="featuredNews_2"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)
            for i in response.xpath('//div[@id="default_0"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)
            for i in response.xpath('//div[@id="default_1"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)
            for i in response.xpath('//div[@id="default_2"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)
            for i in response.xpath('//div[@id="default_3"]').css('a::attr(href)').extract():
                if date_regex.match(i):
                    yield response.follow(url=i, callback=self.article_parse)

    def article_parse(self, response):
        # Drop Register Pages
        register_check =  response.xpath('//div[@id="nav0"]').css('a::attr(href)').extract()
        if register_check == '#register':
            print('Dropping: Register Page')
            return
        
        subject = response.css('a.header_title::text').extract()
        title = response.css('h1.title::text').extract()
        # Author 
        author = response.css('div.source').css('a::text').extract()
        if len(author) == 0:
            author = response.css('div.source').css('span::text').extract()
        # Some articles don't have authors
        if len(author) == 0:
            author = ['None']

        snippets = response.css('div.group').css('ul').css('li::text').extract()
        dates = response.css('time.datestamp::attr(datetime)').extract()
        capitions = response.css('div.caption::text').extract()
 

        # regex checks (went with different approach)
        # treasury_bond_check = re.compile('\w{2,3}\s\d{1,2}\-\w{2}')
        # time_1 = re.compile('\d{1,2}\s(?:Hours)\s(?:Ago)\s\|\s{2}\d{2}\:\d{2}')
        # time_2 = re.compile('\s{0,1}\d{1,2}\:\d{2}\s{2}\w{2}\s\w{2}\s\w{3}\,\s{1,2}\d{1,2}\s\w{3,5}\s\d{4}\s\|\s{2}\d{2}\:\d{2}')

        compare_para = response.xpath('//div[@id="article_body"]').css('p::text').extract()
        compare_tags = response.xpath('//div[@id="article_body"]').css('a::text').extract()
        paragraphs = []
       
        for i in response.xpath('//div[@id="article_body"]/node()[not(self::div[@id="wildcard"] or self::iframe)]').css('::text').extract():
            for key, para in enumerate(compare_para):
                if i == para:
                    paragraphs.append(i)
                    break
                if key == (len(compare_para) - 1):
                    for key_2, link in enumerate(compare_tags):
                        if i == link:
                            if '\t' in i or '\n' in i:
                                break
                            else:
                                paragraphs.append(i)
                        if (len(compare_tags) - 1) == key_2:
                            break
        
        article = CnbcItem(
            author= author,
            dates= dates,
            title= title,
            paragraphs=paragraphs,
            subject=subject,
            capitions=capitions,
            snippets=snippets,
            url= response.request.url
            )
        yield article




