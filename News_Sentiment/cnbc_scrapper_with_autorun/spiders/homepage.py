import scrapy
import re
from cnbc.items import CnbcItem

class Cnbc_Scraper(scrapy.Spider):

    name = 'cnbc_homepage'

    start_urls=[
        'https://www.cnbc.com',
    ]

    allowed_domains = ['cnbc.com']

    def parse(self, response):
        
        date_regex = re.compile('/\d{4}/\d{2}/\d{2}/')
        if 'https://www.cnbc.com' == response.request.url:
            for i in response.css('div.cnbc-contents').css('a::attr(href)').extract():
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
            author = 'None'

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
            url= response.request.url,
            site_location= 'Homepage'
            )
        yield article
        








        