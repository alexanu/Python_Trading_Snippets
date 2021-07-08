import scrapy 
import re
from American_Banker.items import AmericanBankerItem

class American_Banker_Main(scrapy.Spider):
    name = 'american_banker_main'

    start_urls = [
        'https://www.americanbanker.com',
        'https://www.americanbanker.com/technology',
        'https://www.americanbanker.com/retail-and-commercial-banking',
        'https://www.americanbanker.com/community-banking',
        'https://www.americanbanker.com/cap-markets',
        'https://www.americanbanker.com/bankthink'
    ]

    allowed_domain = [
        'americanbanker.com'
    ]

    def parse(self, response):
        

        for i in response.css('body').css('a::attr(href)').extract():
            if '/news/' in i:
                yield response.follow(url= i, callback=self.article_parse)
            if '/opinion/' in i:
                yield response.follow(url= i, callback=self.article_parse)
            if '/research-report/' in i:
                yield response.follow(url= i, callback=self.article_parse)

    def article_parse(self, response):
        title = response.css('h1.bsp-page-title::text').extract()
        
        author_area = response.css('ul.bsp-tag-list').css('a')
        author = []
        if len(author_area) > 1:
            for i in author_area:
                string_i = str(i)
                if '/author/' in string_i:
                    author.append((i.css('::text').extract()))
        date = ''
        keywords = []
        other_information_area = response.css('ul.bsp-tag-list').css('li')
        date_match = re.compile('\w{3,9}\s\d{1,2}\s\d{4}\,\s\d{1,2}\:\d{1,2}\w{2}\s\w{3}')
        for i in  other_information_area:
            if date_match.findall(str(i)):
                date = i.css('::text').extract()
            if '/tag/' in str(i):
                keywords.append(i.css('::text').extract())
        # two dimensional arraay  [['Fintech regulations']]   [['Community banking'], ['De novo banks'], ['Commercial servicers']
        
        # assume for now that there will be no links in the paragraphs, cannot see full articles unless buying
        paragraphs = response.css('p::text').extract()


        news = AmericanBankerItem(author=author, title=title, date=date, keywords=keywords, paragraphs=paragraphs, url=response.request.url )

        yield news