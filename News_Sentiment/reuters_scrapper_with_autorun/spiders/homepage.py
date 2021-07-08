import scrapy
from reuters.items import ReutersItem
import logging

class HomepageSpider(scrapy.Spider):
    name = 'reuters_homepage'

    # need to check if any of the pages or articles are empty
    start_urls = [
        'https://www.reuters.com',
        # business
        'https://www.reuters.com/finance',
        'https://www.reuters.com/legal',
        'https://www.reuters.com/finance/deals',
        'https://www.reuters.com/subjects/aerospace-and-defense',
        'https://www.reuters.com/data-dive',
        'https://www.reuters.com/subjects/banks',
        'https://www.reuters.com/subjects/autos',
        'https://www.reuters.com/finance/summits',
        'https://www.reuters.com/news/subjects/ADventures',
        # markets
        'https://www.reuters.com/finance/markets',
        'https://www.reuters.com/finance/markets/us',
        'https://www.reuters.com/finance/markets/europe',
        'https://www.reuters.com/finance/markets/asia',
        'https://www.reuters.com/finance/commodities',  
        'https://www.reuters.com/finance/funds',
        'https://www.reuters.com/finance/EarningsUS',
        'https://www.reuters.com/finance/markets/dividends',
        'https://www.reuters.com/finance/currencies',
        'https://www.reuters.com/finance/exchange-traded-funds',
        'https://www.reuters.com/subjects/us-lipper-awards',
        # World 
        'https://www.reuters.com/news/world',
        'https://www.reuters.com/news/us',
        'https://www.reuters.com/subjects/specialReports',
        'https://www.reuters.com/investigates/',
        'https://www.reuters.com/places/mexico',
        'https://www.reuters.com/places/brazil',
        'https://www.reuters.com/places/africa',
        'https://www.reuters.com/places/russia',
        'https://www.reuters.com/subjects/euro-zone',
        'https://www.reuters.com/subjects/middle-east',
        'https://www.reuters.com/places/china',
        'https://www.reuters.com/places/japan',
        'https://www.reuters.com/places/india',
        # politics
        'https://www.reuters.com/politics',
        'https://www.reuters.com/subjects/supreme-court',
        # Technology
        'https://www.reuters.com/news/technology',
        'https://www.reuters.com/news/science',
        'https://www.reuters.com/news/media',
        'https://www.reuters.com/energy-environment',
        'https://www.reuters.com/innovation',
        # Commentary
        'https://www.reuters.com/commentary',
        # Breakingviews
        'https://www.reuters.com/breakingviews',
        # wealth
        'https://www.reuters.com/finance/wealth',
        'https://www.reuters.com/finance/personal-finance/retirement',
        # Lifestyle
        'https://www.reuters.com/news/lifestyle',
        'https://www.reuters.com/news/sports/soccer',
        'https://www.reuters.com/news/sports',
        'https://www.reuters.com/news/entertainment/arts',
        'https://www.reuters.com/news/entertainment',
        'https://www.reuters.com/news/oddlyEnough'
    ]
    # need to check pages when get back
    allowed_domains = ['reuters.com']

    def parse(self, response):
        for  value in response.xpath('//div[@id="content"]').css('a::attr(href)').extract():
            
            if '/theWire' in value:
                print('DROPPING: The Wire')
                continue
            if '/video/' in value:
                print('DROPPING: Video')
                continue
            if 'https://www.reuters.com/investigates/special-report/tips/' in value or 'http://newslink.reuters.com/join/subscribe' in value:
                print('DROPPING: Wrong Link')
                continue
            if len(value) <= 16: 
                print('DROPPING: Link to Sub-page')
                continue
            if '/picture/' in value or '/news/pictures' in value:
                print("DROPPING: Picture")
                continue
            
            yield response.follow(url=str(value), callback=self.article_parse)

    def article_parse(self, response):
        paragraphs, location, additional_authors = [], [], []
        
        subject = response.css('div.ArticleHeader_channel').css('a::text').extract()
        published_date = response.css('div.ArticleHeader_date::text').extract()
        if len(published_date) == 0:
            published_date = response.css('div.HeroArticleHeader_date::text').extract()
        # clean date ['September 11, 2018 /  7:12 AM / Updated 18 minutes ago']
        title = response.css('h1.ArticleHeader_headline::text').extract()
        author = response.css('div.BylineBar_byline').css('a::text').extract()
        paragraph_location  = response.css('div.StandardArticleBody_body').css('p::text').extract()
        for key, value in enumerate(paragraph_location):
            if key == 0:
                location.append(value)
            if key == (len(paragraph_location) - 1):
                additional_authors.append(value)
                # maybe leave this for now, unsure if the additional authors makes a difference
            if key != (len(paragraph_location) -1):
                paragraphs.append(value)
        
        if len(paragraphs) == 0:
            print("DROPPING: No Information")
            return

        article = ReutersItem(
            url = response.request.url,
            author = author,
            subject = subject,
            published_date = published_date,
            title = title,
            paragraphs = paragraphs,
            location = location,
            additional_authors = additional_authors
        )
        yield article
