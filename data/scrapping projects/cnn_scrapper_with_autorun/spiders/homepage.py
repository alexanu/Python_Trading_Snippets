import scrapy
from bs4 import BeautifulSoup
from cnn.items import CnnItem
import logging
import json
import uuid


class HomepageSpider(scrapy.Spider):

    name = 'cnn_homepage'

    start_urls = ['https://www.cnn.com',
                'https://www.cnn.com/us',
                'https://www.cnn.com/world',
                'https://www.cnn.com/politics',
                'https://www.cnn.com/opinions',
                'https://www.cnn.com/health',
                'https://www.cnn.com/entertainment',
                ]

    allowed_domains = ['cnn.com']
    # NEED TO MAKE CONDITIONAL FOR DETERMING PAGE

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        # line to look for
        anchor = "var CNN = CNN || {};CNN.isWebview = false;CNN.contentModel = "
        #  s is html script
        s = soup.find(lambda tag:tag.name=="script" and anchor in tag.text)
        # j locates the article list within the element
        j = s.text[s.text.find("articleList")-2:s.text.find("}]")+4]
        d = json.loads(j)
        # ======================================================================
        # targets the articleList
        for url_article in d['articleList']:
            # FIX URL PROBLEM
            # logging.info(url_article['uri'])
            
            yield response.follow(url=str(url_article['uri']), callback= self.article_parse)
        # ======================================================================

    def article_parse(self, response):
        # REMOVE GALLERIES
        if 'gallery' in response.request.url:
            print('Dropping: Gallery')
            return
        # ======================================================================
        # PARAGRAPHS
        paragraphs = response.css('p.speakable::text').extract()
        # need to get paragraphs - the first is seperate from the rest, need to search by class
        
        paragraphs.append(response.css('div.zn-body__paragraph::text').extract())   
       # ====================================================================== 
        # VIDEO CHECK
        # drop the source: it's a video
        if len(paragraphs[0]) == 0:
            print('Dropping: Video')
            return
        # cleans empty ending paragraph
        elif len(paragraphs[-1]) == 0:
            del paragraphs[-1] 
        # ======================================================================
        # SUBJECT
        subject = response.css('div.nav-section__name').css('a::text').extract()
        
        if len(subject) == 0:
            subject = response.css('a.logo-links__politics::attr(aria-label)').extract()
        if len(subject) == 0:
            subject = response.css('a.logo-entertainment--header::attr(href)').extract()
        if len(subject) == 0:
            subject = response.css('div.logo-links').css('a::attr(aria-label)').extract()
        if len(subject) == 0:
            subject = response.css('a.logo-entertainment::attr(href)').extract()
        if len(subject) == 0:
            subject = response.css('section.pg__branding').css('a::attr(href)').extract()
        # ======================================================================
        # TITLE
        title = response.css('h1.pg-headline::text').extract()
        # ======================================================================
        # AUTHOR
        author = response.css('p.metadata__byline').css('a::text').extract()
        # author sometimes not grabbed
        if len(author) == 0:
            # need to clean out CNN shows up as author
            author = response.css('p.metadata__byline').css('span.metadata__byline__author::text').extract()
        if len(author) == 0:
            if 'Opinion' in subject[0]:
            # IN FUTURE APPEND INTO ANOTHER DATABASE
            # these articles are opinions from multiple authors sequentially
                print('Opinion Sentiment Page')
                return
        # ======================================================================
        # DATE
        date = response.css('p.update-time::text').extract()
        # ======================================================================
        # CHECKS AGAINST MISSING FIELDS
        if len(subject) == 0 or len(title) == 0 or len(author) == 0 or len(paragraphs) == 0 or len(date) == 0:
            print('subject' + str(subject))
            print('url response ' + str(response.url))
            print('title ' + str(title))
            print('author ' + str(author))
            print('parag ' + str(paragraphs))
            print('date ' + str(date))
       # ======================================================================
        # Organizer
        news = CnnItem(subject=subject, title=title, author=author, paragraphs=paragraphs, published_date =date, uuid=uuid.uuid4(), cite='CNN', section='Homepage', url=response.request.url)
        
        yield news

