import scrapy
from bs4 import BeautifulSoup
from cnn.items import CnnItem
import logging
import json
import uuid
from cnn.items import CnnMoneyItem
# CURRENT ISSUE hyperlinks in paragraph are not grabbed
class MoneyTechScraper(scrapy.Spider):
    name = 'cnn_tech'

    start_urls = ['http://money.cnn.com/technology',
                    'https://money.cnn.com/technology/culture/',
                    'https://money.cnn.com/technology/business/',
                    'https://money.cnn.com/technology/gadgets/',
                    'https://money.cnn.com/technology/future/',
                    'https://money.cnn.com/technology/startups/',
                    ]

    allowed_domains = ['money.cnn.com']

    def parse(self, response):
        for i in response.css('a._2HBM5::attr(href)').extract():
            yield response.follow(url=i, callback= self.article_parse)

    def article_parse(self, response):
         # AUTHOR 
        author = response.css('span.byline').css('a::text').extract()
        if len(author) == 0:
            author = response.css('span.byline::text').extract()
        # TITLE
        title = response.css('h1.article-title::text').extract()
        # URL
        url = response.request.url
        # CITE
        cite = 'CNN'
        # SUBJECT
        possible_subjects = {'Personal Finance': '/pf/',
                    'Markets': '/markets/',
                    'Investing': '/investing/',
                    'News': '/news/',
                    'Small Business':'/smallbusiness/',
                    'Technology': '/technology/',
                    'Media': '/media/',
                    'Auto': '/autos/'}
        for index, info in possible_subjects.items():
            if info in url: 
                subject = index
                break

        # NEWS HAS DIFFERENT SECONDARY SUBJECTS 
        count, beg, end = 0, 0 , 0
        if  subject == 'News':
            for location, letter in enumerate(url):
                if letter == '/':
                    count += 1
                    if count == 7:
                        beg = location
                    if count == 8:
                        end = location
                        break
        secondary_subject = url[beg:end].replace('/', '')

        if len(secondary_subject) == 0:
            secondary_subject = 'None'
        
        # PARAGRAPH
        paragraph=[]
        outter_only_text = response.xpath('//div[@id="storytext"]').css('::text').extract()
        for index,info in enumerate(outter_only_text):
            if index >= 18 and (len(outter_only_text) - 10) >= index:
                if info == ' ' or info == '(' or info == ')':
                    continue
                elif '\r' in info or '\n' in info or '===' in info or 'vidConfig.' in info:
                    continue
                else:
                    paragraph.append(info)


        # SPAN INNER TEXT OF COMPANY AND CEO names not grabbed
        source, published_date, updated_date = '','',''
        key_words_paragraphs = []
        for index, info in enumerate(response.css('span::text').extract()):
            if 'AM ET' in info or 'PM ET' in info:
                if 'First published' in info:
                    published_date = info
                if  'First published' not in info:
                    updated_date = info    
                continue
            if index > 9:
                if len(info) > 2 and 'CNNMoney' not in info and 'First published' not in info and '\n\t\t\t' not in info and 'Sign up' not in info:
                    key_words_paragraphs.append(info)
                if 'CNNMoney' in info:
                    source = info
                if 'First published' in info:
                    published_date = info
                if '\n\t\t\t' in info:
                    break
            if index == 8:
                updated_date = info
        if len(key_words_paragraphs) == 0:
            key_words_paragraphs = 'Null'


        # ORGANIZER
        cnn_tech = CnnMoneyItem(
         author = author,
         paragraphs= paragraph,
         subject= subject,
         secondary_subject=secondary_subject,
         title= title,
         uuid= uuid.uuid4(),
         cite= cite,
         published_date= published_date,
         updated_date = updated_date,
         url= url,
         section= 'Tech',
         source = source,
         keywords_in_paragraphs= key_words_paragraphs
        )
        # print('title -----------------------------------------------------------------------------------------')        
        # print(title)
        # print('author -----------------------------------------------------------------------------------------')   
        # print(author)
        # print('published date -----------------------------------------------------------------------------------------')
        # print(published_date)
        # print('paragraphs -----------------------------------------------------------------------------------------')
        # print(paragraph)
        # print('subject -----------------------------------------------------------------------------------------')
        # print(subject)
        # print('key words in paragraphs -----------------------------------------------------------------------------------------')
        # print(key_words_paragraphs)
        # print('Updated Date -----------------------------------------------------------------------------------------')
        # print(updated_date)
        # print('Source -----------------------------------------------------------------------------------------')
        # print(source)
   
        yield cnn_tech
        #  in order to get the remaining cnn more content need to get through selenium
        # because it is javascript generated - skip now 