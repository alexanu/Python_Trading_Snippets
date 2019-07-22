import scrapy
from bs4 import BeautifulSoup
from cnn.items import CnnItem
import logging
import json
import uuid
from cnn.items import CnnMoneyItem

class MoneySpider(scrapy.Spider):

    name='cnn_money'

    start_urls = ['https://money.cnn.com',
        'https://money.cnn.com/news/companies/',
                ]

    allowed_domains = ['money.cnn.com']
    # Need a seperate parser for the money.cnn/tech
    def parse(self, response):
        # A) For money homepage either select all info pieces on page
    
        # directed to will alter the parsing function

        checklist = ['/video/',
        '/services/',
        '/data/',
        '/www.facebook.com',
        '//www.instagram.com',
        '//twitter',
        '/interactive/',
        '/pf/money-essentials/',
        '/retirement/new-rules-for-retirement/',
        '/tools/',
        '/pf/loan_center/',
        '/news/boss-files/',
        '/news/fresh-money/',
        '/terms/',
        'privacy/',
        'www.youtube.com',
        'ck.lendingtree.com',
        '//cnnmoney.ch',
        '/?iid=intnledition',
        '/?iid=dmstedition',
        'https://www.cnn.com/email/subscription',
        '/surge/index.html',
        '//markets.money.cnn.com/Marketsdata/Sectors/']
        
        for url in response.css('div._55afeaf6').css('a::attr(href)').extract(): 
            # This sorting I think will miss some articles - Look at it later
            for word_check in checklist:
                if word_check in url:
                    print('Drop: Incorrect Link')
                    break
                elif word_check not in url and word_check == checklist[-1] and url.count('/') > 4:
                    # print('--------------------------------')
                    # print('selected page: ' + str(url))
                    # print('--------------------------------')
                    yield response.follow(url=url, callback= self.article_parse)


# this seems to becoming memory intensive - check
    def article_parse(self, response):
        # AUTHOR 
        author = response.css('span.byline').css('a::text').extract()
        if len(author) == 0:
            author = response.css('span.byline::text').extract()
        # TITLE
        title = response.css('h1.article-title::text').extract()
        # DATE
        published_date = response.css('span.cnnDateStamp::text').extract()
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
        source, published_date, updated_date = '','',''
        key_words_paragraphs = []

        # SPAN INNER TEXT OF COMPANY AND CEO names not grabbed
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

             
        # ORGANIZE AND SEND TO PIPELINE 
        cnn_money = CnnMoneyItem(
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
         section= 'Money',
         keywords_in_paragraphs= key_words_paragraphs,
         source = source
        )
        yield cnn_money
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
