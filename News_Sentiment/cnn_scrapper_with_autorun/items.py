# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
from scrapy.item import Field, Item
import scrapy


class CnnItem(scrapy.Item):
    author = scrapy.Field()
    paragraphs = scrapy.Field()
    subject = scrapy.Field()
    title = scrapy.Field()
    uuid = scrapy.Field()
    cite = scrapy.Field()
    published_date = scrapy.Field()
    section = scrapy.Field()
    url = scrapy.Field()

class CnnMoneyItem(scrapy.Item):
    author = scrapy.Field()
    paragraphs =scrapy.Field()
    subject = scrapy.Field()
    secondary_subject = scrapy.Field()
    title = scrapy.Field()
    uuid = scrapy.Field()
    cite = scrapy.Field()
    published_date = scrapy.Field()
    updated_date = scrapy.Field()
    url = scrapy.Field()
    section = scrapy.Field()
    source = scrapy.Field()
    keywords_in_paragraphs = scrapy.Field()
    
