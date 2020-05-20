# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ReutersItem(scrapy.Item):
    subject = scrapy.Field()
    author = scrapy.Field()
    published_date = scrapy.Field()
    title = scrapy.Field()
    location = scrapy.Field()
    paragraphs = scrapy.Field()
    additional_authors = scrapy.Field()
    url = scrapy.Field()

   