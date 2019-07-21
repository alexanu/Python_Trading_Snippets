# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AmericanBankerItem(scrapy.Item):
    author = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    keywords = scrapy.Field()
    paragraphs = scrapy.Field()
    url = scrapy.Field()

    