import scrapy


class CnbcItem(scrapy.Item):
    author = scrapy.Field()
    dates = scrapy.Field()
    title = scrapy.Field()
    paragraphs = scrapy.Field()
    subject = scrapy.Field()
    capitions = scrapy.Field()
    snippets = scrapy.Field()
    url = scrapy.Field()
    site_location = scrapy.Field()
    
