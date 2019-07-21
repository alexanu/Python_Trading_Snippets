import psycopg2
from dateutil.parser import parse
from scrapy.exceptions import DropItem
import pytz, datetime

class AmericanBankerPipeline(object):
    def open_spider(self, spider):
        hostname = 'localhost'
        port = '5432'
        username = 'scraper'
        password = ''
        database = 'news_articles'
        self.connection = psycopg2.connect(host= hostname, port=port, user=username, password=password,  dbname=database)
        self.cur = self.connection.cursor()
        self.ids_seen = set()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        # ======================================================================
        # UNQUINESS CHECK: URL
        if item['url'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['url'])

        # author cleanse
        author = []
        for i in item['author']:
            author.append(i[0])
        item['author'] = author
        
        # title cleanse
        if len(item['title']) == 1 : 
            title = item['title'][0]
        if len(item['title']) ==2:
            title = item['title'][1]
        if '\n' in title:
           title = title.replace("\n", '')
        if '\t' in title:
            title = title.replace("\t", '')
        item['title'] = title

        #Date cleanse
        date = str(item['date'][0])
        local = pytz.timezone('America/New_York')
        date = parse(date)
        local_dt_update =local.localize(date, is_dst= True)
        date = local_dt_update.astimezone(pytz.utc)
        item['date'] = date
        
        # Keywords Cleanse
        keywords = []
        for i in item['keywords']:
            keywords.append(i[0])
        item['keywords'] = keywords
        
        # Paragraph Cleanse
        replace_paragraph = []
        for  value in item['paragraphs']:
            value = value.encode('utf-8').decode('unicode_escape').encode('ascii', 'ignore').decode('utf-8')
            replace_paragraph.append(value)
        item['paragraphs'] = replace_paragraph
        

        # paragraphs = list
        # date = timestamp  with tz
        # title =str
        # author =list
        # url = str
        # keywords = list

        self.cur.execute("INSERT INTO american_banker_daily (author, paragraphs, date, keywords, url, title) VALUES( %s, %s, %s, %s, %s, %s)",(item['author'], item['paragraphs'], item['date'], item['keywords'], item['url'], item['title']))
        self.connection.commit()
        return item