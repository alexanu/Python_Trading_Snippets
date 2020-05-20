import psycopg2
from dateutil.parser import parse
from scrapy.exceptions import DropItem
import pytz, datetime

class CnbcPipeline(object):
    def open_spider(self, spider):
        hostname='localhost'
        port = '5432'
        username='scraper'
        password='' #< look 
        database= 'news_articles' 
        self.connection = psycopg2.connect(host=hostname, port=port, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()
        self.ids_seen = set()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()


    def process_item(self, item, spider):
        # UNQUINESS CHECK: URL
        # =====================================================================
        if item['url'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['url'])

        # CLEAN THE DATE
        # =====================================================================
        
        published_date, updated_date = None, None
        
        if len(item['dates']) == 2:
            published_date, updated_date =item['dates'][0], item['dates'][1]
            updated_date = datetime.datetime.strptime(updated_date, '%Y-%m-%dT%H:%M:%S%z')
            updated_date = updated_date.astimezone(pytz.utc)  
        else:
            published_date = item['dates'][0]
        published_date = datetime.datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%S%z')
        published_date = published_date.astimezone(pytz.utc)
        

        # CLEAN THE PARAGRAPHS 
        # =====================================================================
        if len(item['paragraphs']) == 0:
            raise DropItem('Dropping: No Paragraph')


        replace_paragraph = []
        for  value in item['paragraphs']:
            value = value.encode('utf-8').decode('unicode_escape').encode('ascii', 'ignore').decode('utf-8')
            replace_paragraph.append(value)
        item['paragraphs'] = replace_paragraph

        # Titles Clean
        # =====================================================================
        if '\n'  in item['title']:
            item['title'].replace('\n', '')
        


        if len(item['paragraphs']) == 0 and len(item['snippets'] <= 10):
            raise DropItem("Duplicate item found: %s" % item)

        # print('author' + str(type(item['author'])))
        # print('paragraphs' + str(type(item['paragraphs'])))
        # print('title' + str(type(item['title'])))   
        # print('published_date' + str(type(updated_date)))
        # print('updated_date' + str(type(published_date)))
        # print('subject' + str(type(item['subject'])))
        # print('snippets' + str(type(item['snippets'])))
        # print('capitions' + str(type(item['capitions'])))
        # print('url' + str(type(item['url'])))

        self.cur.execute("INSERT INTO cnbc_daily (authors, paragraphs, published_date, updated_date, subject, url, title, snippets, capitions) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s)",(item['author'], item['paragraphs'], published_date, updated_date, item['subject'], item['url'], item['title'], item['snippets'], item['capitions']))
        self.connection.commit()
        return item
        
