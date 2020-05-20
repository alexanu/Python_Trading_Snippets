import psycopg2
from dateutil.parser import parse
from scrapy.exceptions import DropItem
import pytz, datetime



class ReutersPipeline(object):
    def open_spider(self, spider):
        hostname='localhost'
        port = '5432'
        username='scraper'
        password='' #< look 
        database= 'news_articles' #server postgres db
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

        # CLEAN THE LOCATION
        # =====================================================================

        # There  types either location w/reuters, no location w/ reuters, breakingviews w/ location, or location no reuters
        
        if 'Breakingviews' not in item['location'][0] and '(Reuters)' in item['location'][0]:
            item['location'] = item['location'][0][:item['location'][0].find('(Reuters)')]
            
        elif 'Breakingviews' in item['location'][0]:
            item['location'] = item['location'][0][:item['location'][0].find('(Reuters Breakingviews)')]
            
        elif '(Reuters)' not in item['location'][0]:
            item['location'] = item['location'][0][:item['location'][0].find('-')]
        
        
        # CLEAN THE DATE
        # =====================================================================
        # I believe this is the posting timezone, but haven't verified 
        local = pytz.timezone('America/New_York')
        
        date = item['published_date'][0][:(item['published_date'][0].rfind('/'))]
        date = str(date.replace('/', ''))
        date = parse(date)
        local_dt = local.localize(date, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        item['published_date'] = utc_dt
            
        # CLEAN THE PARAGRAPHS 
        # =====================================================================
        item['paragraphs'][0] = item['paragraphs'][0][(item['paragraphs'][0].find('(Reuters) -') + 12):]
        replace_paragraph = []
        for  value in item['paragraphs']:
            # Removes this goddamn block element '▒'
            value = value.encode('utf-8').decode('unicode_escape').encode('ascii', 'ignore').decode('utf-8')
            replace_paragraph.append(value)
        item['paragraphs'] = replace_paragraph
        # Not all Articles Have Authors on Reuters
        if len(item['author']) == 0:
            item['author'] = ['None']

        #  DATABASE INSERT
        # =====================================================================
        # print('author type ' + str(type(item['author'])))
        # print('paragraphs type ' + str(type(item['paragraphs'])))
        # print('subject type ' + str(type(item['subject'])))
        # print('title type ' + str(type(item['title'])))
        # print('url type ' + str(type(item['url'])))
        # print('locaiton type ' + str(type(item['location'])))
        # print('additional_authors type ' + str(type(item['additional_authors'])))
        # print('date type ' + str(type(item['published_date'])))

        self.cur.execute("INSERT INTO reuters_daily (author, paragraphs, published_date, subject, location, additional_authors, url, title) VALUES( %s, %s, %s, %s, %s, %s, %s, %s)",(item['author'], item['paragraphs'], item['published_date'], item['subject'], item['location'], item['additional_authors'], item['url'], item['title']))
        self.connection.commit()
        return item

