import scrapy
from scrapy.crawler import CrawlerProcess
# """ location of spiders """
from scrapy.utils.project import get_project_settings
from apscheduler.schedulers.blocking import BlockingScheduler
import psycopg2
import os
# Not sure if I remove if it still works or not because seems intermittent 
from crochet import setup
setup()

class Cnbc_Scraper_Autorun(object):
    def __init__(self):
        self.hostname = 'localhost'
        self.port = '5432'
        self.user = 'scraper'
        self.password = '' # Pass
        self.database = 'news_articles'
        self.connection = psycopg2.connect(host= self.hostname, port= self.port, user= self.user, password= self.password,  dbname= self.database)
        self.cur = self.connection.cursor()
        self.process = CrawlerProcess(get_project_settings())


    def Automator(self):
        scheduler = BlockingScheduler()
        scheduler.add_job(Cnbc_Scraper_Autorun().cnbc_daily_scrape, 'interval', hours=1)
        scheduler.add_job(Cnbc_Scraper_Autorun().cnbc_weekly_update, 'interval', hours= 168)
        scheduler.add_job(Cnbc_Scraper_Autorun().Cnbc_permanent_append, 'interval', hours=730)
        scheduler.start()


    def cnbc_daily_scrape(self):
        self.cur.execute("DELETE FROM cnbc_daily a WHERE a.ctid <> (SELECT min(b.ctid) FROM cnbc_daily b WHERE a.published_date = b.published_date AND a.url = b.url AND a.title = b.title)")
        self.connection.commit()
        # Crawl the Everything(even though it says homepage)
        self.process.crawl('cnbc_homepage')
        self.process.crawl('cnbc_2')
        try:
            reactor.run()
        except:
            pass
        self.process.start(stop_after_crawl=False)
        # prevents the crawler from stopping the twisted reactor


    def cnbc_weekly_update(self):
        # COPY ALL CURRENT DAILY SCRAPED
        self.cur.execute("INSERT INTO cnbc_weekly (SELECT * FROM cnbc_weekly)")
        self.connection.commit()
        # delete the duplicates from cnbc_weekly
        self.cur.execute("DELETE FROM cnbc_weekly a WHERE a.ctid <> (SELECT min(b.ctid) FROM cnbc_weekly b WHERE a.published_date = b.published_date AND a.url = b.url AND a.title = b.title)")
        self.connection.commit()
        # DELETE ALL in cnbc_daily
        self.cur.execute("DELETE FROM cnbc_weekly")
        self.connection.commit()

    def Cnbc_permanent_append(self):
        # There should not be any duplicates that are appended, thus the main archives will not be checked
        self.cur.execute("INSERT INTO cnbc_main (SELECT * FROM cnbc_weekly)")
        self.connection.commit()
        self.cur.execute("DELETE FROM cnbc_weekly")
        self.connection.commit()


Cnbc_Scraper_Autorun().Automator()