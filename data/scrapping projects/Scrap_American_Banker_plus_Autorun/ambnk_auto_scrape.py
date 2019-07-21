import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from apscheduler.schedulers.blocking import BlockingScheduler
import psycopg2
import os
from crochet import setup
setup()

class american_banker_Scraper_Autorun(object):
    def __init__(self):
        self.hostname = 'localhost'
        self.port = '5432'
        self.user = 'scraper'
        self.password = '' 
        self.database = 'news_articles'
        self.connection = psycopg2.connect(host= self.hostname, port= self.port, user= self.user, password= self.password,  dbname= self.database)
        self.cur = self.connection.cursor()
        self.process = CrawlerProcess(get_project_settings())


    def Automator(self):
        scheduler = BlockingScheduler()
        scheduler.add_job(american_banker_Scraper_Autorun().american_banker_daily_scrape, 'interval', hours=1)
        scheduler.add_job(american_banker_Scraper_Autorun().american_banker_weekly_update, 'interval', hours= 168)
        scheduler.add_job(american_banker_Scraper_Autorun().american_banker_permanent_append, 'interval', hours=730)
        scheduler.start()


    def american_banker_daily_scrape(self):
        self.cur.execute("DELETE FROM american_banker_daily a WHERE a.ctid <> (SELECT min(b.ctid) FROM american_banker_daily b WHERE a.date = b.date AND a.url = b.url AND a.title = b.title)")
        self.connection.commit()
        self.process.crawl('american_banker_main')
        try:
            reactor.run()
        except:
            pass
        self.process.start(stop_after_crawl=False)


    def american_banker_weekly_update(self):
        # COPY ALL CURRENT DAILY SCRAPED
        self.cur.execute("INSERT INTO american_banker_weekly (SELECT * FROM american_banker_weekly)")
        self.connection.commit()
        # delete the duplicates from american_banker_weekly
        self.cur.execute("DELETE FROM american_banker_weekly a WHERE a.ctid <> (SELECT min(b.ctid) FROM american_banker_weekly b WHERE a.date = b.date AND a.url = b.url AND a.title = b.title)")
        self.connection.commit()
        # DELETE ALL in american_banker_daily
        self.cur.execute("DELETE FROM american_banker_weekly")
        self.connection.commit()

    def american_banker_permanent_append(self):
        # There should not be any duplicates that are appended, thus the main archives will not be checked
        self.cur.execute("INSERT INTO american_banker_main (SELECT * FROM american_banker_weekly)")
        self.connection.commit()
        self.cur.execute("DELETE FROM american_banker_weekly")
        self.connection.commit()


american_banker_Scraper_Autorun().Automator()

