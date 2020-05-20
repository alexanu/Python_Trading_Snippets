# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import psycopg2
import logging
from dateutil.parser import parse
from scrapy.exceptions import DropItem
import pytz, datetime

# Works with: Homepage, Money, but not tech yet

class CnnPipeline(object):
    def open_spider(self, spider):
        # hostname = 'localhost'
        # port = '5432'
        # username = 'tlesick'
        # password = ''
        # # database = 'news_scrape'
        # # need to make another table
        # self.connection = psycopg2.connect(host= hostname, port=port, user=username, password=password,  dbname=database)
        # self.cur = self.connection.cursor()
        self.ids_seen = set()

    # def close_spider(self, spider):
        # self.cur.close()
        # self.connection.close()

# HOMEPAGE 
    def process_item(self, item, spider):
        # ======================================================================
        # UNQUINESS CHECK: URL
        if item['url'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['url'])
        # ======================================================================
        # PARAGRAPH CLEANING
        #   combines the first paragraph with the second paragraph list
        if 'Homepage' in item['section'] and len(item['paragraphs']) == 2:
            new_list = item['paragraphs'][1]
            new_list = [item['paragraphs'][0]] + new_list
            item['paragraphs'] = new_list
                    
        # SUBJECT
        # subject sometimes comes through as ['/entertainment']
        if item['subject'][0][0] == '/':
                item['subject'] = item['subject'][0][1:]
        # ======================================================================
        # CLEANING AUTHORS
        if 'By' in item['author'][0]:
            item['author'] = item['author'][0].replace('By ', '')
        # CNN only shows up when 'By' is in the author as well
        if 'CNN' in item['author']:
            item['author'] = str(item['author'].replace(', CNN', ''))
        # Format backinto list
        if isinstance(item['author'], str):
            new_list = []
            new_list.append(item['author'])
            item['author'] = new_list
        
       
        # ======================================================================
        # DATE CLEANING - Update
            # CONVERT TIME TO UTC
        local = pytz.timezone ('America/New_York')
        
        if item['section'] == 'Homepage':
            published_date = item['published_date'][0].replace('Updated', '')
            
        else:
            # Published Date
            published_date = item['published_date'].replace('First published', '')
            # Updated Date
            # Sometimes the updated_date is null
            if len(item['updated_date']) != 1:
                updated_date = item['updated_date'].replace('ET','')
                updated_date = updated_date.replace(': ', ' ')
                updated_date = parse(updated_date)
                local_dt_update = local.localize(updated_date, is_dst=None)
                updated_date = local_dt_update.astimezone(pytz.utc)
                item['updated_date'] = updated_date
            else:
                item['updated_date'] = 'Null'

        published_date = published_date.replace(': ', ' ')
        published_date = published_date.replace('ET','')
        date = parse(published_date)

        # published
        local_dt = local.localize(date, is_dst=None)
        #  convert the time in to UTC
        utc_dt = local_dt.astimezone(pytz.utc)
        item['published_date'] = utc_dt
        

        # CHECK
        # print('Cleansed:')
        # print('title -----------------------------------------------------------------------------------------')        
        # print(item['title'])
        # print('author -----------------------------------------------------------------------------------------')   
        # print(item['author'])
        # print('published date -----------------------------------------------------------------------------------------')
        # print(item['published_date'])
        # print('paragraphs -----------------------------------------------------------------------------------------')
        # print(item['paragraphs'])
        # print('subject -----------------------------------------------------------------------------------------')
        # print(item['subject'])
        # if 'Tech' in item['section'] or 'Money' in item['section']:
        #     print('key words in paragraphs -----------------------------------------------------------------------------------------')
        #     print(item['keywords_in_paragraphs'])
        #     print('Updated Date -----------------------------------------------------------------------------------------')
        #     print(item['updated_date'])
        #     print('Source -----------------------------------------------------------------------------------------')
        #     print(item['source'])

        return


    