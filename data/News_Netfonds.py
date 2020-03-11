# Source: https://github.com/ntftrader/ntfdl/blob/master/ntfdl/news.py


import requests as request
from bs4 import BeautifulSoup as bs
import re
from pprint import pprint
import sys
from datetime import datetime
import time
import json


class News(object):
    def __init__(self, instrument, exchange='OSE'):

        self.instrument = instrument
        self.exchange = exchange

    def _get_newsweb(self, url):

        news_html = request.get(url)
        s = bs(news_html.text, "lxml")

        l = s.find('div', {'class': 'box_m'}).find_all('tr')

        links = []
        for url in s.find('div', {'class': 'box_m'}).find('table').find_all('a', href=True):
            if url:
                if url.has_attr('href'):
                    if not url.attrs['href'].startswith('javascript'):
                        links.append("http://www.newsweb.no%s" % url.attrs['href'])

        content = l[len(l) - 1].find('pre').get_text()
        # found_pdf_urls = [link["href"] for link in l.find_all("a", href=True) if link["href"].endswith(".pdf")]

        return content, links

    def get_news(self, date='today', days=365):

        if date == "today":
            date = datetime.now().strftime('%Y%m%d')
        elif isinstance(date, datetime):
            date = date.strftime('%Y%m%d')

        html = request.get('http://www.netfonds.no/quotes/releases.php?date=%s&days=%i&paper=%s&exchange=%s&search=' % (
        date, days, self.instrument, self.exchange))

        s = bs(html.text, "lxml")

        table = s.find(id='updatetable1').find_all('tr')

        all_news = []
        for row in table:
            i = 0
            n = {}
            for cell in row.find_all('td'):
                """First row, first col is date
                Second, third.. col is empty
                Only use col 2 and 4. 4 is two links."""

                if i == 0 and cell.attrs.get('class') and 'nowrap' in cell.attrs.get('class'):
                    cur_date = datetime.strptime(cell.get_text(), '%d/%m-%Y').date()
                elif i == 1:
                    n['time'] = datetime.strptime('%s %s' % (cur_date, cell.get_text()), '%Y-%m-%d %H:%M:%S')

                elif i == 3:
                    n['title'] = re.sub(r'\([^)]*\)', '', cell.get_text())

                    r = re.findall(r'\((.*?)\)', cell.get_text())
                    n['provider'] = r[len(r) - 1]

                    url = cell.find('a', href=True)
                    if url:
                        if url.has_attr('href'):
                            news_html = request.get("http://www.netfonds.no/quotes/%s" % url.attrs['href'])
                            ext = bs(news_html.text, "lxml")
                            news = ext.find('div', class_='hcontent').find_all('p')[1].find('a', href=True)
                            if not news:
                                news = ext.find('div', class_='hcontent').find_all('p')[0].find('a', href=True)
                            if news and news.has_attr('href'):
                                n['url'] = news.attrs['href']
                                # @TODO should download from site by provider and get text with news.
                                if n['provider'] == 'OBI':
                                    txt, links = self._get_newsweb(n['url'])
                                    n['content'] = {'text': txt, 'links': links}
                            else:
                                n['url'] = None

                i = i + 1

            if len(n) > 0:
                all_news.append(n)

        return all_news
