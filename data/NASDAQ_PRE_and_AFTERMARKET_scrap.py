#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import re
import requests
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup

class Settings(object):

    price_finder = re.compile('[0-9]{1,5}(\.[0-9]{2})?') # price finder regex
    volnum = re.compile('[0-9]{1,10}') # vol number only   
    cancelled = re.compile('cancelled', re.IGNORECASE) # cancelled re
    mkt_close_re = re.compile('(?<=\$)[0-9]{1,5}(\.[0-9]{2})?') # mkt close
    day = re.compile('[1-3][0-9]?') # day regex
    year = re.compile('20[0-9]{2}') # year regex

    main_link = 'http://www.nasdaq.com/symbol/{}/after-hours' # main link
    pre_main_link = 'http://www.nasdaq.com/symbol/{}/premarket'# pre market main link
    link_dict = {'after': main_link,
                 'pre': pre_main_link}
    # output formatting
    formatting = {'mktclose': 'price',
                  'highprice': 'price',
                  'lowprice': 'price',
                  'lowtime': 'date',
                  'hightime': 'date',
                  'volume': 'volume'}
    # tag constructs
    settings = {'volume': 'quotes_content_left_lblVolume',
                'highprice': 'quotes_content_left_lblHighprice',
                'lowprice': 'quotes_content_left_lblLowprice',
                'hightime': 'quotes_content_left_lblhightime',
                'lowtime': 'quotes_content_left_lbllowtime',
                'nextpage': 'quotes_content_left_lb_NextPage',
                'pricetable': 'AfterHoursPagingContents_Table',
                'nottrading': 'notTradingIPO',
                'mktclose': re.compile('market', re.IGNORECASE)}

    curdate = datetime.now()

def filter_data(data):
    return ''.join(list(filter(lambda x: ord(x) <= 128, data))) # filter ordinals from string greater than 128

def formatoutput(output, outtype='date'):
    assert outtype in ['date', 'price', 'volume'], """formatout only supports date, price, volume
                                                        \nUnsupported: {}""".format(outtype)
    if outtype == 'date':
        tme, month, day, year = output
        if tme in ['', 'N/A', None]: # if date in defined group indicating no data availalbe...
            return 'DATA NOT AVAILABLE'
        else:
            tme = filter_data(tme) # filter string for any weird characters
            return datetime.strptime('{}-{}-{} {}'.format(month, day, year, tme),
                                     '%m-%d-%Y %H:%M:%S %p')
    else:
        if output in ['', 'N/A', None]:
            return 'DATA NOT AVAILABLE'
        if outtype == 'price':
            price = filter_data(output.replace('$', ''))
            return float(price)
        if outtype == 'volume':
            volume = filter_data(output)
            try:
                return float(volume)
            except ValueError:
                return volume

def clean_df(dataframe):
    dataframe['Cancelled'] = dataframe['Volume'].apply(lambda x: True if Settings.cancelled.search(str(x)) else False)
    dataframe['Volume'] = dataframe['Volume'].apply(lambda x: int(Settings.volnum.search(str(x)).group(0)))
    return dataframe


class AfterHours(object):

    def __init__(self, ticker='appl', typeof='after'):
        # check user inserted correct typeof
        if typeof not in ['after', 'pre']:
            raise ValueError("""Must use `after` or `pre` to define after hours or pre hours trading""")
        self.typeof = typeof

        self.df = pd.DataFrame()
        self.ticker = ticker

        # assign initial beautiful soup
        self.init = BeautifulSoup(requests.get(Settings.link_dict[self.typeof].format(self.ticker)).content,
                                  'html.parser')

        # check if initial beautiful soup is even available from NASDAQ
        if self.init.find('div', {'class': Settings.settings['nottrading']}):
            raise RuntimeError('NASDAQ NOT TRADING IPO ERROR FOR TICKER: {}'.format(self.ticker))

        self.soup = self.init # assign to tmp soup
        self.get_cur_date() # retrieve market close date from Ticker page

        
    def get_cur_date(self):
        tmp = self.soup.find('small', text=re.compile('market', re.IGNORECASE)).text.split('Market')[0].strip()
        self.year = Settings.year.search(tmp).group(0)
        self.day = Settings.day.search(tmp).group(0)
        months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        for ii, mo in enumerate(months, 1): # iterate over months and flag if match found
            more = re.compile(mo, re.IGNORECASE)
            if more.search(tmp):
                self.month = ii
                break

                
    def getdata(self, datatype='lowprice'):
        datatype = datatype.lower()
        if datatype not in ['mktclose', 'highprice', 'lowprice','hightime', 'lowtime', 'volume']:
            raise ValueError("""Only supports mktclose, highprice, lowprice, hightime, lowtime
                                \nUnsupported: {}""".format(datatype))

        outputdata = getattr(self.soup.find('span', {'id': Settings.settings[datatype]}), 'text', None)
        format_out = Settings.formatting[datatype] # secure appropriate formatting type

        if datatype == 'mktclose':
            if not Settings.mkt_close_re.search(outputdata):
                raise ValueError("""Current market close price unavailable""")
            else:
                return formatoutput(Settings.mkt_close_re.search(outputdata).group(0), format_out)

        elif datatype in ['lowprice', 'highprice']:
            return formatoutput(outputdata, Settings.formatting[datatype])

        elif datatype in ['lowtime', 'hightime']:
            return formatoutput((outputdata, self.month, self.day, self.year), format_out)

        else:
            return formatoutput(outputdata, format_out)

    def secure_all_pages(self): # iterate over all pages of price information for after hours trading,

        # check if pricing table is available at all, if not return value error
        if not self.soup.find('table', {'id': Settings.settings['pricetable']}):
            raise ValueError('{} market data not available for {}'.format(self.typeof, self.ticker))

        # format data from table, and convert to pandas dataframe
        table = self.soup.find('table', {'id': Settings.settings['pricetable']}).findAll('td')
        times = [datetime.strptime('{}-{}-{} {}'.format(self.month, self.day, self.year, rec.text),
                                   '%m-%d-%Y %H:%M:%S') for rec in table[::3]]
        prices = [float(Settings.price_finder.search(rec.text).group(0)) for rec in table[1::3]]
        volumes = [rec.text.replace(',', '') for rec in table[2::3]]

        tmpdf = pd.DataFrame({'Time': times,
                              'Price': prices,
                              'Volume': volumes})
        tmpdf['Ticker'] = self.ticker
        self.df = self.df.append(tmpdf)

        next_page_tag = self.soup.find('a', {'id': Settings.settings['nextpage']})
        if next_page_tag and next_page_tag.has_attr('href'):
            next_page_link = next_page_tag['href']
            next_soup = BeautifulSoup(requests.get(next_page_link).content, 'html.parser')
            self.soup = next_soup
            self.secure_all_pages()

        else:    
            self.soup = self.init # reset link pointer to main ticker page
            self.df.drop_duplicates(keep = 'first', inplace = True) # drop duplicate rows
            self.df = clean_df(self.df)

        return self.df

    def run_every(self, seconds = 10, num_iter = 2):
        """
        secure new runs of data from NASDAQ
        :param seconds: number of seconds between runs
        :param num_iter: number of times to collect data
        :return: NA
        """
        while True:
            if num_iter != 0:
                print(num_iter)
                num_iter -= 1
                self.secure_all_pages()
                time.sleep(seconds)
            else:
                break


if __name__ == '__main__':

    AH = AfterHours('nflx', typeof = 'after')
    AH.getdata(datatype='lowprice')
    AH.getdata(datatype='highprice')
    AH.getdata(datatype='volume')
    AH.getdata(datatype='hightime')
    AH.getdata(datatype='lowtime')
    AH.secure_all_pages()
    AH.run_every(seconds = 5, num_iter = 2)
    AH.df.head(1000)
