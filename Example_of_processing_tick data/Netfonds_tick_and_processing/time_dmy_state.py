# -*- coding: utf-8 -*-
"""
Created on Tue May 01 20:59:57 2018

@author: jjplombo
"""

print 'time day month year state machine:  module load...'

from pandas.tseries.offsets import *
import datetime as dt

import time
import itertools
# ================================================================== #
# timer start #
t0 = time.clock() 

# ================================================ #
# functions
# ~~~~~~~~~~~~~~~~~~

now   = dt.date.today()
year  = str(now.year)
m     = str(now.month)
month = '0'+m

day_5 = now - 5 * BDay()
day_4 = now - 4 * BDay()
day_3 = now - 3 * BDay()
day_2 = now - 2 * BDay()
day_1 = now - 1 * BDay()
# Add current day_0
day_0 = now - 0 * BDay()

days  = [ day_5.day, day_4.day, day_3.day, day_2.day, day_1.day, day_0.day ]
months = [ day_5.month, day_4.month, day_3.month, day_2.month, day_1.month, day_0.month ]
years = [ day_5.year, day_4.year, day_3.year, day_2.year, day_1.year, day_0.year ]
days  = [ str(d) for d in days ]
months  = [ str(ms) for ms in months ]
years  = [ str(ys) for ys in years ]

#print now
#print days
#print months
#print years

def get_trade_dump_url_current_day( symbol ):
    current_day_idx = 5
    exchange_sym = 'A'
    url_tdump = r'http://www.netfonds.no/quotes/tradedump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'
    m_getLastFiveTradingDays()
    
    strURL_currentday = []
    #for day in days:
    day = days[current_day_idx]
    month = months[current_day_idx]
    year = years[current_day_idx]
    
    try:        
        if len(day) == 1:
        
            day = '0' + day
        if len(month) == 1:
            month = '0' + month

        full_url = url_tdump % ( year, month, day, symbol, exchange_sym )
        #strURL_currentday =+ full_url
        strURL_currentday.append( full_url )

    except Exception as e:
        print( "{} {} {} {} {} trade dump not found".format( symbol, day, month, year, full_url ) )

    return strURL_currentday


def get_trade_dump_url_five_conseq_days( symbol ):
    exchange_sym = 'A'
    url_tdump = r'http://www.netfonds.no/quotes/tradedump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'
    m_getLastFiveTradingDays()
    
    strURL_five_days = []
    #for day in days:
    for (day, month, year) in itertools.izip(days, months, years):
        try:        
            if len(day) == 1:
        
                day = '0' + day
            if len(month) == 1:
                month = '0' + month

            full_url = url_tdump % ( year, month, day, symbol, exchange_sym )
            strURL_five_days.append( full_url )

        except Exception as e:
            print( "{} {} {} {} {} trade dump not found".format( symbol, day, month, year, full_url ) )
    
    return strURL_five_days
    
def get_bid_ask_dump_url_current_day( symbol ):
    current_day_idx = 5
    exchange_sym = 'A'
    url_bid_ask_dump  = r'http://www.netfonds.no/quotes/posdump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'
    m_getLastFiveTradingDays()
    
    strURL_currentday = []
    #for day in days:
    day = days[current_day_idx]
    month = months[current_day_idx]
    year = years[current_day_idx]
    
    try:        
        if len(day) == 1:
        
            day = '0' + day
        if len(month) == 1:
            month = '0' + month

        full_url = url_bid_ask_dump % ( year, month, day, symbol, exchange_sym )
        #strURL_currentday =+ full_url
        strURL_currentday.append( full_url )

    except Exception as e:
        print( "{} {} {} {} {} trade dump not found".format( symbol, day, month, year, full_url ) )

    return strURL_currentday

def get_bid_ask_dump_url_five_conseq_days( symbol ):
    exchange_sym = 'A'
    #url_tdump = r'http://www.netfonds.no/quotes/tradedump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'
    m_getLastFiveTradingDays()
    url_bid_ask_dump  = r'http://www.netfonds.no/quotes/posdump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'

    # ~~~~~~~~~~~~~~~~~~
    #for day in days:
    strURL_five_conseq_day = []
    
    for (day, month, year) in itertools.izip(days, months, years):
        try:
            #  Append '0' to day and month if single digit
            if len(day) == 1:
                day = '0' + day
            if len(month) == 1:
                month = '0' + month

            full_url = url_bid_ask_dump % ( year, month, day, symbol, exchange_sym )
            strURL_five_conseq_day.append( full_url )
            
        except Exception as e:
            print( "{} {} {} {} {} trade dump not found".format( symbol, day, month, year, full_url ) )
    
    return strURL_five_conseq_day
    
    
def get_current_day_file_bid_ask( symbol ):
    current_day_idx = 5
    current_day_file = ''
    
    m_getLastFiveTradingDays()
     
    day = days[current_day_idx]
    month = months[current_day_idx]
    year = years[current_day_idx]
    
    if len(day) == 1:
        day = '0' + day
    if len(month) == 1:
        month = '0' + month
    
    # format:  IWM_05_02_2018_bid_offer.txt
    current_day_file = symbol + '_' + month + '_' + day + '_' + year + '_bid_offer.txt'
    
    return current_day_file
    
def get_last_business_day_file_bid_ask( symbol ):
    last_business_idx = 4
    last_business_day_file = ''
    
    m_getLastFiveTradingDays()
     
    day = days[last_business_idx]
    month = months[last_business_idx]
    year = years[last_business_idx]
    
    if len(day) == 1:
        day = '0' + day
    if len(month) == 1:
        month = '0' + month
    
    # format:  IWM_05_02_2018_bid_offer.txt
    last_business_day_file = symbol + '_' + month + '_' + day + '_' + year + '_bid_offer.txt'
    
    return last_business_day_file
    
def get_2nd_to_last_business_day_file_bid_ask( symbol ):
    second_to_business_idx = 3
    second_to_last_business_day_file = ''
    
    m_getLastFiveTradingDays()
     
    day = days[second_to_business_idx]
    month = months[second_to_business_idx]
    year = years[second_to_business_idx]
    
    if len(day) == 1:
        day = '0' + day
    if len(month) == 1:
        month = '0' + month
    
    # format:  IWM_05_02_2018_bid_offer.txt
    second_to_last_business_day_file = symbol + '_' + month + '_' + day + '_' + year + '_bid_offer.txt'
    
    return second_to_last_business_day_file

def get_current_day_file_trade( symbol ):
    current_day_idx = 5
    current_day_file = ''
    
    m_getLastFiveTradingDays()
     
    day = days[current_day_idx]
    month = months[current_day_idx]
    year = years[current_day_idx]
    
    if len(day) == 1:
        day = '0' + day
    if len(month) == 1:
        month = '0' + month
    
    # format:  IWM_05_02_2018_bid_offer.txt
    current_day_file = symbol + '_' + month + '_' + day + '_' + year + '_trade.txt'
    
    return current_day_file
    
def get_last_business_day_file_trade( symbol ):
    last_business_idx = 4
    last_business_day_file = ''
    
    m_getLastFiveTradingDays()
     
    day = days[last_business_idx]
    month = months[last_business_idx]
    year = years[last_business_idx]
    
    if len(day) == 1:
        day = '0' + day
    if len(month) == 1:
        month = '0' + month
    
    # format:  IWM_05_02_2018_bid_offer.txt
    last_business_day_file = symbol + '_' + month + '_' + day + '_' + year + '_trade.txt'
    
    return last_business_day_file
    

def is_non_business_day():
    #  Determine current Day:
    now   = dt.date.today()
    #print ( 'Today: {}'.format(now) )
    #print ( 'The day today: {}'.format(now.day) )
    #print ( 'The current and past business days: {}, {}, {}, {}, {}, {}'.format(day_0.day, day_1.day, day_2.day, day_3.day, day_4.day, day_5.day, now.day) )
    
    #  Does current day belong to the last 5 business days?
    if day_0.day == now.day:
        return False
    if day_1.day == now.day:
        return False
    
    return True
    
#b_today_business_day = is_non_business_day()
#print b_today_business_day

#-----------------------------------------------------------------------------------------------------
#
#  Private methods:
#
#-----------------------------------------------------------------------------------------------------
    
def m_getLastFiveTradingDays():
    now   = dt.date.today()
    year  = str(now.year)
    m     = str(now.month)
    month = '0'+m

    day_5 = now - 5 * BDay()
    day_4 = now - 4 * BDay()
    day_3 = now - 3 * BDay()
    day_2 = now - 2 * BDay()
    day_1 = now - 1 * BDay()
    # Add current day_0
    day_0 = now - 0 * BDay()

    days  = [ day_5.day, day_4.day, day_3.day, day_2.day, day_1.day, day_0.day ]
    months = [ day_5.month, day_4.month, day_3.month, day_2.month, day_1.month, day_0.month ]
    years = [ day_5.year, day_4.year, day_3.year, day_2.year, day_1.year, day_0.year ]
    days  = [ str(d) for d in days ]
    months  = [ str(ms) for ms in months ]
    years  = [ str(ys) for ys in years ]

    #print now
    #print days
    #print months
    #print years
        
    return
