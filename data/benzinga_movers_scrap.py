# Source: benzinga_movers_scraper/blob/master/scripts/scrape_benzinga.py

import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
import re
import pandas_datareader.data as web
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def get_equities_up_down(ls): #cleans up mid-afternoon update pages to only show equities trading up and down
    eflag = False
    ls2 = []
    #if paragraph starts with these then don't save those paragraphs' text
    r = re.compile('^Commodities|^Eurozone|^Economics|^Top Headline|^Leading and Lagging Sectors')
    
    for item in ls:
        if 'Equities Trading UP' in item.text or 'Equities Trading DOWN' in item.text:
            eflag = True
        elif r.match(item.text):
            eflag = False
        elif eflag == True:
            ls2.append(item)
    return ls2

def page_is_passed_end_date(ls, end_date): #function for finding if page is past specified end date
    end_date = pd.to_datetime(end_date)
    
    dt_ls = []
    for item in ls:
        date = item.text.strip()[:25].strip()
        date = pd.to_datetime(date)
        dt_ls.append(date)
    
    return all(d < end_date for d in dt_ls)

def get_initial_mid_afternoon_movers(start_page, to_date):
    data = {'ticker':[], 'descriptions':[], 'date':[], 'link':[]}
    
    past_to_date = False
    past_last_mid_afternoon_movers_date = False
    i = start_page
    
    while past_to_date == False and past_last_mid_afternoon_movers_date == False:
        url = 'https://www.benzinga.com/author/lisa-levin?page=' + str(i)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        movers = soup.find_all('a', text = re.compile('Mid-Afternoon Market Update'))
        print('%d mid-afternoon update links found on page %d...' %(len(movers),i))

        #if no movers links found on page, pass
        if len(movers) == 0:
            pass

        for lk in movers:
            href = lk.get('href')

            #open link
            url2 = 'https://www.benzinga.com' + href
            page2 = requests.get(url2)
            soup2 = BeautifulSoup(page2.content, 'html.parser')

            #get date
            date = soup2.find_all('div', class_ = 'article-date-wrap')[0].text.strip()[:25].strip()

            #body of article is list of ticker and movement descriptions
            article_body = soup2.find_all('div', class_ = 'article-content-body-only')[0]

            #get paragraphs of lists of companies
            paragraphs = article_body.find_all('p')
            
            #only keep paragraphs with equities up down updates
            paragraphs = get_equities_up_down(paragraphs)

            for row in paragraphs:
                #if no ticker listed then pass
                try:
                    tck = row.find_next('a', class_ = 'ticker').text
                except:
                    pass

                dsc = row.text

                #add ticker and description to data
                data['ticker'] += [tck]
                data['descriptions'] += [dsc]

                #add date
                data['date'] += [date]

                #add link
                data['link'] += [url2]
        
        #check to see if to_date has been reached or date of last biggest movers article reached
        page_dates = soup.find_all('span', class_ = 'field-content', text = re.compile('AM$|PM$'))
        
        if page_is_passed_end_date(page_dates, to_date):
            past_to_date = True 
        elif page_is_passed_end_date(page_dates, 'October 12, 2015'):
            past_last_mid_afternoon_movers_date = True
        else:    
            i += 1


    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df.date)
    df = df[['date', 'ticker', 'descriptions', 'link']]

    #remove white spaces at beginning and end
    df['descriptions'] = df.descriptions.str.strip() 
    df['ticker'] = df.ticker.str.strip()
    
    #returns dataframe and page stopped on
    return df

def get_initial_mid_day_movers(start_page, to_date):
    data = {'ticker':[], 'descriptions':[], 'date':[], 'link':[]}
    
    past_to_date = False
    past_last_midday_movers_date = False
    i = start_page
    
    while past_to_date == False and past_last_midday_movers_date == False:
        url = 'https://www.benzinga.com/author/lisa-levin?page=' + str(i)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        movers = soup.find_all('a', text = re.compile('Biggest Mid-Day|Stocks Moving In.*Mid-Day'))
        print('%d mid-day movers links found on page %d...' %(len(movers),i))

        #if no movers links found on page, pass
        if len(movers) == 0:
            pass

        for lk in movers:
            href = lk.get('href')

            #open link
            url2 = 'https://www.benzinga.com' + href
            page2 = requests.get(url2)
            soup2 = BeautifulSoup(page2.content, 'html.parser')

            #get date
            date = soup2.find_all('div', class_ = 'article-date-wrap')[0].text.strip()[:25].strip()

            #body of article is list of ticker and movement descriptions
            article_body = soup2.find_all('div', class_ = 'article-content-body-only')[0]

            #check if it's a list or paragraphs of companies
            lists = article_body.find_all('li')
            paragraphs = article_body.find_all('p')


            if len(lists) >= 1:
                #loop through all items in list to get ticker and price movement descriptions
                for row in lists:
                    #if no ticker listed then pass
                    try:
                        tck = row.find_next('a', class_ = 'ticker').text
                    except:
                        pass

                    dsc = row.text

                    #add ticker and description to data
                    data['ticker'] += [tck]
                    data['descriptions'] += [dsc]

                    #add date
                    data['date'] += [date]

                    #add link
                    data['link'] += [url2]

            #if it's not a list of companies, check if it's in paragraph form
            elif len(paragraphs) >= 1:
                for row in paragraphs:
                    #if no ticker listed then pass
                    try:
                        tck = row.find_next('a', class_ = 'ticker').text
                    except:
                        pass

                    dsc = row.text

                    #add ticker and description to data
                    data['ticker'] += [tck]
                    data['descriptions'] += [dsc]

                    #add date
                    data['date'] += [date]

                    #add link
                    data['link'] += [url2]
        
        #check to see if to_date has been reached or date of last biggest movers article reached
        page_dates = soup.find_all('span', class_ = 'field-content', text = re.compile('AM$|PM$'))
        
        if page_is_passed_end_date(page_dates, to_date):
            past_to_date = True    
        elif page_is_passed_end_date(page_dates, 'June 6, 2016'):
            past_last_midday_movers_date = True
        else:    
            i += 1


    df = pd.DataFrame(data)
    
    if past_to_date == False:
        df2 = get_initial_mid_afternoon_movers(i, to_date)
        df = pd.concat([df, df2], 0)
    
    df['date'] = pd.to_datetime(df.date)
    df = df[['date', 'ticker', 'descriptions', 'link']]

    #remove white spaces at beginning and end
    df['descriptions'] = df.descriptions.str.strip() 
    df['ticker'] = df.ticker.str.strip()
    
    #returns dataframe and page stopped on
    return df

def get_initial_biggest_movers_data(to_date): #set date of how far back we want to go
    data = {'ticker':[], 'descriptions':[], 'date':[], 'link':[]}
            
    past_to_date = False
    past_last_biggest_movers_date = False
    i = 0
    
    while past_to_date == False and past_last_biggest_movers_date == False:
        url = 'https://www.benzinga.com/author/lisa-levin?page=' + str(i)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        movers = soup.find_all('a', text = re.compile('Biggest Movers'))
        print('%d movers links found on page %d...' %(len(movers),i))

        #if no movers links found on page, pass
        if len(movers) == 0:
            pass

        for lk in movers:
            href = lk.get('href')

            #open link
            url2 = 'https://www.benzinga.com' + href
            page2 = requests.get(url2)
            soup2 = BeautifulSoup(page2.content, 'html.parser')

            #get date
            date = soup2.find_all('div', class_ = 'article-date-wrap')[0].text.strip()[:25].strip()

            #body of article is list of ticker and movement descriptions
            article_body = soup2.find_all('div', class_ = 'article-content-body-only')[0]

            #loop through all items in list to get ticker and price movement descriptions
            for row in article_body.find_all('li'):
                 #if no ticker listed then pass
                try:
                    tck = row.find_next('a', class_ = 'ticker').text
                except:
                    pass
                
                dsc = row.text

                #add ticker and description to data
                data['ticker'] += [tck]
                data['descriptions'] += [dsc]

                #add date
                data['date'] += [date]

                #add link
                data['link'] += [url2]
        
        #check to see if to_date has been reached or date of last biggest movers article reached
        page_dates = soup.find_all('span', class_ = 'field-content', text = re.compile('AM$|PM$'))

        if page_is_passed_end_date(page_dates, to_date):
            past_to_date = True 
        elif page_is_passed_end_date(page_dates, 'October 16, 2017'):
            past_last_biggest_movers_date = True
        else: 
            i += 1
                    
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df.date) - BDay(1) #set to previous business day
    df = df[['date', 'ticker', 'descriptions', 'link']]
    
    #remove white spaces at beginning and end
    df['descriptions'] = df.descriptions.str.strip() 
    df['ticker'] = df.ticker.str.strip()
    
    if past_to_date == False:
        #gather Biggest Mid-Day|Stocks Moving In.*Mid-Day links starting where biggest movers from yesterday ends          
        df2 = get_initial_mid_day_movers(i, to_date)

        #concat biggest movers from yesterday to biggest mid-day movers
        df = pd.concat([df, df2], 0)
            
    #drop duplicates
    df = df.drop_duplicates()
        
    return df

def get_new_benzinga_data(old_data): #gets new data given an df of previous collected data
    data = {'ticker':[], 'descriptions':[], 'date':[], 'link':[]}
    link_saved_already = 0
    i = 0
    while link_saved_already == 0:
        url = 'https://www.benzinga.com/author/lisa-levin?page=' + str(i)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        movers = soup.find_all('a', text = re.compile('Biggest Movers'))
        print('%d movers links found on page %d...' %(len(movers),i))        
        if len(movers) == 0: # #if no movers links found on page, ...
            break

        for lk in movers:
            href = lk.get('href')
            url2 = 'https://www.benzinga.com' + href
            if url2 in set(old_data.link): # if link already encountered, ...
                link_saved_already = 1 # ... break and set boolean for link saved already = 1
                break
            page2 = requests.get(url2)
            soup2 = BeautifulSoup(page2.content, 'html.parser')
            date = soup2.find_all('div', class_ = 'article-date-wrap')[0].text.strip()[:25].strip()
            article_body = soup2.find_all('div', class_ = 'article-content-body-only')[0] #body of article is list of ticker and movement descriptions             
            for row in article_body.find_all('li'): #loop through all items in list
                tck = row.find_next('a', class_ = 'ticker').text
                dsc = row.text
                data['ticker'] += [tck] #add ticker and description to data
                data['descriptions'] += [dsc]                
                data['date'] += [date] #add date                
                data['link'] += [url2] #add link
        i += 1
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df.date) - BDay(1) #set to previous business day
    df = df[['date', 'ticker', 'descriptions', 'link']]
    df = df.drop_duplicates()  #drop duplicates
    
    return(df)
