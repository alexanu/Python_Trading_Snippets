#*********************************************************************
 # Author: James Whiteley IV
 # Creation Date: 2017-12-14
 # Description: Web scraper that gets equity data from Morningstar.com, Nasdaq.com,
 #   and Marketwatch.com. Stores data as .pkl and/or .csv files.
 # Copyright 2017 James Whiteley IV
 # *******************************************************************
 
 # Schema of the scraper:
 # https://bubbl.us/NDc3NDc4NC8zODMxNzgzLzJlMDZmNTliOTgwYjYyN2U3NGZmZDRmZWQwNWY5Zjg2@X?utm_source=shared-link&utm_medium=link&s=10033173
 
 
import os
import time
import datetime as dt
import pandas as pd
import urllib2
from BeautifulSoup import BeautifulSoup


def analyst_est(ticker):
    '''
    returns Analyst estimates, current price, and target price from Marketwatch.com
    '''
    try:
        date = dt.datetime.today().strftime('%Y-%m-%d')
        data = []
        header = ['Ticker', 'Recommendation', 'Target Price', 'Current Price']
        url = 'http://www.marketwatch.com/investing/stock/' + ticker + '/analystestimates'
        page = urllib2.urlopen(url)
        soup = BeautifulSoup(page)
        tr = soup.find('tr')
        price = soup.find('p', {"class":"data bgLast"}).text
        target = tr.findAll('td')[-1].text
        rec = str(tr.find('td', attrs={'class': 'recommendation'}).text)
        data.append([ticker, rec, target, price])
        df = pd.DataFrame(data, columns=header)        
        df['Date'] = date
        df = df.set_index(['Date'])
        df.index = pd.to_datetime(df.index)
        df['Target Price'] = pd.to_numeric(df['Target Price'], errors='coerce')
        df['Current Price'] = pd.to_numeric(df['Current Price'], errors='coerce')
        return df 
    except:
        print 'Could not access Analyst Estimates for ', ticker



def earnings_surprise(ticker):
    '''
    returns Earnings Surprise data from Nasdaq.com
    '''
    try:
        sup = []
        url = 'http://www.nasdaq.com/symbol/' + ticker + '/earnings-surprise'
        page = urllib2.urlopen(url)
        soup = BeautifulSoup(page)
        table = soup.find('div', attrs={'class': 'genTable'}).find('table').findAll('tr')[1:]
        temp = []
        headers = []
        for td in table:
            tData = td.findAll('td')
            data = tData[4].text
            temp.insert(0, data)
            headData = tData[1].text
            headData = dt.datetime.strptime(headData, '%m/%d/%Y')
            headData = headData.strftime('%Y-%m-%d')
            headers.insert(0, headData)

        sup.append(temp)
        df = pd.DataFrame(sup, columns=headers) 
        df = df.transpose()
        df = df.rename(columns={0: 'Surprise'})
        df['Ticker'] = ticker
        df.index.name = 'Date'
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('Ticker')))
        df = df.reindex(columns=cols)
        df.index = pd.to_datetime(df.index)
        df['Surprise'] = pd.to_numeric(df['Surprise'], errors='coerce')
        df.to_string(columns=['Ticker'])
        return df 
    except:
        print 'Could not access earnings surprise for ', ticker


def get_data(ticker_list, pkl=True, csv=False):
    '''
    gets quarterly and annual fundamentals, analyst recommendations, and earnings surprise data
    for specified ticker then stores in separate .pkl or .csv files for quick access.
    '''
    est_df = []
    sup_df = []

    for ticker in ticker_list:

        print "getting data for", ticker
        time.sleep(1) #don't scrape to fast and overload their servers!

        try: 
            df = analyst_est(ticker)
            if len(est_df) == 0: #empty df, need to create
                est_df = df
            else: #append 
                est_df = est_df.append(df, ignore_index=False)
        except:
            print "Could not access est", ticker

        try:
            df = earnings_surprise(ticker)
            if len(sup_df) == 0: #empty df, need to create
                sup_df = df
            else: #append 
                sup_df = sup_df.append(df, ignore_index=False)
        except:
            print "Could not access sup", ticker

    
    #create a directory 'equity data [today's date]' if not exists
    if csv or pkl:
        directory = 'equity data ' + str(dt.date.today())
        if not os.path.exists(directory):
            os.makedirs(directory)

    if csv: #store data as .csv
        est_df.to_csv('./' + directory + '/analyst_estimates.csv')
        sup_df.to_csv('./' + directory + '/earnings_surprises.csv')
    if pkl: #store data as .pkl
        est_df.to_pickle('./' + directory + '/analyst_estimates.pkl')
        sup_df.to_pickle('./' + directory + '/earnings_surprises.pkl')
    else: #print data if not storing otherwise
        print est_df
        print "------------------------------"
        print sup_df
        print "------------------------------"


