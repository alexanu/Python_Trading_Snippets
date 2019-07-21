#*********************************************************************
 # Author: James Whiteley IV
 # Creation Date: 2017-12-14
 # Description: Web scraper that gets equity data from Morningstar.com, Nasdaq.com,
 #   and Marketwatch.com. Stores data as .pkl and/or .csv files.
 # Copyright 2017 James Whiteley IV
 # *******************************************************************
import os
import time
import datetime as dt
import pandas as pd
import urllib2
from BeautifulSoup import BeautifulSoup

def quarterly_fundamentals(ticker):
    '''
        returns last 5 quarters of income statement data from morningstar, 
        properly formatted for pandas df
    '''
    try:
        url = 'http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=' 
               + ticker + '&reportType=is&period=3&dataType=A&order=asc&columnYear=5&number=3'
        df = pd.read_csv(url, header=1)
        df = df.set_index(df.columns[0])
        Rev_idx = df.index.get_loc('Revenue')
        NI_idx = df.index.get_loc('Net income')
        EPS_idx = df.index.get_loc('Earnings per share')
        COGS = df.index.get_loc('Cost of revenue')
        EBITDA = df.index.get_loc('EBITDA')
        rows = [Rev_idx, NI_idx, EPS_idx+1, EPS_idx+2, COGS, EBITDA]
        df = df.iloc[rows]
        df.index.names = ['Date']
        df = df.transpose()
        df = df.rename(columns = {'Basic': 'EPS'})
        df = df.rename(columns = {'Cost of revenue': 'COGS'})
        df = df.ix[:-1]
        df.index = pd.to_datetime(df.index)
        df['Ticker'] = ticker
        df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce')
        df['Net income'] = pd.to_numeric(df['Net income'], errors='coerce')
        df['EPS'] = pd.to_numeric(df['EPS'], errors='coerce')
        df['Diluted'] = pd.to_numeric(df['Diluted'], errors='coerce')
        df['EBITDA'] = pd.to_numeric(df['EBITDA'], errors='coerce')
        df['COGS'] = pd.to_numeric(df['COGS'], errors='coerce')
        df.to_string(columns=['Ticker'])
        return df
    except:
        print 'Could not access fundamentals for', ticker


def annual_fundamentals(ticker):
    ''' 
        returns last 10 years of fundamental data from morningstar, 
        properly formatted for pandas df
    '''
    url = 'http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=' 
            + ticker + '&region=usa&culture=en-US&cur=USD'
    try:
        df = pd.read_csv(url, header=2, usecols=[i for i in range(0,11)], index_col=0)
        rows = ['Revenue USD Mil', 'Net Income USD Mil', 'Earnings Per Share USD', 'Operating Cash Flow USD Mil', 'Gross Margin %', 'Operating Margin %', 'Book Value Per Share * USD', 'Free Cash Flow Per Share * USD', 'Return on Equity %']
        df = df.loc[rows]
        df = df.rename(index={'Revenue USD Mil': 'Revenue'})
        df = df.rename(index={'Net Income USD Mil': 'Net_Income'})
        df = df.rename(index={'Earnings Per Share USD': 'EPS'})
        df = df.rename(index={'Operating Cash Flow USD Mil': 'Op_CF'})
        df = df.rename(index={'Gross Margin %': 'NPM'})
        df = df.rename(index={'Operating Margin %': 'OPM'})
        df = df.rename(index={'Book Value Per Share * USD': 'BVPS'})
        df = df.rename(index={'Free Cash Flow Per Share * USD': 'FCF'})
        df = df.rename(index={'Return on Equity %': 'ROE'})
        df = df.transpose()
        df['Ticker'] = ticker
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('Ticker')))
        df = df.reindex(columns=cols)
        df.to_string()
        df.Revenue = df.Revenue.str.replace(',', '')
        df.Net_Income = df.Net_Income.str.replace(',', '')
        df.Op_CF = df.Op_CF.str.replace(',', '')
        df.index.names = ['Date']
        df.index = pd.to_datetime(df.index)
        df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce')
        df['Net_Income'] = pd.to_numeric(df['Net_Income'], errors='coerce')
        df['EPS'] = pd.to_numeric(df['EPS'], errors='coerce')
        df['Op_CF'] = pd.to_numeric(df['Op_CF'], errors='coerce')
        df['NPM'] = pd.to_numeric(df['NPM'], errors='coerce')
        df['OPM'] = pd.to_numeric(df['OPM'], errors='coerce')
        df['BVPS'] = pd.to_numeric(df['BVPS'], errors='coerce')
        df['FCF'] = pd.to_numeric(df['FCF'], errors='coerce')
        df['ROE'] = pd.to_numeric(df['ROE'], errors='coerce')
        df.to_string(columns=['Ticker'])
        return df
    except:
        print 'Could not access fundamentals for ', ticker


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
    quarterly_df = []
    annual_df = []
    est_df = []
    sup_df = []

    for ticker in ticker_list:

        print "getting data for", ticker
        time.sleep(1) #don't scrape to fast and overload their servers!

        try:
            df = quarterly_fundamentals(ticker)
            if len(quarterly_df) == 0: #empty df, need to create
                quarterly_df = df
            else: #append 
                quarterly_df = quarterly_df.append(df, ignore_index=False)
        except:
            print "Could not access quart", ticker

        try:
            df = annual_fundamentals(ticker)
            if len(annual_df) == 0: #empty df, need to create
                annual_df = df
            else: #append 
                annual_df = annual_df.append(df, ignore_index=False)
        except:
            print "Could not access ann", ticker

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
        quarterly_df.to_csv('./' + directory + '/quarterly_fundamentals.csv')
        annual_df.to_csv('./' + directory + '/annual_fundamentals.csv')
        est_df.to_csv('./' + directory + '/analyst_estimates.csv')
        sup_df.to_csv('./' + directory + '/earnings_surprises.csv')
    if pkl: #store data as .pkl
        quarterly_df.to_pickle('./' + directory + '/quarterly_fundamentals.pkl')
        annual_df.to_pickle('./' + directory + '/annual_fundamentals.pkl')
        est_df.to_pickle('./' + directory + '/analyst_estimates.pkl')
        sup_df.to_pickle('./' + directory + '/earnings_surprises.pkl')
    else: #print data if not storing otherwise
        print quarterly_df
        print "------------------------------"
        print annual_df
        print "------------------------------"
        print est_df
        print "------------------------------"
        print sup_df
        print "------------------------------"


