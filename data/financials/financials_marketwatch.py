# Source: https://github.com/VincentTatan/ValueInvesting/blob/55a94ef264aa1dcc3d764b73ff69f0631222e3b5/financialreportingdfformatted.py

import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests



def format(list):
    newlist=[]
    posornegnumber = 1
    for text in list:
        if text.endswith(')'):
            text = text[1:-1] # remove the parentheses
            posornegnumber = -1
            
        if text.endswith('%'):
            endtext = float(text[:-1].replace(",",""))/100.0 * posornegnumber 
        elif text.endswith('B'):
            endtext = int(float(text[:-1].replace(",",""))*1000000000)* posornegnumber 
        elif text.endswith('M'):
            endtext = int(float(text[:-1].replace(",",""))*1000000)* posornegnumber 
        elif ',' in text:
            endtext = int(float(text.replace(",","")))* posornegnumber 
        elif text.endswith('-'):
            endtext = 0
        else:
            endtext = float(text)* posornegnumber 
        newlist.append(endtext)
    return newlist 



def getfinancialreportingdf(ticker):

    # try:
    urlfinancials = 'https://www.marketwatch.com/investing/stock/'+ticker+'/financials'
    urlbalancesheet = 'https://www.marketwatch.com/investing/stock/'+ticker+'/financials/balance-sheet'

    text_soup_financials = BeautifulSoup(requests.get(urlfinancials).text,"lxml") #read in
    text_soup_balancesheet = BeautifulSoup(requests.get(urlbalancesheet).text,"lxml") #read in


    # Income statement
    titlesfinancials = text_soup_financials.findAll('td', {'class': 'rowTitle'})
    epslist=[]
    netincomelist = []
    longtermdebtlist = [] 
    interestexpenselist = []
    ebitdalist= []

    for title in titlesfinancials:
        if 'EPS (Basic)' in title.text:
            epslist.append ([td.text for td in title.findNextSiblings(attrs={'class': 'valueCell'}) if td.text])
        if 'Net Income' in title.text:
            netincomelist.append ([td.text for td in title.findNextSiblings(attrs={'class': 'valueCell'}) if td.text])
        if 'Interest Expense' in title.text:
            interestexpenselist.append ([td.text for td in title.findNextSiblings(attrs={'class': 'valueCell'}) if td.text])
        if 'EBITDA' in title.text:
            ebitdalist.append ([td.text for td in title.findNextSiblings(attrs={'class': 'valueCell'}) if td.text])


    # Balance sheet
    titlesbalancesheet = text_soup_balancesheet.findAll('td', {'class': 'rowTitle'})
    equitylist=[]
    for title in titlesbalancesheet:
        if 'Total Shareholders\' Equity' in title.text:
            equitylist.append( [td.text for td in title.findNextSiblings(attrs={'class': 'valueCell'}) if td.text])
        if 'Long-Term Debt' in title.text:
            longtermdebtlist.append( [td.text for td in title.findNextSiblings(attrs={'class': 'valueCell'}) if td.text])

    # Variables        
    # eps = epslist[0]
    # epsgrowth = epslist[1]
    # netincome = netincomelist[0]
    # shareholderequity = equitylist[0]
    # roa = equitylist[1]

    # longtermdebt = longtermdebtlist[0]
    # interestexpense = interestexpenselist[0]
    # ebitda = ebitdalist[0]

    eps = getelementinlist(epslist,0)
    epsgrowth = getelementinlist(epslist,1)
    netincome = getelementinlist(netincomelist,0)
    shareholderequity = getelementinlist(equitylist,0)
    roa = getelementinlist(equitylist,1)

    longtermdebt = getelementinlist(longtermdebtlist,0)
    interestexpense =  getelementinlist(interestexpenselist,0)
    ebitda = getelementinlist(ebitdalist,0)
    # Don't forget to add in roe, interest coverage ratio

    ## Make it into Dataframes
    df= pd.DataFrame({'eps': eps,'epsgrowth': epsgrowth,'netincome': netincome,'shareholderequity': shareholderequity,'roa': 
                  roa,'longtermdebt': longtermdebt,'interestexpense': interestexpense,'ebitda': ebitda},index=[2014,2015,2016,2017,2018])
    return df

def getelementinlist(list,element):
    try:
        return list[element]
    except:
        return '-'

# Getting financial reporting df
def getfinancialreportingdfformatted(ticker):
    df = getfinancialreportingdf(ticker)
    # Format all the number in dataframe
    dfformatted = df.apply(format)

    # Adding roe, interest coverage ratio
    dfformatted['roe'] = dfformatted.netincome/dfformatted.shareholderequity
    dfformatted['interestcoverageratio'] = dfformatted.ebitda/dfformatted.interestexpense

#     Insert ticker and df
    return dfformatted


# This will keep tickers + gics industries & sub industries
def save_sp500_stocks_info():
    print("Getting SP500 stocks info from wikipedia")
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    stocks_info=[]
    tickers = []
    securities = []
    gics_industries = []
    gics_sub_industries = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        security = row.findAll('td')[1].text
        gics_industry = row.findAll('td')[3].text
        gics_sub_industry = row.findAll('td')[4].text

        tickers.append(ticker.lower().replace(r"\n", " "))
        securities.append(security)
        gics_industries.append(gics_industry.lower())
        gics_sub_industries.append(gics_sub_industry.lower())
    
    stocks_info.append(tickers)
    stocks_info.append(securities)
    stocks_info.append(gics_industries)
    stocks_info.append(gics_sub_industries)
    
    stocks_info_df = pd.DataFrame(stocks_info).T
    stocks_info_df.columns=['tickers','security','gics_industry','gics_sub_industry']
    stocks_info_df['seclabels'] = 'SP500'
    stocks_info_df['labels'] = stocks_info_df[['tickers','security', 'gics_industry','gics_sub_industry','seclabels']].apply(lambda x: ' '.join(x), axis=1)

    # Create a list of dict based on tickers and labels
    dictlist = []
    for index, row in stocks_info_df.iterrows():
        dictlist.append({'value':row['tickers'], 'label':row['labels']})
    return dictlist

# This will keep tickers from russell
def save_russell_info():
    print("Getting russell stocks info")

    dfrussel=pd.read_csv('C:/Users/vintatan/Desktop/Investment/RussellandData.csv',index_col='Symbol')
    dfrussel['tickers'] = dfrussel.index.str.upper()
    dfrussel['tickers'] = dfrussel['tickers'].replace(r"\n", " ")
    dfrussel['security'] = dfrussel.Description.str.title()
    dfrussel['gics_industry'] = dfrussel.Sector.str.lower()
    dfrussel['gics_sub_industry'] = dfrussel.Industry.str.lower()
    dfrussel['seclabels'] = 'RUSSELL'

    dfrussel['labels'] = dfrussel[['tickers','security','gics_industry','gics_sub_industry','seclabels']].apply(lambda x: ' '.join(x), axis=1)

    dictlist = []
    for index, row in dfrussel.iterrows():
        dictlist.append({'value':row['tickers'], 'label':row['labels']})
    return dictlist


# self append
def save_self_stocks_info():
    print("Adding own list of stocks info")

    dictlist = []

    dictlist.append({'value':'ajbu', 'label':'AJBU Keppel DC Reit Data REITS SA'})
    dictlist.append({'value':'gme', 'label':'GME Game Stop Corp SA'})
    dictlist.append({'value':'aeg', 'label':'AEG Aegon Insurance SA'})
    dictlist.append({'value':'ntic', 'label':'NTIC Northern Technologies International SA'})
    dictlist.append({'value':'sq', 'label':'SQ Square SA'})
    dictlist.append({'value':'kbsty', 'label':'Kobe steel'})
    dictlist.append({'value':'NESN', 'label':'Nestle'})
    dictlist.append({'value':'BN', 'label':'Danone'})
    dictlist.append({'value': 'DATA', 'label': 'Tableau Software Data Visualization'})

    return dictlist

def eligibilitycheck(ticker,dfformatted):
    
    legiblestock = True
    reasonlist=[]

    # print (dfformatted)
    # EPS increases over the year (consistent)
    for growth in dfformatted.epsgrowth:
        if growth<0:
            legiblestock = False
            reasonlist.append('there is negative growth '+str(growth))
            break
    # ROE > 0.15
    if dfformatted.roe.mean()<0.13:
            legiblestock = False
            reasonlist.append('roe mean is less than 0.13 '+ str(dfformatted.roe.mean()))
    # ROA > 0.07 (also consider debt to equity cause Assets = liabilities + equity)
    if dfformatted.roa.mean()<0.07:
            legiblestock = False
            reasonlist.append('roa mean is less than 0.07 ' + str(dfformatted.roa.mean()))
    # Long term debt < 5 * income
    if dfformatted.longtermdebt.tail(1).values[0]>5*dfformatted.netincome.tail(1).values[0]:
            legiblestock = False
            reasonlist.append('longtermdebt is 5 times the netincome ')
    # Interest Coverage Ratio > 3
    if dfformatted.interestcoverageratio.tail(1).values[0]<3:
            legiblestock = False
            reasonlist.append('Interestcoverageratio is less than 3 ')
#     print ticker,legiblestock,reasonlist
    return reasonlist
