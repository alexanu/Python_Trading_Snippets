

# SPDRs Holdings
# list of SPDRs ETFs is taken https://www.ssga.com/de/en_gb/institutional/etfs/fund-finder
# to-do: parse for all ETFs

import pandas as pd
import requests

String_of_SPDR_ETF = ("ZPRL-GY, SPYF-GY, ZPRD-GY, SPP1-GY, SPYI-GY, SPYY-GY, SPP2-GY, SPYA-GY, ZPRE-GY, SPYX-GY, SPYM-GY, SPYT-GY, SPYR-GY, SPYC-GY, "
                       "SPYN-GY, SPYZ-GY, SPYH-GY, SPYQ-GY, SPYP-GY, SMC-FP^, ZPRX-GY, SPYK-GY, SPYE-GY, SPYU-GY, ZPDW-GY, ZPDJ-GY, ZPRV-GY, WTEL-NA, "
                       "WCOD-NA, WCOS-NA, WNRG-NA, WFIN-NA, WHEA-NA, WIND-NA, WMAT-NA, ZPRS-GY, WTCH-NA, SPPW-GY, WUTI-NA, ZPRR-GY, SPY4-GY, "
                       "SPPY-GY, SPPE-GY, SPY1-GY, SPY5-GY, SPYV-GY, SPYW-GY, ZPRG-GY, ZPRA-GY, ZPDK-GY, ZPDD-GY, ZPDS-GY, SPPD-GY, SPYD-GY, ZPDE-GY, "
                       "ZPDF-GY, ZPDH-GY, ZPDI-GY, ZPDM-GY, ZPDT-GY, ZPDU-GY, SPYG-GY, ZPDX-GY, GLOW-NA")

SPDR_ETF=[x.lower() for x in String_of_SPDR_ETF.split(", ")]
url = "https://www.ssga.com/library-content/products/fund-data/etfs/emea/holdings-daily-emea-en-"
SPDRs = pd.concat((pd.read_excel(url+ticker+".xlsx", skiprows=5).assign(Ticker=ticker) for ticker in SPDR_ETF if requests.head(url+ticker+".xlsx").status_code == 200), ignore_index=True)
SPDRs.to_csv("SPDRs_constit.csv")


###############################################################################################

"""download to csv file ETF holdings

    Materials (XLB), Energy (XLE), Financials (XLF), Industrials (XLI), Technology (XLK),
    Staples (XLP), Utilities (XLU), Health care (XLV), Consumer discretionary (XLY)
"""

import pandas as pd

url = 'https://www.sectorspdr.com/sectorspdr/IDCO.Client.Spdrs.Portfolio/Export/ExportCsv?symbol='
tickers = ['XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY']
SPDRs = pd.concat((pd.read_csv(url+ticker, skiprows=1).assign(Ticker=ticker) for ticker in tickers), ignore_index=True)

#################################################################################################


    import pandas as pd
    import finnhub
    token='XXXX'
    hub = finnhub.Client(api_key=token)


    # def etfs_holdings(self, symbol): return self._get("/etf/holdings", params={"symbol": symbol})
    # Only for US
    pd.json_normalize(hub.etfs_holdings(symbol='RYT')['holdings'])
    pd.json_normalize(hub.etfs_holdings(symbol='FTEC')['holdings'])

    # def etfs_ind_exp(self, symbol): return self._get("/etf/holdings", params={"symbol": symbol})
    # def etfs_country_exp(self, symbol): return self._get("/etf/country", params={"symbol": symbol})
    # Only for US
    pd.json_normalize(hub.etfs_country_exp(symbol='ROBO')['countryExposure'])
    pd.json_normalize(hub.etfs_country_exp(symbol='GACA.DE')['countryExposure'])

    # Which funds owns a stock
        # def fund_ownership(self, symbol, limit=None): return self._get("/stock/fund-ownership", params={"symbol": symbol,"limit": limit})
        # For global ETFs, but old data (from quarterly forms)
        pd.json_normalize(hub.fund_ownership(symbol='AAPL',limit=10)['ownership'])
        pd.json_normalize(hub.fund_ownership(symbol='PUM.DE',limit=10)['ownership'])



###########################################################################################################



# iShares constitutes
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as bs # BS parses HTML into an easy machine readable tree format to extract DOM Elements quickly. 
import html5lib
import requests # get HTML element from URL, this will become the input for BS

import time
from datetime import datetime 
import pandas as pd
import numpy as np
import csv


iShares_link = "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments#!type=ishares&view=keyFacts"
html_content = requests.get(iShares_link).text
soup = bs(html_content)
print(soup.prettify())
print(soup.title.text)

find_all(class_="className")
mydivs = soup.find_all(class_="tb_fundNames ng-star-inserted")
spans = soup.find_all('div', {'class' : 'inline isin'})
mydivs = soup.find_all("a", {"class": "tb_fundNames ng-star-inserted"})
for td in soup.find_all("td"):
    print(td)
    print("Inner Text: {}".format(link.text))
    print("Title: {}".format(link.get("title")))
    print("href: {}".format(link.get("href")))



Invesco_ETFs_list="https://etf.invesco.com/de/private/en/products"
html_content = requests.get(Invesco_ETFs_list, headers={'User-Agent': 'Mozilla/5.0'}).text
soup = bs(html_content, "html.parser")


soup = bs(html_content, "html.parser")

print(soup.prettify())

for foo in soup.find_all('a'):
    bar = foo.find('div', {'class': 'inline isin'})
    print(bar.text)

data = soup.find_all('div', {'class': 'inline isin'})
print(data[0].text)

for item in data:
    print(item['href'])

import json

results = json.loads(soup.find('div', {'class': 'inline isin'}))
for result in results:
    print result["title"]

for a in soup.find_all('a'):
    print("Found the URL:", a['href'])



ETF_directory= "C:\\Users\\oanuf\\Google Drive\\Programming\\GitHub\\Python_Trading_Snippets\\data\\Tkrs_metadata\\Tkrs\\ETF\\"
holdings_links=pd.read_excel(ETF_directory + 'ETFDB.xlsx',sheet_name='Holdings_links') 
BlackRock_holdings = pd.concat([pd.read_csv(link,skiprows=2,decimal=",",thousands='.').assign(ISIN_ETF = ISI) 
                                for link, ISI in zip(holdings_links["Link"][0:3], holdings_links["ISIN"][0:3])])


frames = list()
ETFs = 0
for link, ISI in zip(holdings_links[holdings_links.Product_Family=="iShares"]["Link"], holdings_links[holdings_links.Product_Family=="iShares"]["ISIN"]):
    try:
        df=pd.read_csv(link,skiprows=2,decimal=",",thousands='.').assign(ISIN_ETF = ISI)
    except:
        print("Not able to read holdings for {}".format(ISI))
    else:
        frames.append(df)
    ETFs = ETFs + 1
print("Read %i out of %i ETFs" % (ETFs, len(holdings_links)))
BlackRock_holdings = pd.concat(frames, ignore_index=True)
BlackRock_holdings.to_excel(ETF_directory + 'iShares_UCITS_holdings_'+time.strftime("%d-%m-%Y")+'.xlsx', sheet_name='Holdings',index=False) 



linke = holdings_links[holdings_links.Product_Family=="Xtrackers"]["Link"][434]
ISI = holdings_links[holdings_links.Product_Family=="Xtrackers"]["ISIN"][434]
df=pd.read_csv(linke,encoding = "ISO-8859-1",skiprows=3)

#----------------------------------------------------------------------------------------
# XTF fund holdings

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import *
from selenium.webdriver.common.keys import Keys
import pandas
import xlwt
import time
import re



def launchurl(url,driverpath):
    browser = webdriver.Chrome(driverpath)
    browser.get(url)
    time.sleep(2)

    #give time to close the pop up
    time.sleep(8)

    return browser



def downloadFundHoldings(browser):
    '''
    steps:    
    1.launch url
    2.click fundholdings tab
    3.parsse html to soup object
    4.find maintable
        
    5.iterate row wise under maintable
    6.find column data 0,2 under each row and store each text in a list of lists -data= [[ , ],[ , ],[ , ],..]
    7.switch back to other tab
    '''

    df=pandas.read_excel('ETF ratings.xls',header=0)
    
    etfs=df.values.transpose().tolist()[0]
    print(etfs[:10])
    
    fund_holdings_url='http://www.xtf.com/ETF-Ratings/'
    browser.get(fund_holdings_url) #1
    time.sleep(3)

    wb = xlwt.Workbook()        
    s2 = wb.add_sheet('sheet 0')

    headers=['Symbol','Weight']

    for i in range(len(headers)):
        s2.write(0,i,headers[i])
    
    
    tabsPanel_id='ctl00_Main_RatingsTabs_TC'
    fundholdings_id='ctl00_Main_RatingsTabs_T2'
    ative_fundholdings_id='ctl00_Main_RatingsTabs_AT2'
    maintable_id='ctl00_Main_RatingsTabs_ctl46_grdListOfETFs_DXMainTable'
    headerrow_id='ctl00_Main_RatingsTabs_ctl46_grdListOfETFs_DXHeadersRow0'
    symbolcol_id='ctl00_Main_RatingsTabs_ctl46_grdListOfETFs_col0'
    weightcol_id='ctl00_Main_RatingsTabs_ctl46_grdListOfETFs_col2'
    row_id='ctl00_Main_RatingsTabs_ctl46_grdListOfETFs_DXDataRow'
    
    rowdata_class='dxgv' #used to get column data in a row


    rowcount=1    
    for etf in etfs:
        print(etf)
        try:
            browser.get(fund_holdings_url+etf) #1
            time.sleep(2)

            try:        
                fundholdings_tab=browser.find_element_by_id(fundholdings_id)
                fundholdings_tab.click()#2
                time.sleep(1.5)
            except:
                fundholdings_tab=browser.find_element_by_id(ative_fundholdings_id)
                fundholdings_tab.click()#2
                time.sleep(1.5) 

            soup2=BeautifulSoup(browser.page_source,'lxml') #3
            maintable_soup=soup2.find(id=maintable_id)#4

            number_of_rows=len(maintable_soup.find_all(id=re.compile('^'+row_id)))
            print(number_of_rows)

            if number_of_rows<5:
                n=len(maintable_soup.find_all(id=re.compile('^'+row_id)))
            else:
                n=5
            
            rowdata=[]
            for i in range(n):
                row=maintable_soup.find(id=row_id+str(i)) #5
                
                rowdata=[row.find_all('td',{'class':rowdata_class})[j] for j in (0,2)] #6 [x:y:z] start -x end-y step -z
                
                for i in range(len(rowdata)):
                    try:
                        s2.write(rowcount,i,rowdata[i].text)
                    except:
                        print(etf)
                        s2.write(rowcount,i,"")
                rowcount+=1
        except Exception as e:
            print(e)
            
            
        
    wb.save('XTF fundholdings.xls')           


url="http://www.xtf.com/ETF-Explorer"
driverpath='D:\Software Center\ARPAN SOFTWARES\chromedriver\chromedriver'


browser=launchurl(url,driverpath)
downloadFundHoldings(browser)

browser.close()




