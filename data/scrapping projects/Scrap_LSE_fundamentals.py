# https://github.com/mcatan01/python-web-scraping-LSE-fundamentals/blob/master/code_data_collect.py



from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import time
import numpy as np
import pandas as pd
from currency_converter import CurrencyConverter


rate = CurrencyConverter('http://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip')

print('\n')
print('Software inizialised...Please wait...')
print('\n')
path = 'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/constituents-indices.html'

links_pages = list()
r = range(1,78)
for x in r:
    path = 'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/constituents-indices.html?&page=' + str(x)
    links_pages.append(path)



#fourWayKey
keys = list()

for link in links_pages:
    req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req)
    soup = BeautifulSoup(webpage, 'html.parser')
    t = soup.select('a[title="View detailed prices page"]')
    for link_2 in t:
        link_2 = str(link_2)
        id = link_2[69:90]
        keys.append(id)
print(str(len(keys)) + ' companies found...')
print('\n')
print('Stocks keys obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')

#Get main page link for each stocks
main_links = list()
for key in keys:
    stock_main_link = 'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/summary/company-summary/' + key + '.html'
    main_links.append(stock_main_link)
print('Main page links for each stocks obtained...')
print('\n')


#Get fundamentals page link for each stocks
funda_links = list()
for key in keys:
    stock_funda_link = 'http://www.londonstockexchange.com/exchange/prices/stocks/summary/fundamentals.html?fourWayKey=' + key
    funda_links.append(stock_funda_link)
print('Fundamentals page links for each stocks obtained...')
print('\n')

#req = Request(stock_main_link, headers={'User-Agent': 'Mozilla/5.0'})
#webpage = urlopen(req)
#soup = BeautifulSoup(webpage, 'html.parser')


#Get company name (from soup of key loop)
names = list()
count = 0
for link in main_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        name = str(soup.find("h1", class_="tesummary"))
        name = name.split('</a>')
        name = name[1].split('</')
        name = name[0].strip("\r\n")
        name = name.split("\r\n")
        name = name[0]
        names.append(name)
        count = count + 1
        print('Company ' + str(count) + ' : done! ' + 'Result: ' + name)
    except:
        count = count + 1
        name = np.nan
        names.append(name)
        print('Company ' + str(count) + ' failed. ' + link)
print('Companies names obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')

#Get Sector [-29]
sector = list()
count = 0
for link in main_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        tbody = soup.find('body')
        page = tbody.find_all('td')
        sec = page[-29]
        sec = str(sec)
        sec = sec.split('">')
        sec = sec[1]
        sec = sec.split('<')
        sec = sec[0]
        sector.append(sec)
        count = count + 1
        print('Company ' + str(count) + ' : done! ' + 'Result: ' + sec)
    except:
        count = count + 1
        sec = np.nan
        sector.append(sec)
        print('Company ' + str(count) + ' failed. ' + link)
print('Sectors obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')


#Get Sub-sector [-27]
sub_sector = list()
count = 0
for link in main_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        tbody = soup.find('body')
        page = tbody.find_all('td')
        sub = page[-27]
        sub = str(sub)
        sub = sub.split('">')
        sub = sub[1]
        sub = sub.split('<')
        sub = sub[0]
        sub_sector.append(sub)
        count = count + 1
        print('Company ' + str(count) + ' : done! ' + 'Result: ' + sub)
    except:
        count = count + 1
        sub = np.nan
        sub_sector.append(sub)
        print('Company ' + str(count) + ' failed. ' + link)
print('Sub-Sectors obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')


#Get share price currency and share price
currency = list()
shares_prices = list()
count = 0
for link in main_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        tbody = soup.find('tbody')
        price_main = tbody.find_all('td')
        price_main = str(price_main)
        c = price_main.split(', ')
        c = c[0]
        c = c.split('(')
        c = c[1].replace(")</td>", "")
        currency.append(c)
        price = price_main.split(', ')
        price = price[1]
        price = price.replace("<td>", "")
        price = price.replace("</td>", "")
        price = price.replace(",", "")
        shares_prices.append(float(price))
        count = count + 1
        print('Company ' + str(count) + ' : done! ' + 'Result: ' + c + ' ' + price)
    except:
        count = count + 1
        c = np.nan
        currency.append(c)
        price = np.nan
        shares_prices.append(price)
        print('Company ' + str(count) + ' failed. ' + link)
print('Current Share price and Share Price currency obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')

#Get market cap value
market_cap = list()
count = 0
for link in main_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        tbody = soup.find('body')
        cap = tbody.find_all('td')
        cap =  cap[-1]
        cap = str(cap)
        cap = cap.split('">')
        cap = cap[1]
        cap = cap.split('<')
        capital = cap[0]
        capital = capital.replace(",", "")
        market_cap.append(float(capital))
        count = count + 1
        print('Company ' + str(count) + ' : done! ' + 'Result = ' + capital)
    except:
        count = count + 1
        capital = np.nan
        market_cap.append(capital)
        print('Company ' + str(count) + ' failed. ' + link)
print('Companies market cap obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')



#Get net asset value per share (pence)
companies_navps = list()
count = 0
for link in funda_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        x = str(soup.find_all("td", class_=""))
        x = x.split(', ')
        netAsset = x[-7]
        netAsset = netAsset.split('\n')
        netAsset = netAsset[1]
        if 'p' in netAsset:
            netAsset = netAsset.split('p')
            netAsset = netAsset[0]
            netAsset = netAsset.replace(",", "")
            netAsset = netAsset.strip()
            netAsset = float(netAsset)
            conv_currency = 'p'
        elif '¢' in netAsset:
            netAsset = netAsset.split('¢')
            netAsset = netAsset[0]
            netAsset = netAsset.replace(",", "")
            netAsset = float(netAsset)
            netAsset = rate.convert((netAsset/100),'USD', 'GBP',)
            netAsset = netAsset*100
            conv_currency = '$'
        elif 'c' in netAsset:
            netAsset = netAsset.split('c')
            netAsset = netAsset[0]
            netAsset = netAsset.replace(",", "")
            netAsset = float(netAsset)
            netAsset = rate.convert((netAsset/100),'EUR', 'GBP',)
            netAsset = netAsset*100
            conv_currency = '€'
        else:
            netAsset = np.nan   
        companies_navps.append(float('{0:.2f}'.format(netAsset)))
        count = count + 1
        print('company ' + str(count) + ' : done! ' + 'Result= ' + str(netAsset) + ' ' + conv_currency)
    except:
        count = count + 1
        netAsset = np.nan
        companies_navps.append(netAsset)
        print('Company ' + str(count) + ' failed. ' + link)
print('Companies net asset value per share obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')

#Get last statement year
stat_year = list()
count = 0
for link in funda_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        x = soup.find_all("th")
        x = x[-5]
        x = str(x).split(' ')
        x = x[0]
        x = x.split('-')
        x = x[2]
        x = '20' + x
        x = int(x)
        stat_year.append(x)
        count = count + 1
        print('company ' + str(count) + ' : done! ' + 'Result: ' + str(x))
    except:
        count = count + 1
        x = np.nan
        stat_year.append(x)
        print('Company ' + str(count) + ' failed. ' + link)
print('Companies last statement year obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')

#Getting Gearing % (from soup webpage fundamentals)
companies_gearing = list()
count = 0
for link in funda_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        x = str(soup.find_all("td", class_=""))
        x = x.split(', ')
        g = x[-1]
        g = g.split('\n')
        g = g[1]
        g = g.split('%')
        g = g[0]
        g = g.rstrip()
        if ',' in g:
            g = g.replace(",", "")
        companies_gearing.append(float(g))
        count = count + 1
        print('company ' + str(count) + ' : done! ' + 'Result = ' + g)
    except:
        count = count + 1
        g = np.nan
        companies_gearing.append(g)
        print('Company ' + str(count) + ' failed. ' + link)
print('Companies gearings obtained...')
print('\n')
print('Sleep...')
time.sleep(60)
print('Wake up...')
print('\n')


#Get dividend yield
companies_yield = list()
count = 0
for link in funda_links:
    try:
        req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req)
        soup = BeautifulSoup(webpage, 'html.parser')
        x = str(soup.find_all("td", class_=""))
        x = x.split(', ')
        div = x[-19]
        div = div.split('="">')
        div = div[1].split('<')
        div = div[0].strip('%')
        div = div.strip()
        if ',' in div:
            div = div.replace(",", "")
        companies_yield.append(float(div))
        count = count + 1
        print('company ' + str(count) + ' : done! ' + 'Result = ' + div)
    except:
        count = count + 1
        div = np.nan
        companies_yield.append(div)
        print('Company ' + str(count) + ' failed. ' + link)
print('Companies dividend yields obtained...')
print('\n')


print('Building Data Frame...')
print('\n')
d = {'Sector': sector, 'Subsector': sub_sector, 'Share Price Currency': currency, 'Current Price per Share (pence)' : shares_prices, 'Market Cap (£ m)': market_cap, 'Net Asset Value per Share (pence)': companies_navps,'Last Statement Year': stat_year, 'Gearing (%)' : companies_gearing, 'Dividend Yield (%)' : companies_yield}
frame = pd.DataFrame(d, index=names)

frame.to_csv('original_table.csv')

print('Data Frame exported')