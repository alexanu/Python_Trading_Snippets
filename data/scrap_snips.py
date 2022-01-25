from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from datetime import datetime, timedelta
import requests
import urllib3
import re
import pandas_datareader.data as web
from pandas.tseries.offsets import BDay

# Scrap wikipedia for index constituents
    tickers=[]
    r = pd.read_html('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')
    for ticker in r[1].iloc[:, 2].tolist():
        tickers.append(ticker)

    r = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    for ticker in r[0].iloc[:, 0].tolist():
        tickers.append(ticker)

    r = pd.read_html('https://it.wikipedia.org/wiki/DAX_30')[1]
    for ticker in pd.DataFrame(r)[1][1:].tolist():
        tickers.append(ticker)

    r = pd.read_html('https://de.finance.yahoo.com/quote/%5ETECDAX/components?p=%5ETECDAX')
    for ticker in r[1]['Symbol'].tolist():
        tickers.append(ticker)

    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)


    # financials , industrials , energy ,health_care, information_technology, consumer_staples
    hdr = {'User-Agent': 'Mozilla/5.0'}
    # req = urllib2.Request(site, headers=hdr)
    page = urllib.request.urlopen("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(page, features='lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)


    print(promt_time_stamp() + 'load nikkei tickers ..')
    resp = requests.get('https://www.bloomberg.com/quote/NKY:IND/members')
    html = bs.BeautifulSoup(resp.text)
    html_list = str(html).split('security-summary__ticker')
    for h in html_list:
        try:
            e = h.split(':JP">')[1].split(':JP</a>')[0]
            tickers.append(e)
        except IndexError as e:
            pass


    print(promt_time_stamp() + 'load hangseng tickers ..')
    resp = requests.get('https://en.wikipedia.org/wiki/Hang_Seng_Index')
    html = bs.BeautifulSoup(resp.text)
    html_list = str(html).split('<a href="/wiki')
    for h in html_list:
        try:
            s = h.split('<li>')[1]
            x = int(s)
            tickers.append(s)
        except (IndexError, ValueError) as e:
            pass

# historical dividends from tickertech.com
    for ticker in ticker_list:

        output_filename = "dividends\{0}.csv".format(ticker)     # set the output file to be `$ticker.csv` in the folder `dividends`
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)     # Check if the folder exists, if not make it
        output_file = open(output_filename, 'w')     # make/open the ticker file

        url = "https://www.tickertech.net/bnkinvest/cgi/?n=2&ticker=" + ticker + "&js=on&a=historical&w=dividends2"
        req = requests.get(url)
        soup = BeautifulSoup(req.content, "lxml")
        # find the one table we are looking for with dividend data, and get all table rows from that table
        dividend_rows = soup.find("table", attrs={"bgcolor": "#EEEEEE"}).find_all("tr")
        for row in dividend_rows:
            columns = list(row.stripped_strings) # extract all the strings from the row
            columns = [x for x in columns if 'allow' not in x] # remove the lingering javascript rows
            if len(columns) == 2: # if there are only 2 columns (date, ratio), ...
                output_file.write("{0}, {1} \n".format(columns[0], columns[1])) # ... the data is correct and we can write it
        output_file.close()

# 10-Ks from marketwatch
    for t in tcks:
        ten_k_end_reached = 0
        ten_q_end_reached = 0
        filing_page = 1
        while ten_k_end_reached == 0:
            url = 'https://www.marketwatch.com/investing/stock/' + t + '/secfilings?seqid=' + str(filing_page) + '&subview=10K'
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            table = soup.find('table', id = 'Table2')
            rows = table.find_all('tr')

            filing_dates = []
            for i in range(1, len(rows) - 1):
                row = rows[i]
                fdt = row.find_next('td').text.strip()
                if pd.to_datetime(fdt) <= start_date:
                    ten_k_end_reached = 1
                    break
                else:
                    filing_dates.append(fdt)
            temp = pd.DataFrame({'filing_dates':filing_dates, 'ticker':t})

            fd = pd.concat([fd, temp], axis = 0)
            filing_page += 20 #increment filings page

# premarket most active from nasdaq
    nasdaq_url = "http://www.nasdaq.com/extended-trading/premarket-mostactive.aspx"
    page = urlopen(nasdaq_url)

    text_data = []
    soup = BeautifulSoup(page, "html.parser")
    # search all html lines containing table data about stock 
    ##    html_data = soup.find_all('div', id="_advanced")
    ##    for row in html_data:
    ##        print("here")
    ##        print(row.get_text().rstrip())
    ##        text_data.append(row.get_text())

    div_data = soup.find_all('div', id="_advanced")
    tbody_data = []
    # find table data in html_data 
    for elem in div_data:
        tbody_data = elem.find_all('td')
    stocks = []        
    for stock in tbody_data:
        stocks.append(stock.get_text())

# short interest from Nasdaq
    symbols=['AAPL','DSX','MSFT']
    for symbol in symbols:
        url="https://www.nasdaq.com/market-activity/stocks/"+symbol+"/short-interest"
        # url_request = Request(url, headers = {"User-Agent" : "mozilla/5.0"})
        response=urlopen(url,timeout=10)
        html = response.read()
        data=pd.read_html(html)
    print('Done')


#######################################################################################################

from selenium import webdriver
from lxml import html
import pandas as pd
import time
import datetime

#get username and pw
user_in = input("enter username:\n")
pass_in = input("enter pw:\n")

browser = webdriver.Firefox() #replace with .Firefox(), or with the browser of your choice
time.sleep(3)

login = "https://www.etf.com/user/login"
browser.get(login) #navigate to the login page
time.sleep(3)
username = browser.find_elements_by_id("edit-name")
password = browser.find_elements_by_id("edit-pass")
username[0].send_keys(user_in) 
time.sleep(3)
password[0].send_keys(pass_in) 
time.sleep(3)
submitButton = browser.find_element_by_id("user_login") 
submitButton.click()
time.sleep(3)


##############################################################################################################


i = 0
data = {'ticker':[], 'descriptions':[], 'date':[], 'link':[]}

url = 'https://www.benzinga.com/author/lisa-levin?page=' + str(i)
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")
movers = soup.find_all('a', text = re.compile('Biggest Movers'))
print('%d movers links found on page %d...' %(len(movers),i))    
    for lk in movers:
        href = lk.get('href')
        url2 = 'https://www.benzinga.com' + href
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

df = pd.DataFrame(data)
df['date'] = pd.to_datetime(df.date) - BDay(1) #set to previous business day
df = df[['date', 'ticker', 'descriptions', 'link']]
df = df.drop_duplicates()  #drop duplicates

#################################################################################
def analyst_Ratings(symbol):
    url = 'https://www.benzinga.com/stock/' + symbol + '/ratings'
    df = pd.read_html(url)
    df2 = df[0].iloc[:, 0:4]
    return [df2['Action'].tolist()[0], df2['Current'].tolist()[0]]


#########################################################################

finviz_url_top_gainers = "http://finviz.com/screener.ashx?v=111&s=ta_topgainers&f=sh_curvol_o500,sh_price_1to20"
url_request = Request(finviz_url_top_gainers, headers = {"User-Agent" : "mozilla/5.0"})
page = urlopen(url_request).read()
text_data = []
soup = BeautifulSoup(page, "html.parser")


# search all html lines containing table data about stock 
html_data = soup.find_all('td', class_="screener-body-table-nw")
counter = 0 

for rw in html_data:
    print(rw.get_text())

for rw in html_data:
    counter+= 1
    text_data.append(rw.get_text())

# takes as input text_data array and outputs list of lists
# each list contains information about one stock
def helper(data):
    counter = 0 
    list_of_lists = []
    temp_list = []
    for elem in data:
        # end of each stock
        if counter % 11 == 0:
            # add currently filled temp_list to list_of_lists if not empty
            if temp_list: 
                list_of_lists.append(temp_list)
            # reset to empty list
            temp_list = []
        # change from string to int for % change
        if elem[-1] == '%':
            elem = float(elem[:-1])
        temp_list.append(elem)
        counter += 1
    list_of_lists.append(temp_list)
    return list_of_lists

stock_data = helper(text_data)

# remove the numerical index from each list
for each_stock in stock_data:
    del each_stock[0]
    
labels = ['Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Price', '% Change', 'Volume']
df = pd.DataFrame.from_records(stock_data, columns=labels)
# date of retrieval
print(str(datetime.now())) 
# time taken to retrieve data
print('Time taken to draw data: ' + str(round(time.time() - start_time, 2)) + ' seconds')
# save as csv file
df.to_csv('/Users/ZengHou/Desktop/Stock Strategies/finviz_data_strategy' + str(strategyNum) + '.csv', index=False)



#####################################

import urllib2

import pandas as pd
url="http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
nasd_tick=pd.DataFrame(requests.get(url).content)



# IPOs from Nasdaq
    url = 'https://api.nasdaq.com/api/ipo/calendar?date=2020-11'
    headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36"}
    r = requests.get(url, headers=headers)
    result=r.json()
    companies = result['data']['priced']['rows']
    df = pd.json_normalize(companies)

# ETF price from marketwatch
    r = requests.get("https://www.marketwatch.com/investing/fund/ivv")
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    if soup.h1.string == "Pardon Our Interruption...":
        print("They detected we are a bot. We hit a captcha.")
    else:
        price = soup.find("h3", class_="intraday__price").find("bg-quote").string
        print(price)


#############################################################################

r = requests.get('http://www.etf.com/etf-finder-funds-api//-aum/0/3000/1', 
                headers={'Referer': 'http://www.etf.com/etfanalytics/etf-finder'})
>>> data = r.json()


# Income statement from barchart.com
    url = "https://www.barchart.com/stocks/quotes/"+ticker+"/income-statement/annual"
    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find_all('table')[0]
        data = pd.read_html(table.prettify())
        data = data[0]
        data.iloc[0][0] = 'Year'