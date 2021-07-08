from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as bs
import requests
import urllib3
import re
import pandas as pd

fundList=["BLOK","DALT","AESR"]
etfSymbol="BLOK"
for etfSymbol in fundList:
    url_request = Request("https://www.etf.com/" + etfSymbol, headers = {"User-Agent" : "mozilla/5.0"})
    page = urlopen(url_request).read()
    text_data = []
    soup = BeautifulSoup(page, "html.parser")
    etfDescr = soup.find('p', class_="pull-left mb30")
    print(etfDescr.get_text())



    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    }
    url_request = Request("https://www.etf.com/" + etfSymbol, headers = {"User-Agent" : "mozilla/5.0"})
    res = requests.get("https://www.etf.com/" + etfSymbol, headers=headers)






import bs4
import requests

def getAmazonPrice(productUrl):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    }
    res = requests.get(productUrl, headers=headers)
    res.raise_for_status()
    soup = bs(res.text, 'html.parser')
    elems = soup.select('#newOfferAccordionRow .header-price')
    return elems[0].text.strip()


price = getAmazonPrice('http://www.amazon.com/Automate-Boring-Stuff-Python-Programming/dp/1593275994/ref=tmm_pap_swatch_0?_encoding=UTF8&amp;qid=&amp;sr=')
print('The price is ' + price)






df = pd.read_html(requests.get('https://www.etf.com/' + etfSymbol,headers={'User-agent': 'Mozilla/5.0'}).text)

r = requests.get('https://www.etf.com/' + etfSymbol, 
                headers={'Referer': 'http://www.etf.com/etfanalytics/etf-finder'})
data = r.json()

r = pd.read_html("https://www.etf.com/" + etfSymbol)
for ticker in r[1].iloc[:, 2].tolist():
    tickers.append(ticker)



    #extract etfName contents (etfTicker & etfLongName)
    etfInsight = etfName.contents[0]
    etfLongName = etfName.contents[1]
    etfTicker = str(etfTicker)
    etfLongName = etfLongName.text
    etfLongName = str(etfLongName)




### -------------------------------------------------
fundList=["VTIAX","PTTRX","PRFDX"]
for x in fundList:
    print(x)

x=0
while x < 3 do:
    print('Problem solving in teams')
    x+=1

i = 1
while i < 6:
    print(i)
    i += 1

for i in range(3):
    print('Problem solving in teams')

baseURL1 = "https://www.etf.com/"
baseURL2 = "http://www.maxfunds.com/funds/data.php?ticker="
baseURL3 = "http://www.marketwatch.com/investing/Fund/"


for etfSymbol in fundList:
    website = urllib2.urlopen(baseURL1 + etfSymbol)
    sourceCode = website.read()
    soup = BeautifulSoup(sourceCode)

    #parse document to find etf name 
    etfName = soup.find('h1', class_="etf")
    #extract etfName contents (etfTicker & etfLongName)
    etfTicker = etfName.contents[0]
    etfLongName = etfName.contents[1]
    etfTicker = str(etfTicker)
    etfLongName = etfLongName.text
    etfLongName = str(etfLongName)

    #get the time stamp for the data scraped 
    etfInfoTimeStamp = soup.find('div', class_="footNote")
    dataTimeStamp = etfInfoTimeStamp.contents[1]
    formatedTimeStamp =  'As of ' + dataTimeStamp.text
    formatedTimeStamp = str(formatedTimeStamp)

    #create vars 
    etfScores = []
    cleanEtfScoreList = []
    #parse document to find all divs with the class score
    etfScores = soup.find_all('div', class_="score")
    #loop through etfScores to clean them and add them to the cleanedEtfScoreList
    for etfScore in etfScores:
        strippedEtfScore = etfScore.string.extract()
        strippedEtfScore = str(strippedEtfScore)
        cleanEtfScoreList.append(strippedEtfScore)
    #turn cleanedEtfScoreList into a dictionary for easier access
    
    ETFInfoToWrite = [etfTicker, etfLongName, formatedTimeStamp, int(cleanEtfScoreList[0]), int(cleanEtfScoreList[1]), int(cleanEtfScoreList[2])]

#---------------------------------------------------

for etfSymbol in fundList:
    website = urllib2.urlopen(baseURL2 + etfSymbol)
    sourceCode = website.read()
    soup = BeautifulSoup(sourceCode)

    #get ETFs name
    etfName = self.soup.find('div', class_="dataTop")
    etfName = self.soup.find('h2')
    etfName = str(etfName.text)
    endIndex = etfName.find('(')
    endIndex = int(endIndex)
    fullEtfName = etfName[0:endIndex]
    startIndex = endIndex + 1
    startIndex = int(startIndex)
    lastIndex = etfName.find(')')
    lastIndex = int(lastIndex)
    lastIndex = lastIndex - 1
    tickerSymbol = etfName[startIndex: lastIndex]
    #get ETFs Max rating score
    etfMaxRating = self.soup.find('span', class_="maxrating")
    etfMaxRating = str(etfMaxRating.text)

    #create array to store name and rating 
    ETFInfoToWrite = [fullEtfName, tickerSymbol, int(etfMaxRating)]

#---------------------------------------------------

for etfSymbol in fundList:
    website = urllib2.urlopen(baseURL3 + etfSymbol)
    sourceCode = website.read()
    soup = BeautifulSoup(sourceCode)

#get etf Name
    etfName = self.soup.find('h1', id="instrumentname")
    etfName = str(etfName.text)
    #get etf Ticker
    etfTicker = self.soup.find('p', id="instrumentticker")
    etfTicker = str(etfTicker.text)
    etfTicker = etfTicker.strip()

    self.ETFInfoToWrite.append(etfName)
    self.ETFInfoToWrite.append(etfTicker)

    #get Lipper scores ***NEEDS REFACTORING***
    lipperScores = self.soup.find('div', 'lipperleader')
    lipperScores = str(lipperScores)
    lipperScores = lipperScores.split('/>')
    for lipperScore in lipperScores:
        startIndex = lipperScore.find('alt="')
        startIndex = int(startIndex)
        endIndex = lipperScore.find('src="')
        endIndex = int(endIndex)
        lipperScore = lipperScore[startIndex:endIndex]
        startIndex2 = lipperScore.find('="')
        startIndex2 = startIndex2 + 2
        endIndex2 = lipperScore.find('" ')
        lipperScore = lipperScore[startIndex2:endIndex2]
        seperatorIndex = lipperScore.find(':')
        endIndex3 = seperatorIndex
        startIndex3 = seperatorIndex + 1

        lipperScoreNumber = lipperScore[startIndex3:]
        if lipperScoreNumber == '' and lipperScoreNumber == '':
            pass
        else:
            self.ETFInfoToWrite.append(int(lipperScoreNumber))    

