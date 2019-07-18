import requests
from bs4 import BeautifulSoup
import csv
import pdb
import datetime
#pdb.set_trace() - python step by step debugger command
print datetime.datetime.now()
print "Finviz Performance Start"
url = "http://www.finviz.com/screener.ashx?v=141&f=geo_usa"
response = requests.get(url)
html = response.content
soup = BeautifulSoup(html)
firstcount = soup.find_all('option')
lastnum = len(firstcount) - 1
lastpagenum = firstcount[lastnum].attrs['value']
currentpage = int(lastpagenum)

alldata = []
templist = []
# Overview = 111, Valuation = 121, Financial = 161, Ownership = 131, Performance = 141
#pagesarray = [111,121,161,131,141]
titleslist = soup.find_all('td',{"class" : "table-top"})
titleslisttickerid = soup.find_all('td',{"class" : "table-top-s"})
titleticker = titleslisttickerid[0].text
titlesarray = []
for title in titleslist:
    titlesarray.append(title.text)

titlesarray.insert(1,titleticker)
i = 0
currentpage = 21
while(currentpage > 0):
    i += 1
    print str(i) + " page(s) done"
    secondurl = "http://www.finviz.com/screener.ashx?v=" + str(141) + "&f=geo_usa" + "&r=" + str(currentpage)
    secondresponse = requests.get(secondurl)
    secondhtml = secondresponse.content
    secondsoup = BeautifulSoup(secondhtml)
    stockdata = secondsoup.find_all('a', {"class" : "screener-link"})
    stockticker = secondsoup.find_all('a', {"class" : "screener-link-primary"})
    datalength = len(stockdata)
    tickerdatalength = len(stockticker)

    while(datalength > 0):
        templist = [stockdata[datalength - 15].text,stockticker[tickerdatalength-1].text,stockdata[datalength - 14].text,stockdata[datalength - 13].text,stockdata[datalength - 12].text,stockdata[datalength - 11].text,stockdata[datalength - 10].text,stockdata[datalength - 9].text,stockdata[datalength - 8].text,stockdata[datalength - 7].text,stockdata[datalength - 6].text,stockdata[datalength - 5].text,stockdata[datalength - 4].text,stockdata[datalength - 3].text,stockdata[datalength - 2].text,stockdata[datalength - 1].text,]
        alldata.append(templist)
        templist = []
        datalength -= 15
        tickerdatalength -= 1
    currentpage -= 20

with open('stockownership.csv', 'wb') as csvfile:
    ownership = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=titlesarray)
    ownership.writeheader()

    for stock in alldata:
        ownership.writerow({titlesarray[0] : stock[0], titlesarray[1] : stock[1],titlesarray[2] : stock[2],titlesarray[3] : stock[3],titlesarray[4] : stock[4], titlesarray[5] : stock[5], titlesarray[6] : stock[6], titlesarray[7] : stock[7] , titlesarray[8] : stock[8], titlesarray[9] : stock[9], titlesarray[10] : stock[10],titlesarray[11] : stock[11],titlesarray[12] : stock[12],titlesarray[13] : stock[13],titlesarray[14] : stock[14],titlesarray[15] : stock[15] })

print datetime.datetime.now()
print "Finviz Ownership Completed"
