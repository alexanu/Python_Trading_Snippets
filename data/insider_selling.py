
# Source: https://github.com/shea12underwood/InsiderSellsVideo


import pandas as pd
import pandas_datareader as dr
from datetime import datetime,date,timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys



def getSells():
    startTime = datetime.now()
    df = pd.read_html("https://www.insidearbitrage.com/insider-sales/?desk=yes")
    df = df[2]
    columns = df.iloc[0]
    df.columns = columns
    df.drop(df.columns[0],axis=1,inplace=True) #axis 1 chooses the columns
    df = df[1:]
    df.to_csv('Insider.csv')
    print(f'CSV File Created - Execution Time: {datetime.now() - startTime}')


startTime = datetime.now()

chrome_path = "C:\Videospace\chromedriver.exe"
chrome_options = Options()  
chrome_options.add_argument("headless") 
driver = webdriver.Chrome(chrome_path,options=chrome_options,keep_alive=False)
# driver = webdriver.Chrome(chrome_path)
url = "https://www.insidearbitrage.com/insider-sales/?desk=yes"
driver.get(url)

tickerlist=[]
relationshiplist=[]
datelist=[]
costlist=[]
shareslist=[]
sharesheldlist=[]

for i in range(2,102):
    tickerpath = f"""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[{i}]/td[2]"""
    ticker = driver.find_element_by_xpath(tickerpath)
    tickerlist.append(ticker.text)

    relationshippath = f"""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[{i}]/td[4]"""
    relationship = driver.find_element_by_xpath(relationshippath)
    relationshiplist.append(relationship.text)

    datepath = f"""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[{i}]/td[5]"""
    date = driver.find_element_by_xpath(datepath)
    datelist.append(date.text)

    costpath = f"""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[{i}]/td[6]"""
    cost = driver.find_element_by_xpath(costpath)
    costlist.append(cost.text)

    sharespath = f"""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[{i}]/td[7]"""
    shares = driver.find_element_by_xpath(sharespath)
    shareslist.append(shares.text)

    sharesheldpath = f"""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[{i}]/td[9]"""
    sharesheld = driver.find_element_by_xpath(sharesheldpath)
    sharesheldlist.append(sharesheld.text)

allinfo = list(
        zip(tickerlist, relationshiplist, datelist, costlist, shareslist, sharesheldlist)
    )

df = pd.DataFrame(
        allinfo,
        columns=["Ticker", "Position", "Date", "Share Cost", "Shares Bought", "Shares Held"]
    )

df.to_csv('Insider.csv',index=False)

print(f"""Execution Time: {datetime.now() - startTime}""")




startTime = datetime.now()

# getSells() #uncomment to refresh the csv file
df = pd.read_csv('Insider.csv',index_col = 0)

infoDict = {}
numperiods = 180

def getinfo(ticker,n):
    try:
        tickerdf = dr.data.get_data_yahoo(ticker,start = date.today() - timedelta(300) , end = date.today())
        currentprice = tickerdf.iloc[-1]['Close']
        MA = pd.Series(tickerdf['Close'].rolling(n, min_periods=0).mean(), name='MA')
        currentma = MA[-1]
        return (currentprice,currentma)
    except:
        return ('na','na')
    
def getPrice(row):
    ticker = row['Symbol']
    if ticker not in infoDict.keys():
        tickerinfo = getinfo(ticker,numperiods)
        infoDict[ticker] = {}
        infoDict[ticker]["price"] = tickerinfo[0]
        infoDict[ticker]["ma"] = tickerinfo[1]
        return infoDict[ticker]["price"]
    else:
        return infoDict[ticker]["price"]

def getMovingAverage(row):
    ticker = row['Symbol']
    return infoDict[ticker]["ma"]

df['currentprice'] = df.apply (lambda row: getPrice(row), axis=1)
df['movingaverage'] = df.apply (lambda row: getMovingAverage(row), axis=1)
df.to_csv('InsiderPrices.csv')

print(f'Execution Time: {datetime.now() - startTime}')



def getinsiders(numpages):
    stocklist = []
    insiderlist = []
    costlist = []
    pricelist = []
    sharepricelist = []
    datelist = []
    chrome_path = r"C:\Users\u15866\OneDrive - Kimberly-Clark\chromedriver.exe"
    driver = webdriver.Chrome(chrome_path)
    URL = "https://www.gurufocus.com/insider/summary"

    driver.get(URL)

    counter = 0
    while counter < numpages:

        tickernames = driver.find_elements_by_class_name("table-stock-info")
        for ticker in tickernames:
            stocklist.append(ticker.text)

        insiderpositions = driver.find_elements_by_class_name("table-position-info")
        for insider in insiderpositions:
            insiderlist.append(insider.text)

        for i in range(1, 41):
            cost = driver.find_element_by_xpath(
                """//*[@id="wrapper"]/div/table/tbody/tr[""" + str(i) + """]/td[12]"""
            )
            costlist.append(cost.text)

        for i in range(1, 41):
            price = driver.find_element_by_xpath(
                """//*[@id="wrapper"]/div/table/tbody/tr["""
                + str(i)
                + """]/td[4]/span"""
            )
            pricelist.append(price.text)

        for i in range(1, 41):
            shareprice = driver.find_element_by_xpath(
                """//*[@id="wrapper"]/div/table/tbody/tr[""" + str(i) + """]/td[11]"""
            )
            sharepricelist.append(shareprice.text)

        purchasedates = driver.find_elements_by_class_name("table-date-info")
        for date in purchasedates:
            datelist.append(date.text)

        counter += 1
        nextbutton = driver.find_element_by_xpath(
            """//*[@id="components-root"]/div/section/main/div[7]/div/button[2]/i"""
        )
        nextbutton.click()

    driver.close()
    intrades = list(
        zip(stocklist, insiderlist, costlist, pricelist, sharepricelist, datelist)
    )
    #    print(intrades)

    df = pd.DataFrame(
        intrades,
        columns=["ticker", "position", "position size", "price", "shareprice", "date"],
    )

    for index, row in df.iterrows():
        if "C" in row["position"]:
            psize = row["position size"]
            psize = psize.replace(",", "")
            if float(psize) > 100_000:
                print(row["ticker"], end=": ")
                print(
                    "On " + row["date"] + ", The " + row["position"]
                    + " spent " + psize + " at a price of " + row["shareprice"] + " per share. "
                    + row["ticker"]  + " is now trading at " + row["price"] + " per share."
                )
                print("\n")

def getBuys(numpages=1):
    stocklist = []
    positionlist = []
    valuelist = []
    costlist = []
    datelist= []
    chrome_path = r"C:\Users\u15866\OneDrive - Kimberly-Clark\chromedriver.exe"
    driver = webdriver.Chrome(chrome_path)
    URL = "https://www.insidearbitrage.com/insider-buying/"
    driver.get(URL)
    

    counter = 0
    while counter < numpages:

        if counter != 0:
            nextbutton = driver.find_element_by_xpath("""//*[@id="sortTableM"]/div[2]/table[3]/tbody/tr/td/a[1]""")
            nextbutton.click()

        for i in range(2,102):
            position = driver.find_element_by_xpath("""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[""" + str(i) + """]/td[4]""")
            if "C" in position.text:
                pass
            else:
                continue
            positionlist.append(position.text)

            ticker = driver.find_element_by_xpath("""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[""" + str(i) + """]/td[2]/a""")
            stocklist.append(ticker.text)

            value = driver.find_element_by_xpath("""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[""" + str(i) + """]/td[8]""")
            valuelist.append(value.text)

            cost = driver.find_element_by_xpath("""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[""" + str(i) + """]/td[6]""")
            costlist.append(cost.text)

            date = driver.find_element_by_xpath("""//*[@id="sortTableM"]/div[2]/table[2]/tbody/tr[""" + str(i) + """]/td[5]/span""")
            datelist.append(date.text)
        
        counter +=1

    driver.close()

    finallist = list(zip(stocklist,positionlist,valuelist,costlist,datelist))

    df = pd.DataFrame(finallist, columns=["ticker", "position", "totalcost", "shareprice","date"])

    returnlist=[]
    for index, row in df.iterrows():
        if "C" in row["position"]:
            psize = row["totalcost"]
            psize = psize.replace(",", "")
            if float(psize) > 100_000:
                print(row['ticker'] +": On " + row['date'] + ", The " + row['position'] +" bought " + row['totalcost'] + " worth of shares at a price of " +row['shareprice'] + " per share.")
                returnlist.append(row['ticker'])
    return returnlist
    

if __name__ == "__main__":
    getBuys()