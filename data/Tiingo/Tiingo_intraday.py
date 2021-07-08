import logging
import requests
from structjour.stock.utilities import ManageKeys, dictDate2NYTime, movingAverage, excludeAfterHours
from structjour.stock.mystockapi import StockApi
import pandas as pd


def getApiKey():
    mk = ManageKeys()
    key = mk.getKey('tgo')
    return key


KEY = getApiKey()
docs = 'https://api.tiingo.com/documentation/general/overview'
developers = 'https://api.tiingo.com/documentation/appendix/developers'

HEADERS = {'Content-Type': 'application/json', 'Authorization' : 'Token '+ KEY}


def getLimits():
    s = '''
    Limits for free accounts are more than generous enough in terms of numbers
    to handle the needs of Daly Journaling for a typical day trader.
    Unique Symbols per Monthy: 500
    Max Requests per Hour: 500
    MaX Requests per day 20K
    Pre market starts at 8:00am. Maybe should create a rule in apichooser
    Claim data since 1962. 
    '''

# requestResponse = requests.get("https://api.tiingo.com/api/test/", headers=HEADERS)
# print(requestResponse.json())

# Endpoints
# Meta Data
ticker = "AAPL"
TGO_URL_METADATA = "https://api.tiingo.com/tiingo/daily/{ticker}"
TGO_URL_LATESTPRICE ="https://api.tiingo.com/tiingo/daily/{ticker}/prices"
TGO_URL_HISTPRICE = "https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={sd}&endDate={ed}"
TGO_URL_INTRADAY = "https://api.tiingo.com/iex/{ticker}/prices?startDate={sd}&resampleFreq={interval}&columns={cols}"
TGO_URL_INTRADAY0 = "https://api.tiingo.com/iex/{ticker}/prices?"

class Tingo_REST(StockApi):

    def getLimits(self):
        return  ('500 calls per hour, 20,000 calls per month, 500 unique symbols per month.\n'
                 'Retail day traders are unlikely to hit the limit unless they use HF algos.\n\n')

    def getMetadata(self, ticker):
        md = TGO_URL_METADATA.format(ticker=ticker)
        r = requests.get(md, headers=HEADERS)
        if r.status_code != 200:
            return {"code": r.status_code, "message": r.text}
        return r.json()

    def getLatestprice(self, ticker):
        lp = TGO_URL_LATESTPRICE.format(ticker=ticker)
        r = requests.get(lp, headers=HEADERS)
        if r.status_code != 200:
            return {"code": r.status_code, "message": r.text}
        return r.json()

    def getHistoricalDailyPrice(self, ticker, start, end):
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        hp = TGO_URL_HISTPRICE.format(ticker=ticker, sd=start.strftime("%Y-%m-%d"), ed=end.strftime("%Y-%m-%d"))
        r = requests.get(hp, headers=HEADERS)
        if r.status_code != 200:
            return {"code": r.status_code, "message": r.text}
        return r.json()

    # Note that his endpoint is processed differently by iex
    # Note the the docs say the token goes in the url but it works to keep it in the header like the other calls
    def getIntraday(self, ticker, start, end, resolution, showUrl=False, key=None):

        logging.info('======= Called Tiingo -- no practical limit, 500/hour =======')
        
        # hd = TGO_URL_INTRADAY.format(ticker=ticker, sd='2019-01-02', interval="1min", cols="date,close,high,low,open,volume")
        hd = TGO_URL_INTRADAY0.format(ticker=ticker)
        header = {'Content-Type': 'application/json'}

        start = pd.Timestamp(start)
        startsent = start - pd.Timedelta(days=14)
        end = pd.Timestamp(end)
        if resolution < 60:
            resolution = str(resolution) + 'min'
        else:
            resolution = (resolution // 60) + 1
            resolution = str(resolution) + "hour"

        params = {}
        params['startDate'] =startsent.strftime('%Y-%m-%d')
        # params['endDate'] =end.strftime('%Y-%m-%d')
        params['resampleFreq'] = resolution
        params['afterHours'] = 'false' if excludeAfterHours() else 'true'
        params['forceFill'] = 'true'
        params['format'] = 'json'
        # params['token'] = KEY
        params['columns'] = "date,open,high,low,close,volume"


        r = requests.get(hd, params=params, headers=HEADERS)

        meta = {'code': r.status_code}
        if r.status_code != 200:
            meta['message'] = r.content
            return meta, pd.DataFrame(), None
        r = r.json()
        if len(r) == 0:
            meta['code'] = 666
            logging.error('Error: Tiingo returned no data')
            return meta, pd.DataFrame(), None
        
        r = dictDate2NYTime(r)
        df = self.getdf(r)
        maDict = movingAverage(df.close, df, start)
        meta, df, maDict = self.trimit(df, maDict, start, end, meta)

        return meta, df, maDict

    def getdf(self, j):
        df = pd.DataFrame(data=j)
        df.set_index('date', inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]
        return df

def get_intraday(symbol, start=None, end=None, minutes=5, showUrl=False, key=None):
    ''' Have to use this function till all the apis are objects'''
    t = Tingo_REST()
    return t.getIntraday(symbol, start, end, minutes, showUrl, key)


def dometa():
    t = Tingo_REST()
    meta, result = t.getMetadata('SQ')
    if not result:
        print(meta)
    else:
        print (result)

def dolatest():
    t = Tingo_REST()
    result = t.getLatestprice('PTON')
    print(result)

def dohd():
    t = Tingo_REST()
    result = t.getHistoricalDailyPrice('AAPL', '2012-1-1', '2016-1-1')
    print(result)

def dohi():
    t = Tingo_REST()
    meta, df, madDict = t.getIntraday('AAPL', '2020-11-15 06:30:00', '2020-12-01 12:35:00', 15)
    print(meta)
    print(df.index)

if __name__ == '__main__':
    # dometa()
    # dolatest()
    # print(dohd())
    print(dohi())