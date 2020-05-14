__author__= 'cgomezfandino@gmail.com'

import pandas as pd
import numpy as np
import websocket
import requests 
from configparser import ConfigParser
import json

## https://finnhub.io/docs/api
config = ConfigParser()
config.read(r'API_Connections/connections.cfg')

token=config['FinnHub']['access_token']


def get_CompanyProfile(symbol):
    """
    Return:
        address:  Address of company's headquarter.
        city: City of company's headquarter.
        country: Country of company's headquarter.
        currency: Currency used in company filings.
        cusip: CUSIP number. Only available for north america company
        description: Company business summary.
        exchange: Listed exchange.
        ggroup: GICS industry group.
        gind: GICS industry.
        gsector: GICS sector.
        gsubind: GICS sub-industry.
        isin: ISIN number.
        naics: NAICS classification.
        name: Company name.
        phone: phone number.
        state: State of company's headquarter.
        ticker: Company symbol/ticker as used on the listed exchange.
        weburl: Company website.
        ipo: IPO date
    """

    r = requests.get('https://finnhub.io/api/v1/stock/profile?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_CEOCompensation(symbol):
    """
        Return:
            symbol: Company symbol.
            name: CEO name
            companyName: Company name
            location: CEO location
            salary: CEO base salary
            bonus: CEO bonus
            stockAwards: Compensation in the form of stock.
            optionAwards: Compensation in the form of option.
            nonEquityIncentives: Non-equity compensation total.
            pensionAndDeferred: Pension and deferred compensation.
            otherComp: Other compensation.
            total: Total compensation.
            year:Fiscal year of the data.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/ceo-compensation?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_RecommendationTrends(symbol):
    """
        Return:
            symbol: Company symbol.
            buy: Number of recommendations that fall into the Buy category
            hold: Number of recommendations that fall into the Hold category
            period: Updated period
            sell: Number of recommendations that fall into the Sell category
            strongBuy: Number of recommendations that fall into the Strong Buy category
            strongSell: Number of recommendations that fall into the Strong Sell category
    """
    r = requests.get('https://finnhub.io/api/v1/stock/recommendation?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_PriceTarget(symbol):
    """
        Return:
            symbol: Company symbol.
            targetHigh: Highes analysts' target.
            targetLow: Lowest analysts' target.
            targetMean: Mean of all analysts' targets.
            targetMedian: Median of all analysts' targets.
            lastUpdated: Updated time of the data
    """
    r = requests.get('https://finnhub.io/api/v1/stock/price-target?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_Stock_UpgradeDowngrade(symbol):
    """
        Return:
            symbol: Company symbol.
            gradeTime: Upgrade/downgrade time in UNIX timestamp.
            fromGrade: From grade.
            toGrade: To grade.
            Company: Company/analyst who did the upgrade/downgrade.
            action: Action can take any of the following values: up(upgrade), down(downgrade), main(maintains), init(initiate), reit(reiterate).
    """
    r = requests.get('https://finnhub.io/api/v1/stock/upgrade-downgrade?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_OptionChain(symbol):
    """
        Return:
            code: Company symbol.
            exchange: US
            data: Array of option chain with different expiration date.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/option-chain?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_Peers(symbol):
    """
        Return:
            Array: Array of peers' symbol.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/peers?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_Earnings(symbol):
    """
        Return:
            actual: Actual earning result.
            estimate: Estimated earning.
            period: Reported period.
            symbol: Company symbol.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/earnings?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_StockExchanges(symbol):
    """
        Return:
            Array : Array of supported forex exchanges.
            name: Exchange name.
            code: Exchange code.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/exchange?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_StockSymbol(exchange = 'US'):
    """
        Examples:
            /stock/symbol?exchange=US
            /stock/symbol?exchange=L
            /stock/symbol?exchange=NS
        Return:
            Array: Array of supported stocks.
            description: Symbol description
            displaySymbol: Display symbol name.
            symbol: Unique symbol used to identify this symbol used in /stock/candle endpoint.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange={0}&token={1}'.format(exchange,token))
    ## print(r.json())
    return r.json()

def get_Quote(symbol):
    """
        Examples:
            /quote?symbol=AAPL
            /quote?symbol=SBIN.NS
        Return:
            o: Open price
            h: High price
            l: Low price
            c: Current price
            pc: Previous close price
    """
    r = requests.get('https://finnhub.io/api/v1/quote?symbol={0}&token={1}'.format(symbol,token))
    ## print(r.json())
    return r.json()

def get_StockCandles(symbol, resolution, start, end, format = None, count = None):
    """
        Examples:
            /stock/candle?symbol=AAPL&resolution=1&from=1572651390&to=1572910590
            /stock/candle?symbol=SBIN.NS&resolution=D&from=1572651390&to=1575243390
            /stock/candle?symbol=ABF.L&resolution=60&from=1572651390&to=1576452990
        Argument:
            symbol (REQUIRED): Symbol.
            resolution (REQUIRED): Supported resolution includes 1, 5, 15, 30, 60, D, W, M .Some timeframes might not be available depending on the exchange.
            from (optional): UNIX timestamp. Interval initial value. If count is not provided, this field is required
            to (optional): UNIX timestamp. Interval end value. If count is not provided, this field is required
            format (optional): By default, format=json. Strings json and csv are accepted.
            count (optional): Shortcut to set to=Unix.Now and from=Unix.Now - count * resolution_second. With small resolution such as 1, This option might result in no_data over the weekend.
        Return:
            o: List of open prices for returned candles.
            h: List of high prices for returned candles.
            l: List of low prices for returned candles.
            c: List of close prices for returned candles. 
            v: List of volume data for returned candles.
            t: List of timestamp for returned candles.
            s: Status of the response. This field can either be ok or no_data.
           
    """
    r = requests.get('https://finnhub.io/api/v1/stock/candle?symbol={0}&resolution={1}&from={2}&to={3}&token={4}'.format(symbol, resolution, start, end, token))
    ## print(r.json())
    return r.json()

def get_TickData(symbol,start, end):
    """
        Examples: 
            /stock/tick?symbol=AAPL&from=1575968404&to=1575968424
        Arguments:
            symbol (REQUIRED): Symbol.
            from (REQUIRED): UNIX timestamp. Interval initial value.
            to (REQUIRED): UNIX timestamp. Interval end value
        Return:
            s: Status of the response. This field can either be ok or no_data.
            trades: Array of trades.
            t: UNIX milliseconds timestamp.
            p: Price. 
            v: Volume of the trade.
    """
    r = requests.get('https://finnhub.io/api/v1/stock/tick?symbol={0}&from={1}&to={2}&token={3}'.format(symbol, start, end, token))

    ## print(r.json())
    return r.json()

def get_ForexExchanges():
    """
        Examples: 
            /forex/exchange
        Arguments:
            None:
        Return:
            Array: Array of supported forex exchanges.
    """
    r = requests.get('https://finnhub.io/api/v1/forex/exchange?&token={0}'.format(token))

    ## print(r.json())
    return r.json()

def get_ForexSymbol(exchange):
    """
        Examples: 
            /forex/symbol?exchange=oanda
        Arguments:
            exchange (REQUIRED): Exchange you want to get the list of symbols from.
        Return:
            Array: Array of supported forex symbols.
            description: Symbol description
            displaySymbol: Display symbol name.
            symbol: Unique symbol used to identify this symbol used in /forex/candle endpoint.
    """
    r = requests.get('https://finnhub.io/api/v1/forex/symbol?exchange={0}&token={1}'.format(exchange, token))

    ## print(r.json())
    return r.json()

def get_ForexCandles(symbol = 'OANDA:EUR_USD', resolution = 'D', start=1572651390, end = 1575243390):
    """
        Examples: 
            /forex/candle? symbol=OANDA:EUR_USD&resolution=D&from=1572651390&to=1575243390
        Arguments:
            symbol (REQUIRED): Use symbol returned in /forex/symbol endpoint for this field.
            resolution (REQUIRED): Supported resolution includes 1, 5, 15, 30, 60, D, W, M .Some timeframes might not be available depending on the exchange.
            from (optional): UNIX timestamp. Interval initial value. If count is not provided, this field is required
            to (optional): UNIX timestamp. Interval end value. If count is not provided, this field is required
            format (optional): By default, format=json. Strings json and csv are accepted.
            count (optional): Shortcut to set to=Unix.Now and from=Unix.Now - count * resolution_second. With small resolution such as 1, This option might result in no_data over the weekend.
        Return:
            o: List of open prices for returned candles.
            h: List of high prices for returned candles.
            l: List of low prices for returned candles.
            c: List of close prices for returned candles.
            v: List of volume data for returned candles.
            t: List of timestamp for returned candles.
            s: Status of the response. This field can either be ok or no_data.
    """
    r = requests.get('https://finnhub.io/api/v1/forex/candle?symbol={0}&resolution={1}&from={2}&to={3}&token={4}'.format(symbol, resolution, start, end, token))

    ## print(r.json())
    return r.json()


if __name__ == "__main__":
    symbol = 'AAPL'
    # res = get_ForexCandles(symbol = 'OANDA:EUR_USD', resolution = 'D', start=1572651390, end = 1575243390)
    res = get_CryptoSymbol('binance', token)
    res

