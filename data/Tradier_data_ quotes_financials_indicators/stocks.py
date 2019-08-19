"""This module contains all functions related to stocks"""
#pylint: disable = trailing-whitespace,line-too-long, too-many-lines, no-name-in-module,  multiple-imports, pointless-string-statement, too-many-arguments

import datetime
import io
import json
import os
import requests
import typing
import pandas
from .exceptions import (
    InvalidBrokerage,
    InvalidStockExchange,
    BadRequest,
    InvalidCredentials,
)
from .config import *
"""Config starts"""

USER_ACCESS_TOKEN = os.environ['USER_ACCESS_TOKEN']
USER_ACCOUNT_NUMBER = os.environ['USER_ACCOUNT_NUMBER']
USER_BROKERAGE = os.environ['USER_BROKERAGE']
TR_STREAMING_API_URL = "https://stream.tradier.com"
TR_BROKERAGE_API_URL = "https://production-api.tradier.com"
TR_SANDBOX_BROKERAGE_API_URL = "https://production-sandbox.tradier.com"


def tr_get_headers(access_token: str) -> dict:
    '''headers for TR brokerage'''
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + access_token
    }
    return headers


def tr_get_content_headers() -> dict:
    '''content headers for TR brokerage'''
    headers = {
        'Accept': 'application/json',
    }
    return headers


"""Config ends"""
"""Data APIs Start"""


def latest_price_info(symbols: str,
                      brokerage: typing.Any = USER_BROKERAGE,
                      access_token: str = USER_ACCESS_TOKEN,
                      dataframe: bool = False) -> dict:
    """Get latest price information for an individual or multiple symbols"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/v1/markets/quotes?symbols={}".format(url, str(symbols.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        if 'quote' not in response.json()['quotes']:
            return response.json()
        data = response.json()['quotes']['quote']
        if isinstance(data, list):
            for i in data:
                i['trade_date'] = datetime.datetime.fromtimestamp(
                    float(i['trade_date']) /
                    1000.0).strftime("%Y-%m-%d %H:%M:%S")
                i['bid_date'] = datetime.datetime.fromtimestamp(
                    float(i['bid_date']) /
                    1000.0).strftime("%Y-%m-%d %H:%M:%S")
                i['ask_date'] = datetime.datetime.fromtimestamp(
                    float(i['ask_date']) /
                    1000.0).strftime("%Y-%m-%d %H:%M:%S")
        else:
            data['trade_date'] = datetime.datetime.fromtimestamp(
                float(data['trade_date']) /
                1000.0).strftime("%Y-%m-%d %H:%M:%S")
            data['bid_date'] = datetime.datetime.fromtimestamp(
                float(data['bid_date']) / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
            data['ask_date'] = datetime.datetime.fromtimestamp(
                float(data['ask_date']) / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        if not dataframe:
            return data
        else:
            if isinstance(data, list):
                return pandas.DataFrame(data)
            else:
                return pandas.DataFrame([data])
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def create_session(brokerage: typing.Any = USER_BROKERAGE,
                   access_token: str = USER_ACCESS_TOKEN) -> str:
    """Create a live session to receive sessionid which is needed for streaming live quotes and trades"""
    if brokerage == "miscpaper":
        access_token = os.environ["KT_ACCESS_TOKEN"]
    elif brokerage == "Tradier Inc.":
        access_token = access_token
    else:
        raise InvalidBrokerage
    response = requests.post(
        "{}/v1/markets/events/session".format(TR_BROKERAGE_API_URL),
        headers=tr_get_headers(access_token),
    )
    if response:
        stream = response.json()["stream"]
        sessionid = str(stream["sessionid"])
        return sessionid
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def latest_quote(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get live quotes direct from various exchanges"""
    # for flagging quotes:
    # https://docs.dxfeed.com/misc/dxFeed_TimeAndSale_Sale_Conditions.htm
    if brokerage == "miscpaper":
        access_token = os.environ["KT_ACCESS_TOKEN"]
    elif brokerage == "Tradier Inc.":
        access_token = access_token
    else:
        raise InvalidBrokerage

    payload = {
        "sessionid": create_session(brokerage=brokerage,
                                    access_token=access_token),
        "symbols": str(symbols.upper()),
        "filter": 'quote',
        "linebreak": True,
    }
    response = requests.post(
        "{}/v1/markets/events".format(TR_STREAMING_API_URL),
        params=payload,
        headers=tr_get_headers(access_token),
        stream=True,
    )
    if response:
        for data in response.iter_content(chunk_size=None,
                                          decode_unicode=True):
            lines = data.decode("utf-8").replace("}{", "}\n{").split("\n")
            for line in lines:
                _quotes = json.loads(line)
                converted_biddata = float(_quotes["biddate"]) / 1000.0
                converted_askdate = float(_quotes["biddate"]) / 1000.0
                _quotes["biddate"] = datetime.datetime.fromtimestamp(
                    converted_biddata).strftime("%Y-%m-%d %H:%M:%S")
                _quotes["askdate"] = datetime.datetime.fromtimestamp(
                    converted_askdate).strftime("%Y-%m-%d %H:%M:%S")
                return _quotes
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        if response.text == "Session not found":
            live_quotes(symbol)
        else:
            raise InvalidCredentials(response.text)


def latest_trade(
        symbols: str,
        filter: str = "trade",
        valid_only: bool = True,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get live trades direct from various exchanges"""
    # for flagging trades:
    # https://docs.dxfeed.com/misc/dxFeed_TimeAndSale_Sale_Conditions.htm
    if brokerage == "miscpaper":
        access_token = os.environ["KT_ACCESS_TOKEN"]
    elif brokerage == "Tradier Inc.":
        pass
    else:
        raise InvalidBrokerage
    sessionid: str = create_session(brokerage=USER_BROKERAGE,
                                    access_token=USER_ACCESS_TOKEN)
    payload = {
        "sessionid": sessionid,
        "symbols": str(symbols.upper()),
        "filter": filter,
        "linebreak": True,
        "validOnly": valid_only,
    }
    response = requests.post(
        "{}/v1/markets/events".format(TR_STREAMING_API_URL),
        params=payload,
        headers=tr_get_headers(access_token),
        stream=True,
    )
    if response:
        for data in response.iter_content(chunk_size=None,
                                          decode_unicode=True):
            lines = data.decode("utf-8").replace("}{", "}\n{").split("\n")
            for line in lines:
                trades = json.loads(line)
                converted_date = float(trades["date"]) / 1000.0
                trades["date"] = datetime.datetime.fromtimestamp(
                    converted_date).strftime("%Y-%m-%d %H:%M:%S")
                return trades
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        if response.text == "Session not found":
            live_quotes(symbol)
        else:
            raise InvalidCredentials(response.text)


def intraday_summary(
        symbols: str,
        filter: str = "summary",
        valid_only: bool = True,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get live summary"""
    # for flagging trades:
    # https://docs.dxfeed.com/misc/dxFeed_TimeAndSale_Sale_Conditions.htm
    if brokerage == "miscpaper":
        access_token = os.environ["KT_ACCESS_TOKEN"]
    elif brokerage == "Tradier Inc.":
        pass
    else:
        raise InvalidBrokerage
    sessionid: str = create_session(brokerage=USER_BROKERAGE,
                                    access_token=USER_ACCESS_TOKEN)
    payload = {
        "sessionid": sessionid,
        "symbols": str(symbols.upper()),
        "filter": filter,
        "linebreak": True,
        "validOnly": valid_only,
    }
    response = requests.post(
        "{}/v1/markets/events".format(TR_STREAMING_API_URL),
        params=payload,
        headers=tr_get_headers(access_token),
        stream=True,
    )
    if response:
        for data in response.iter_content(chunk_size=None,
                                          decode_unicode=True):
            lines = data.decode("utf-8").replace("}{", "}\n{").split("\n")
            for line in lines:
                summary = json.loads(line)
                return summary
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        if response.text == "Session not found":
            live_quotes(symbol)
        else:
            raise InvalidCredentials(response.text)


def ohlcv(symbol: str,
          start: str,
          end: str,
          interval: str = "daily",
          brokerage: typing.Any = USER_BROKERAGE,
          access_token: str = USER_ACCESS_TOKEN,
          dataframe: bool = True) -> dict:
    """Get OHLCV(Open-High-Low-Close-Volume) data for a symbol (As back as you want to go)"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    params: dict = {
        "symbol": str(symbol.upper()),
        "start": str(start),
        "end": str(end),
        "interval": str(interval.lower()),
    }
    response = requests.get(
        "{}/v1/markets/history?".format(url),
        params=params,
        headers=tr_get_headers(access_token),
    )
    if response:
        if not dataframe:
            return response.json()
        else:
            data = response.json()['history']['day']
            dataframe = pandas.DataFrame(data)
            dataframe['date'] = pandas.to_datetime(dataframe['date'])
            dataframe.set_index(['date'], inplace=True)
            return dataframe
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def tick_data(symbol: str,
              start: typing.Any = None,
              end: typing.Any = None,
              data_filter: str = "open",
              brokerage: typing.Any = USER_BROKERAGE,
              access_token: str = USER_ACCESS_TOKEN,
              dataframe: bool = True) -> dict:
    """Get historical tick data(trades placed) for a particular period of time. 
    Goes upto 5 days in the past."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    params = {
        "symbol": str.upper(symbol),
        "start": start,
        "end": end,
        "session_filter": 'open',
    }
    response = requests.get(
        "{}/v1/markets/timesales".format(url),
        headers=tr_get_headers(access_token),
        params=params,
        stream=True,
    )
    try:
        if response:
            if not dataframe:
                return response.json()
            else:
                data = response.json()["series"]["data"]
                dataframe = pandas.DataFrame(data)
                dataframe['time'] = pandas.to_datetime(dataframe['time'])
                dataframe.set_index(['time'], inplace=True)
                return dataframe
        if response.status_code == 400:
            raise BadRequest(response.text)
        if response.status_code == 401:
            raise InvalidCredentials(response.text)
    except Exception as exception:
        raise exception


def min1_bar_data(symbol: str,
                  start: typing.Any = None,
                  end: typing.Any = None,
                  data_filter: str = "all",
                  brokerage: typing.Any = USER_BROKERAGE,
                  access_token: str = USER_ACCESS_TOKEN,
                  dataframe: bool = True) -> dict:
    """Get historical bar data with 1 minute interval for a given period of time. 
    Goes upto 20 days with data points during open market. Goes upto 10 days will all data points."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    params = {
        "symbol": str.upper(symbol),
        "interval": "1min",
        "start": start,
        "end": end,
        "session_filter": str(data_filter),
    }
    response = requests.get(
        "{}/v1/markets/timesales".format(url),
        headers=tr_get_headers(access_token),
        params=params,
        stream=True,
    )
    if response:
        if not dataframe:
            return response.json()
        else:
            data = response.json()["series"]["data"]
            dataframe = pandas.DataFrame(data)
            dataframe['time'] = pandas.to_datetime(dataframe['time'])
            dataframe.set_index(['time'], inplace=True)
            return dataframe
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def min5_bar_data(
        symbol: str,
        start: typing.Any = None,
        end: typing.Any = None,
        data_filter: str = "all",
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
        dataframe: bool = True,
) -> dict:
    """Get historical bar data with 5 minute interval for a given period of time. 
    Goes upto 40 days with data points duing open market. Goes upto 18 days will all data points."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    params = {
        "symbol": str.upper(symbol),
        "interval": "5min",
        "start": start,
        "end": end,
        "session_filter": str(data_filter),
    }
    response = requests.get(
        "{}/v1/markets/timesales".format(url),
        headers=tr_get_headers(access_token),
        params=params,
    )
    if response:
        if not dataframe:
            return response.json()
        else:
            data = response.json()["series"]["data"]
            dataframe = pandas.DataFrame(data)
            dataframe['time'] = pandas.to_datetime(dataframe['time'])
            dataframe.set_index(['time'], inplace=True)
            return dataframe
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def min15_bar_data(symbol: str,
                   start: typing.Any = None,
                   end: str = None,
                   data_filter: str = "all",
                   brokerage: typing.Any = USER_BROKERAGE,
                   access_token: str = USER_ACCESS_TOKEN,
                   dataframe: bool = True) -> dict:
    """Get historical bar data with 15 minute interval for a given period of time. 
    Goes upto 40 days with data points duing open market. Goes upto 18 days will all data points."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    params = {
        "symbol": str.upper(symbol),
        "interval": "15min",
        "start": start,
        "end": end,
        "session_filter": str(data_filter),
    }
    response = requests.get(
        "{}/v1/markets/timesales".format(url),
        headers=tr_get_headers(access_token),
        params=params,
    )
    if response:
        if not dataframe:
            return response.json()
        else:
            data = response.json()["series"]["data"]
            dataframe = pandas.DataFrame(data)
            dataframe['time'] = pandas.to_datetime(dataframe['time'])
            dataframe.set_index(['time'], inplace=True)
            return dataframe
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def stream_live_quotes(symbol: str,
                       brokerage: str = USER_BROKERAGE,
                       access_token: str = USER_ACCESS_TOKEN):
    """Stream live quotes"""
    while True:
        yield latest_quote(symbol)


def stream_live_trades(symbol: str,
                       brokerage: str = USER_BROKERAGE,
                       access_token: str = USER_ACCESS_TOKEN):
    """Stream live trades"""
    while True:
        yield latest_trade(symbol)


def stream_live_summary(symbol: str,
                        brokerage: str = USER_BROKERAGE,
                        access_token: str = USER_ACCESS_TOKEN):
    """Stream live summary"""
    while True:
        yield intraday_summary(symbol)


def list_of_companies(exchange: str = "all"):
    """Get Companies listed on Nasdaq, NYSE, AMEX, their symbols, last prices, market-cap and 
    other information about them."""
    try:
        nasdaq_request = requests.get(
            "https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
        )
        nasdaq_companies = nasdaq_request.content
        nasdaq_dataframe = pandas.read_csv(
            io.StringIO(nasdaq_companies.decode("utf-8")))
        nyse_request = requests.get(
            "https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download"
        )
        nyse_companies = nyse_request.content
        nyse_dataframe = pandas.read_csv(
            io.StringIO(nyse_companies.decode("utf-8")))
        amex_request = requests.get(
            "https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download"
        )
        amex_companies = amex_request.content
        amex_dataframe = pandas.read_csv(
            io.StringIO(amex_companies.decode("utf-8")))
        if exchange == "all":
            dataframe = nyse_dataframe.append(nasdaq_dataframe,
                                              ignore_index=True)
            dataframe = dataframe.append(amex_dataframe, ignore_index=True)
        elif exchange.upper() == "NYSE":
            dataframe = nyse_dataframe
        elif exchange.upper() == "NASDAQ":
            dataframe = nasdaq_dataframe
        elif exchange.upper() == "AMEX":
            dataframe = amex_dataframe
        else:
            raise InvalidStockExchange
        dataframe = dataframe.drop(columns=["Summary Quote", "Unnamed: 8"])
        return dataframe
    except Exception as exception:
        raise exception


def intraday_status(brokerage: typing.Any = USER_BROKERAGE,
                    access_token: str = USER_ACCESS_TOKEN) -> dict:
    """Get the intraday market status"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/v1/markets/clock".format(url),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()['clock']
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def market_calendar(month: int, year: int) -> dict:
    """Get the market calendar of a given month(Goes back till 2013)"""
    params = {"year": year, "month": month}
    response = requests.get(
        "{}/v1/markets/calendar".format(TR_BROKERAGE_API_URL),
        headers=tr_get_content_headers(),
        params=params,
    )
    if response:
        return response.json()["calendar"]
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def symbol_search(
        company_name: str,
        indexes: bool = True,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Search for listed company's symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/v1/markets/search?q={}&indexes={}".format(url, str(company_name),
                                                      str(indexes)),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def symbol_lookup(
        symbol: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Search for securities/companies using symbols"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/v1/markets/lookup?q={}".format(url, str(symbol)),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def shortable_securities(brokerage: typing.Any = USER_BROKERAGE,
                         access_token: str = USER_ACCESS_TOKEN,
                         dataframe: bool = True) -> dict:
    '''Get list of all securitites that can be sold short for the given broker'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/v1/markets/etb".format(url),
        headers=tr_get_headers(access_token),
    )
    if response:
        if not dataframe:
            return response.json()
        else:
            data = response.json()['securities']['security']
            dataframe = pandas.DataFrame(data)
            return dataframe
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def check_if_shortable(symbol: str,
                       brokerage: typing.Any = USER_BROKERAGE,
                       access_token: str = USER_ACCESS_TOKEN) -> typing.Any:
    '''Check if the given stock/security is shortable or not for the given broker'''
    try:
        data = shortable_securities(brokerage=brokerage,
                                    access_token=access_token,dataframe=False)
        if "security" in data['securities']:
            return bool(
                next((item for item in data['securities']['security']
                      if item["symbol"] == symbol), False))
        else:
            return data
    except Exception as exception:
        raise exception


def company_fundamentals(
        symbol: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get company fundamental information"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/company?symbols={}".format(
            url, str(symbol.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def corporate_calendar(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get the corporate calendar information for given symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/calendars?symbols={}".format(
            url, str(symbols.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def dividend_information(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get companydividend information for given symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/dividends?symbols={}".format(
            url, str(symbols.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def corporate_actions(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get corporate actions information for given symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/corporate_actions?symbols={}".format(
            url, str(symbols.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def operation_ratio(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get operation ratio information for given symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/ratios?symbols={}".format(
            url, str(symbols.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def corporate_financials(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get corporate financials information for given symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/financials?symbols={}".format(
            url, symbols.upper()),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def price_statistics(
        symbols: str,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
) -> dict:
    """Get price statistics information for given symbol"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get(
        "{}/beta/markets/fundamentals/statistics?symbols={}".format(
            url, str(symbols.upper())),
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


"""Data APIs end"""
"""Trading APIs start"""


def buy_preview(
        symbol: str,
        quantity: int,
        duration: str = "gtc",
        order_type: str = "market",
        price: typing.Any = None,
        stop: typing.Any = None,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
        account_number: str = USER_ACCOUNT_NUMBER,
) -> dict:
    """Preview your buy order. 
    This will allow you to place a buy order without it being sent to the market 
    so that you can know what will it actaully look like when you place a real order"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        "class": "equity",
        "symbol": str(symbol.upper()),
        "duration": str(duration.lower()),
        "side": "buy",
        "quantity": str(quantity),
        "type": str(order_type.lower()),
        "price": price,
        "stop": stop,
        "preview": "true",
    }
    response = requests.post(
        "{}/v1/accounts/{}/orders/".format(url, account_number),
        params=post_params,
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def buy_to_cover_preview(
        symbol: str,
        quantity: int,
        duration: str = "gtc",
        order_type: str = "market",
        price: typing.Any = None,
        stop: typing.Any = None,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
        account_number: str = USER_ACCOUNT_NUMBER,
) -> dict:
    """Preview your buy to cover order. 
    This will allow you to place a buy_to_cover order without it being sent to the market 
    so that you can know what will it actaully look like when you place a real order. 
    Buy to cover order cannot be placed for long and zero position."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        "class": "equity",
        "symbol": str(symbol.upper()),
        "duration": str(duration.lower()),
        "side": "buy_to_cover",
        "quantity": str(quantity),
        "type": str(order_type.lower()),
        "price": price,
        "stop": stop,
        "preview": "true",
    }
    response = requests.post(
        "{}/v1/accounts/{}/orders/".format(url, account_number),
        params=post_params,
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def sell_preview(symbol: str,
                 quantity: int,
                 brokerage: typing.Any = USER_BROKERAGE,
                 access_token: str = USER_ACCESS_TOKEN,
                 account_number: str = USER_ACCOUNT_NUMBER,
                 duration: str = "gtc",
                 order_type: str = "market",
                 price: typing.Any = None,
                 stop: typing.Any = None) -> dict:
    '''Preview your sell order. 
    This will allow you to place a sell order without it being sent to the market 
    so that you can know what will it actaully look like when you place a real order'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        'class': 'equity',
        'symbol': str(symbol.upper()),
        'duration': str(duration.lower()),
        'side': 'sell',
        'quantity': str(quantity),
        'type': str(order_type.lower()),
        'price': price,
        'stop': stop,
        'preview': 'true'
    }
    response = requests.post("{}/v1/accounts/{}/orders/".format(
        url, account_number),
                             params=post_params,
                             headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def sell_short_preview(symbol: str,
                       quantity: int,
                       brokerage: typing.Any = USER_BROKERAGE,
                       access_token: str = USER_ACCESS_TOKEN,
                       account_number: str = USER_ACCOUNT_NUMBER,
                       duration: str = "gtc",
                       order_type: str = "market",
                       price: typing.Any = None,
                       stop: typing.Any = None) -> dict:
    '''Preview your sell short order. 
    This will allow you to place a sell_short order without it being sent to the market 
    so that you can know what will it actaully look like when you place a real order. 
    Sell short order cannot be placed for long position or if there are pending Buy orders.'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        'class': 'equity',
        'symbol': str(symbol.upper()),
        'duration': str(duration.lower()),
        'side': 'sell_short',
        'quantity': str(quantity),
        'type': str(order_type.lower()),
        'price': price,
        'stop': stop,
        'preview': 'true'
    }
    response = requests.post("{}/v1/accounts/{}/orders/".format(
        url, account_number),
                             params=post_params,
                             headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def buy(
        symbol: str,
        quantity: int,
        duration: str = "gtc",
        order_type: str = "market",
        price: typing.Any = None,
        stop: typing.Any = None,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
        account_number: str = USER_ACCOUNT_NUMBER,
) -> dict:
    """Buy stocks. 
    Order placed using this API will be sent to the market 
    and executed according to your specifications."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        "class": "equity",
        "symbol": str(symbol.upper()),
        "duration": str(duration.lower()),
        "side": "buy",
        "quantity": str(quantity),
        "type": str(order_type.lower()),
        "price": price,
        "stop": stop,
    }
    response = requests.post(
        "{}/v1/accounts/{}/orders/".format(url, account_number),
        params=post_params,
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def buy_to_cover(
        symbol: str,
        quantity: int,
        duration: str = "gtc",
        order_type: str = "market",
        price: typing.Any = None,
        stop: typing.Any = None,
        brokerage: typing.Any = USER_BROKERAGE,
        access_token: str = USER_ACCESS_TOKEN,
        account_number: str = USER_ACCOUNT_NUMBER,
) -> dict:
    """Buy to cover short positions. 
    Order placed using this API will be sent to the market 
    and executed according to your specifications."""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        "class": "equity",
        "symbol": str(symbol.upper()),
        "duration": str(duration.lower()),
        "side": "buy_to_cover",
        "quantity": str(quantity),
        "type": str(order_type.lower()),
        "price": price,
        "stop": stop,
    }
    response = requests.post(
        "{}/v1/accounts/{}/orders/".format(url, account_number),
        params=post_params,
        headers=tr_get_headers(access_token),
    )
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def sell(symbol: str,
         quantity: int,
         brokerage: typing.Any = USER_BROKERAGE,
         access_token: str = USER_ACCESS_TOKEN,
         account_number: str = USER_ACCOUNT_NUMBER,
         duration: str = "gtc",
         order_type: str = "market",
         price: typing.Any = None,
         stop: typing.Any = None) -> dict:
    '''Sell stocks. 
    Order placed using this API will be sent to the market 
    and executed according to your specifications.'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        'class': 'equity',
        'symbol': str(symbol.upper()),
        'duration': str(duration.lower()),
        'side': 'sell',
        'quantity': str(quantity),
        'type': str(order_type.lower()),
        'price': price,
        'stop': stop,
    }
    response = requests.post("{}/v1/accounts/{}/orders/".format(
        url, account_number),
                             params=post_params,
                             headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def sell_short(symbol: str,
               quantity: int,
               brokerage: typing.Any = USER_BROKERAGE,
               access_token: str = USER_ACCESS_TOKEN,
               account_number: str = USER_ACCOUNT_NUMBER,
               duration: str = "gtc",
               order_type: str = "market",
               price: typing.Any = None,
               stop: typing.Any = None) -> dict:
    '''Sell short stocks(stocks you don't own yet). 
    Order placed using this API will be sent to the market 
    and executed according to your specifications.'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    post_params = {
        'class': 'equity',
        'symbol': str(symbol.upper()),
        'duration': str(duration.lower()),
        'side': 'sell_short',
        'quantity': str(quantity),
        'type': str(order_type.lower()),
        'price': price,
        'stop': stop,
    }
    response = requests.post("{}/v1/accounts/{}/orders/".format(
        url, account_number),
                             params=post_params,
                             headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def change_order(order_id: str,
                 duration: str,
                 order_type: str,
                 price: typing.Any = None,
                 stop: typing.Any = None,
                 brokerage: typing.Any = USER_BROKERAGE,
                 access_token: str = USER_ACCESS_TOKEN,
                 account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    '''Change an order if it is not filled yet.'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    put_params = {
        'order_id': order_id,
        'type': str(order_type.lower()),
        'duration': str(duration),
        'price': str(price),
        'stop': str(stop)
    }
    response = requests.put("{}/v1/accounts/{}/orders/{}".format(
        url, account_number, order_id),
                            data=put_params,
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def cancel_order(order_id: str,
                 brokerage: typing.Any = USER_BROKERAGE,
                 access_token: str = USER_ACCESS_TOKEN,
                 account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    '''Cancel an order if it is not filled yet.'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.delete("{}/v1/accounts/{}/orders/{}".format(
        url, account_number, order_id),
                               headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


'''Trading APIs end'''
'''User/Account APIs start'''


def user_profile(brokerage: typing.Any = USER_BROKERAGE,
                 access_token: str = USER_ACCESS_TOKEN) -> dict:
    '''Get information pertaining to all your accounts with your brokerage.'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/user/profile".format(url),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def user_account_number(brokerage: typing.Any = USER_BROKERAGE,
                        access_token: str = USER_ACCESS_TOKEN) -> typing.Any:
    '''Not in docs. Get your account number or list of account numbers associated with your user profile'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/user/profile".format(url),
                            headers=tr_get_headers(access_token))
    if response:
        data = response.json()['profile']['account']
        account_numbers = [row['account_number'] for row in data]
        return account_numbers
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def account_balance(brokerage: typing.Any = USER_BROKERAGE,
                    access_token: str = USER_ACCESS_TOKEN,
                    account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    '''Get your account balance'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/accounts/{}/balances".format(
        url, account_number),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def account_positions(brokerage: typing.Any = USER_BROKERAGE,
                      access_token: str = USER_ACCESS_TOKEN,
                      account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    '''Get all the postions your account holds'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/accounts/{}/positions".format(
        url, account_number),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def account_history(brokerage: str = USER_BROKERAGE,
                    access_token: str = USER_ACCESS_TOKEN,
                    account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    """Get your account's history"""
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/accounts/{}/history".format(
        url, account_number),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def account_closed_positions(brokerage: str = USER_BROKERAGE,
                             access_token: str = USER_ACCESS_TOKEN,
                             account_number: str = USER_ACCOUNT_NUMBER
                             ) -> dict:
    '''Get info(gain, loss, etc) on your closed positions'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/accounts/{}/gainloss".format(
        url, account_number),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def account_orders(brokerage: str = USER_BROKERAGE,
                   access_token: str = USER_ACCESS_TOKEN,
                   account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    '''Get intraday and open/pending order information for your account'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/accounts/{}/orders".format(
        url, account_number),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


def get_order(order_id: str,
              brokerage: str = USER_BROKERAGE,
              access_token: str = USER_ACCESS_TOKEN,
              account_number: str = USER_ACCOUNT_NUMBER) -> dict:
    '''Get detailed information for a specific order'''
    if brokerage == "Tradier Inc.":
        url = TR_BROKERAGE_API_URL
    elif brokerage == "miscpaper":
        url = TR_SANDBOX_BROKERAGE_API_URL
    else:
        raise InvalidBrokerage
    response = requests.get("{}/v1/accounts/{}/orders/{}".format(
        url, account_number, order_id),
                            headers=tr_get_headers(access_token))
    if response:
        return response.json()
    if response.status_code == 400:
        raise BadRequest(response.text)
    if response.status_code == 401:
        raise InvalidCredentials(response.text)


'''User/Account APIs end'''
