# Every new function should be also added to init.py

# added to insider_trading.py on 11.12.2021
from sqlalchemy import false


def insider_trans(apikey: str) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /insider-trading/ API

    :param apikey: Your API key.
    :return: A list of dictionaries.
    """
    path = f"insider-trading-transaction-type"
    query_vars = {"apikey": apikey}
    return __return_json_v4(path=path, query_vars=query_vars)


# added to insider_trading.py on 11.12.2021
def fail_to_deliver(
    apikey: str,
    symbol: str = None,
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /fail_to_deliver/ API

    :param apikey: Your API key.
    :param name: String of name.
    :return: A list of dictionaries.
    """
    path = f"fail_to_deliver/"
    query_vars = {"apikey": apikey}
    if symbol:
        query_vars["symbol"] = symbol
    return __return_json_v4(path=path, query_vars=query_vars)


# added to company_valuation.py on 11.12.2021
def historical_rating(
    apikey: str,
    symbol: str,
    limit: int = DEFAULT_LIMIT,
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /financial-growth/ API.

    :param apikey: Your API key.
    :param symbol: Company ticker.
    :param limit: Number of rows to return.
    :return: A list of dictionaries.
    """
    path = f"historical-rating/{symbol}"
    query_vars = {"apikey": apikey, "limit": limit}
    return __return_json_v3(path=path, query_vars=query_vars)


# added to company_valuation.py on 11.12.2021
def shares_float(
    apikey: str,
    symbol: str = None,
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /shares_float/ API

    :param apikey: Your API key.
    :param symbol: String of name.
    :return: A list of dictionaries.
    """
    path = f"shares_float"
    query_vars = {"apikey": apikey}
    if symbol:
        query_vars["symbol"] = symbol
    return __return_json_v4(path=path, query_vars=query_vars)


# added to company_valuation.py on 11.12.2021
def shares_float_all(
    apikey: str,
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /shares_float/ API

    :param apikey: Your API key.
    :return: A list of dictionaries.
    """
    path = f"shares_float/all"
    query_vars = {"apikey": apikey}
    return __return_json_v4(path=path, query_vars=query_vars)

# added to company_valuation.py on 11.12.2021
def financial_reports_dates(
    apikey: str,
    symbol: str = None,
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /financial-reports-dates/ API

    :param apikey: Your API key.
    :param symbol: String of name.
    :return: A list of dictionaries.
    """
    path = f"financial-reports-dates"
    query_vars = {"apikey": apikey}
    if symbol:
        query_vars["symbol"] = symbol
    return __return_json_v4(path=path, query_vars=query_vars)

# added to stock_market.py on 11.12.2021
def is_the_market_open(apikey: str) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /is-the-market-open/ API

    :param apikey: Your API key.
    :return: A list of dictionaries.
    """
    path = f"is-the-market-open"
    query_vars = {"apikey": apikey}
    return __return_json_v3(path=path, query_vars=query_vars)


# added to institutional_fund.py on 11.12.2021
def etf_stock_exposure(
    apikey: str, symbol: str
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP / etf-stock-exposure / API.

    :param apikey: Your API key.
    :param symbol: Company ticker.
    :return: A list of dictionaries.
    """
    path = f"etf-stock-exposure/{symbol}"
    query_vars = {"apikey": apikey}
    return __return_json_v3(path=path, query_vars=query_vars)

# added to calendar.py on 11.05.2022---------------------------------------------------

def earning_calendar_confirmed(
    apikey: str, from_date: str = None, to_date: str = None
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /earning-calendar-confirmed/ API.

    confirmed earnings within selected time frame that contains fields like date, time, exchange and more

    Note: Between the "from" and "to" parameters the maximum time interval can be 3 months.
    :param apikey: Your API key.
    :param from_date: 'YYYY-MM-DD'
    :param to_date: 'YYYY-MM-DD'
    :return: A list of dictionaries.
    """
    path = f"earning-calendar-confirmed"
    query_vars = {
        "apikey": apikey,
    }
    if from_date:
        query_vars["from"] = from_date
    if to_date:
        query_vars["to"] = to_date
    return __return_json_v4(path=path, query_vars=query_vars)


# added to institutional_fund.py on 11.05.2022---------------------------------------------------
# only for Enterprise Plan
def inst_ownership_symbols(
    apikey: str,
    symbol: str = None,
    date: str = None,
    includeCurrentQuarter: bool = False
) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Query FMP /institutional-ownership/symbol-ownership/ API
    Each filing is due within 45 days after the reporting period ...
        ... for example after December 31, or, stated differently, by February 14 of the subsequent calendar year. Rule 13f-1(a)
    
    Setting includeCurrentQuarter property to true will include the current non terminated filing period to the results data. 
    """
    path = f"institutional-ownership/symbol-ownership"
    query_vars = {"apikey": apikey}
    if symbol:
        query_vars["symbol"] = symbol
    if date:
        query_vars["date"] = date
    if includeCurrentQuarter: 
        query_vars["includeCurrentQuarter"] = true
    return __return_json_v4(path=path, query_vars=query_vars)

# to implement:
#   https://site.financialmodelingprep.com/developer/docs/stock-ownership-by-holders-api#Python
#   https://site.financialmodelingprep.com/developer/docs/institutional-holdings-portfolio-positions-summary-api#Python
