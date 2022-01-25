# Every new function should be also added to init.py

# added to insider_trading.py on 11.12.2021
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