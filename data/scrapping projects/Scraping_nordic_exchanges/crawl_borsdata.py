import requests
import pandas as pd
import numpy as np
import datetime as dt
import os


# https://www.nordnet.se/graph/indicator/sse/omxspi?from=1970-01-01&to=2018-08-04&fields=last
# https://www.nordnet.se/graph/instrument/11/992?from=1970-01-01&to=2018-08-08&fields=last,open,high,low,volume


def crawl_borsdata(stock_name):
    try:
        request = requests.post('https://borsdata.se/analysis/excel', data={'q': stock_name})
        data = request.json()
        print(data)
    except:
        print('request error!')



crawl_borsdata("skistar")