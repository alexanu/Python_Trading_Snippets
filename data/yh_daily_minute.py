
import requests
import pandas as pd
import pandas_datareader.data as web
import os
import arrow
import datetime
import pandas as pd
import numpy as np

# uses Yahoo Finance's API to get minute-by-minute ticks

    def get_quote_data(symbol, data_range, data_interval):
        res = requests.get(
            'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={data_range}&interval={data_interval}'.format(
                **locals()))
        data = res.json()
        body = data['chart']['result'][0]
        dt = datetime.datetime
        dt = pd.Series(map(lambda x: arrow.get(x).to('EST').datetime.replace(tzinfo=None), body['timestamp']), name='dt')
        df = pd.DataFrame(body['indicators']['quote'][0], index=dt)
        dg = pd.DataFrame(body['timestamp'])
        df = df.loc[:, ('close', 'volume')]
        df.dropna(inplace=True)  # removing NaN rows
        df.columns = ['CLOSE', 'VOLUME']  # Renaming columns in pandas
        df.to_csv('out.csv')

        return df

    data = get_quote_data('F', '5d', '1m')
    print(data)


# Update daily quotes from Yahoo
    DATE_FORMAT = "%Y-%m-%d"
    #symbols_list = ["FB","AAPL","NFLX","GOOG","BA","GS","BABA","TSLA"]
    symbols_list = ["AAPL"]

    def file_exists(fn):
        exists = os.path.isfile(fn)
        if exists:
            return 1
        else:
            return 0

    def write_to_file(exists, fn, f):
        if exists:
            f1 = open(fn, "r")
            last_line = f1.readlines()[-1]
            f1.close()
            last = last_line.split(",")
            date = (datetime.datetime.strptime(last[0], DATE_FORMAT)).strftime(DATE_FORMAT)
            today = datetime.datetime.now().strftime(DATE_FORMAT)
            if date != today:
                with open(fn, 'a') as outFile:
                    f.tail(1).to_csv(outFile, header=False)
        else:
            print("new file")
            f.to_csv(fn)

    def get_daily_quote(ticker):
        today = datetime.datetime.now().strftime(DATE_FORMAT)
        f = web.DataReader([ticker], "yahoo", start=today)
        return f

    def get_history_quotes(ticker):
        today = datetime.now().strftime(DATE_FORMAT)
        f = web.DataReader([ticker], "yahoo", start='2018-08-01', end=today)
        return f


    for ticker in symbols_list:
        fn = "./quotes/" + ticker + "_day.csv";
        if file_exists(fn):
            f = get_daily_quote(ticker)
            write_to_file(OLD, fn, f)
        else:
            f = get_history_quotes(ticker)
            write_to_file(NEW, fn, f)
