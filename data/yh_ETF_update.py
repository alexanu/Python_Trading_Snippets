# Source: https://github.com/sasadangelo/finance

import datetime as dt
import pandas as pd
pd.core.common.is_list_like = pd.api.types.is_list_like
import pandas_datareader.data as web
import yfinance as yf
import csv
from pathlib import Path

yf.pdr_override() # <== that's all it takes :-)

start_date = dt.datetime(1970, 1, 1)
end_date = dt.datetime.now() - dt.timedelta(days=1)
# Read ETF.csv file in order to iterate through all the ETF in our database
with open('database/ETF.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    # For each ETF do the following ... 
    for row in reader:
        print("Update quotes for ETF ", row['Name'])

        # If a csv file already exist for the current ETF update the quotes in 
        # latest one. If a csv file does not exist for that ETF download all 
        # quotes for it.
        csv_file_path = Path("database/quotes/" + row['Ticker'] + ".csv")
        if csv_file_path.exists():
            with open(csv_file_path, "r") as csv_file:
                last_quote_date=list(csv.reader(csv_file))[-1][0]
            last_update_date = dt.datetime.strptime(last_quote_date, '%Y-%m-%d')
            last_update_date += dt.timedelta(days=1)
            try:
                df = web.get_data_yahoo(row['Ticker'], last_update_date, end_date)
            except Exception as e:
                print("Cannot download quotes for ",row['Ticker']," for the specified period: [", last_update_date, ", ", end_date)
                print(getattr(e, 'message', repr(e)))
            with open(csv_file_path, "a") as csv_file:
                df.to_csv(csv_file, header=False)
        else:
            df = web.get_data_yahoo(row['Ticker'], start_date, end_date)
            df.to_csv('database/quotes/' + row['Ticker'] + '.csv')
