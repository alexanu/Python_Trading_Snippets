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

with open('database/ETF.csv') as csvfile: # get ETF list
    reader = csv.DictReader(csvfile)
    for row in reader: # For each ETF do the following ... 
        print("Update quotes for ETF ", row['Name'])

        # If a csv file with quotes already exist for the current ETF => update the quotes 
        # If doesn't exist - download all quotes for it
        
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



############################################################################
# https://rapidapi.com/apidojo/api/yahoo-finance1?endpoint=apiendpoint_cc4ecbd1-001b-485f-9166-7f0b6163bd43
import requests

url_profile = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-profile"
url_holders = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-holders"
url_insi_trans = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-insider-transactions"
url_hist_data = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data"
url_fin = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-financials"

querystring = {"symbol":"AMRN","region":"US"}
headers = {
    'x-rapidapi-key': "7a3621970amsh9275d1c7c9ed661p15c96ajsnbc45c95d106c",
    'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }
response = requests.request("GET", url_fin, headers=headers, params=querystring).json()
response.keys()

# url_profile
    pd.json_normalize(response['assetProfile']['companyOfficers'])
    pd.json_normalize(response['secFilings']['filings'])
    response['calendarEvents']
    response['quoteType']


# url_holders
    pd.json_normalize(response['fundOwnership']['ownershipList'])
    ins_trans = pd.json_normalize(response['insiderTransactions']['transactions'])
    pd.json_normalize(response['insiderHolders']['holders'])
    response['netSharePurchaseActivity']
    response['majorHoldersBreakdown']
    response['quoteType']
    pd.json_normalize(response['institutionOwnership']['ownershipList'])
    
# url_hist_data
    quotes = pd.json_normalize(response['prices']) # 250 rows of EOD data
    quotes['date'] = pd.to_datetime(quotes['date'],unit='s')
    pd.to_datetime(response['firstTradeDate'],unit='s')
    response['timeZone']

# financials
    response['meta']
    response['financialsTemplate']

    pd.json_normalize(response['cashflowStatementHistoryQuarterly']['cashflowStatements']).transpose()
    pd.json_normalize(response['balanceSheetHistoryQuarterly']['balanceSheetStatements']).transpose()
    pd.json_normalize(response['incomeStatementHistoryQuarterly']['incomeStatementHistory']).transpose()

    pd.json_normalize(response['cashflowStatementHistory']['cashflowStatements']).transpose()
    pd.json_normalize(response['balanceSheetHistory']['balanceSheetStatements']).transpose()
    pd.json_normalize(response['incomeStatementHistory']['incomeStatementHistory']).transpose()

    response['earnings']['earningsChart']
    response['earnings']['financialsChart']

    pd.json_normalize(response['quoteType']).transpose()
    response['timeSeries'].keys()
    response['timeSeries']['annualOperatingIncome']
    
