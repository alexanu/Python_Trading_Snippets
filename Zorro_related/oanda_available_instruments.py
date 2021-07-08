import json
import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import pandas as pd

import os
from configparser import ConfigParser
Path_To_TKNS = os.path.join(os.path.abspath(os.path.join(__file__ ,"../../..")), "connections.cfg")
config = ConfigParser()
config.read(Path_To_TKNS)
accountID = config['Oanda']['accountID']
token = config['Oanda']['token']


client = oandapyV20.API(access_token="e675daec169c0349374fb184e82571b0-5996df09a9104009f55ddcc30971e8dd")



r = accounts.AccountInstruments(accountID=ACCOUNT_ID)
rv = client.request(r)
data = pd.json_normalize(pd.DataFrame(rv)['instruments'])


needed_columns = ['name', 'type', 'displayName', 'minimumTradeSize','marginRate','financing.longRate','financing.shortRate']
frame[needed_columns].to_csv("Oanda_instruments.csv", index=False)

####################################################################################
from collections import defaultdict
import pandas as pd

import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.pricing as pricing

ACCOUNT_ID = '001-004-1898331-001'
TOKEN = '9cc46a4d26109d643d6bdc2184d97275-17f2ec8612722a8059880f910e87ec0c'
ACCOUNT_CURRENCY = 'EUR'
OUTPUT_FILENAME = 'AssetsOanda.csv'

client = oandapyV20.API(access_token=TOKEN,environment="live") # or "practice"

# Retrieve all instruments for account
r = accounts.AccountInstruments(accountID=ACCOUNT_ID)
rv = client.request(r)
data = pd.json_normalize(pd.DataFrame(rv)['instruments'])

# Retrieve pricing data for all instruments
instrument_list = list(data['name'])
r = pricing.PricingInfo(accountID=ACCOUNT_ID, params={'instruments': ','.join(map(str, instrument_list))})
rv = client.request(r)
prices = pd.json_normalize(pd.DataFrame(rv)['prices']).set_index('instrument').rename(columns={'instrument': 'Name'})
prices[['closeoutAsk', 'closeoutBid']] = prices[['closeoutAsk', 'closeoutBid']].astype(float)
prices['Price'] = prices[['closeoutAsk', 'closeoutBid']].mean(axis=1)
prices['Spread'] = prices['closeoutAsk'] - prices['closeoutBid']

# Rename columns, set instrument column as index, join with prices, and convert data types as necessary
data = data.rename(columns={'name': 'Name',
                            'minimumTradeSize': 'LotAmount'}). \
            set_index('Name'). \
            join(prices[['Price', 'Spread']])
data.index = data.index.str.replace('_','/')  # Replace '_' with '/' in line with Zorro convention
convert_columns = ['LotAmount', 'marginRate', 'financing.longRate', 'financing.shortRate']
data[convert_columns] = data[convert_columns].astype(float)
data['currency'] = data.index.map(lambda c: c[-3:])

# Add Index as type, mainly for cosmetic reasons (where name ends in a number or in 'Index')
data.loc[(data['displayName'].str[-1].str.isnumeric()) | (data['displayName'].str[-5:]=='Index'),'type'] = 'INDEX'

# Store currency rates in dict, add inverse and necessary cross currency rates to be able to convert all
# assets to account currency (also add XAG as currency for XAU/XAG)
conversion_rate = data[data['type']=='CURRENCY']['Price'].to_dict()     # All OANDA currency rates
conversion_rate.update({pair[-3:]+"/"+pair[:3]: 1/rate
                        for pair, rate in conversion_rate.items()})     # Inverse of existing currencies
conversion_rate[f"{ACCOUNT_CURRENCY}/{ACCOUNT_CURRENCY}"] = 1
assert 'USD/'+ACCOUNT_CURRENCY in conversion_rate, "Cannot calculate account currency rates"
conversion_rate['XAG/USD'] = data.loc['XAG/USD']['Price']
# Add missing currencies as cross currency via USD
conversion_rate.update({currency+'/'+ACCOUNT_CURRENCY: conversion_rate[currency+'/USD'] * conversion_rate['USD/'+ACCOUNT_CURRENCY]
                        for currency in
                            (currency for currency in data['currency']
                             if currency+'/'+ACCOUNT_CURRENCY not in conversion_rate)})

# Add additional columns, calculate PipCost based on conversion rates
data['PIP'] = 10.**data['pipLocation']
data['PIPCost'] = data['PIP'] * (data['currency']+'/'+ACCOUNT_CURRENCY).map(conversion_rate)
data['Leverage'] = (1/data['marginRate']).astype(int)
data['MarginCost'], data['Commission'] = 0, 0
data['Symbol'] = data.index

# Adjust for weekend rate inflation on Wednesdays/Fridays if necessary
# data[['financing.longRate', 'financing.shortRate']] = data[['financing.longRate', 'financing.shortRate']] / 4

# Calculate RollLong and RollShort in account currency, for the quantities used by Zorro
financing_pos_size = defaultdict(lambda: 1, {'CURRENCY': 10000})    # Everything but CURRENCY defaults to 1
data['RollLong'] = data['financing.longRate']/365 * \
                   data['Price'] * \
                   data['type'].map(financing_pos_size) * \
                  (data['currency']+'/'+ACCOUNT_CURRENCY).map(conversion_rate)
data['RollShort'] = data['financing.shortRate']/365 * \
                    data['Price'] * \
                    data['type'].map(financing_pos_size) * \
                   (data['currency']+'/'+ACCOUNT_CURRENCY).map(conversion_rate)

EXPORT_COLUMNS = ['Price', 'Spread', 'RollLong', 'RollShort',
                  'PIP', 'PIPCost', 'MarginCost', 'Leverage',
                  'LotAmount', 'Commission', 'Symbol']
DISPLAY_COLUMNS = ['type', 'displayName', 'Price', 'Spread',
                   'financing.longRate', 'financing.shortRate',
                   'RollLong', 'RollShort',
                   'PIP', 'PIPCost', 'MarginCost', 'Leverage',
                   'LotAmount', 'Commission']
data.sort_values(['type', 'Name'], inplace=True)

# Write CSV with group headers
data.to_csv("All_data_from_Oanda.csv")
data[EXPORT_COLUMNS].head(0).to_csv(OUTPUT_FILENAME)
max((open(OUTPUT_FILENAME, 'a').write(f"### {data_type}\n"),
          data.loc[data_group, EXPORT_COLUMNS].to_csv(OUTPUT_FILENAME, float_format='%g', header=False, mode='a'))
    for data_type, data_group in data.groupby('type').groups.items())

print(data[DISPLAY_COLUMNS].to_string())
 

####################################################################################

def get_available_symbols_tick_oanda(self):

    try:
        api = v20.Context(
            'api-fxpractice.oanda.com',
            443,
            True,
            application='sample_code',
            token=config['oanda_v20']['access_token'],
            datetime_format='RFC3339'
        )

        resp = api.account.instruments('101-004-5238032-001')
        instruments = resp.get('instruments')
        instruments = [ins.dict() for ins in instruments]
        instruments = [(ins['displayName'], ins['name'])
                        for ins in instruments]

        return(instruments)

    except:
        print('retrieve error, try again!')