import json
import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import pandas as pd

from configparser import ConfigParser
config = ConfigParser()
config.read(r'..\API_Connections\connections.cfg')
accountID = "xxx-xxx-xxxxxxx-xxx"
token = "xxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxx"
client = oandapyV20.API(access_token=token)

r = accounts.AccountInstruments(accountID=accountID)
rv = client.request(r)
data = json.dumps(rv, indent=2)
frame = pd.json_normalize(pd.read_json(data)['instruments'])
needed_columns = ['name', 'type', 'displayName', 'minimumTradeSize','marginRate','financing.longRate','financing.shortRate']
frame[needed_columns].to_csv("Oanda_instruments.csv", index=False)
 

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