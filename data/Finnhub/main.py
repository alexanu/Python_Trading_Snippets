
import os
from pandas.io.json import json_normalize #package for flattening json in pandas df
import pandas as pd


Path_To_TKNS = os.path.join(os.path.abspath(os.path.join(__file__ ,"../../..")), "connections.cfg")
from configparser import ConfigParser
config = ConfigParser()
config.read(Path_To_TKNS)
token=config['FinnHub']['access_token']

#########################################################################################
import fhub
hub = fhub.Session(token)

# from fhub import core as hub
# hub = fhub.Session(token)



### FX pairs from different providers --------------------------------------

All_FX = pd.concat((hub.symbols(exch,kind='forex').assign(source = exch) # pull all available FX asset (FX pairs and CFDs) ...
                        for exch in hub.exchanges('forex').forex), ignore_index = True) # ... from every broker supported by FinnHub
FH_FX = pd.read_csv("..\\Tkrs_metadata\\All_FX_FINNHUB.csv",index_col=0) # file was downloaded earlier
EURUSD = pd.concat((hub.quote(x) for x in FH_FX[FH_FX.Asset=='EUR/USD'].symbol.to_list()),axis=1) # get last quote of EUR/USD from different providers

### All tickers from main exchanges  --------------------------------------

main_x=pd.read_csv("..\\Tkrs_metadata\\exchanges.csv")
MAIN_STOCKS = pd.concat((hub.symbols(exch).assign(source = exch) # pull all available symbols ...
                        for exch in main_x.FinnHub_code[main_x.Main==1].to_list()), ignore_index = True) # ... for every german stock exhange
MAIN_STOCKS.to_csv("All_tickers_from_main_X.csv")


### German stocks ### -------------------------------------------------------

German_exchanges= ['SG','DE','MU','F','BE','DU','HM'] # german stock exchanges
GER_STOCKS = pd.concat((hub.symbols(exch).assign(source = exch) # pull all available symbols ...
                        for exch in German_exchanges), ignore_index = True) # ... for every german stock exhange
GER_STOCKS['Exch_name']=GER_STOCKS.source.map( # vlookup name of exchange..
                            hub.exchanges('stock').drop_duplicates('code').set_index('code').name) # we need drop_dublicates as there is 1 dublicate
GER_STOCKS['Family']=GER_STOCKS["symbol"].str.split('.').str[0] # 1 stock is traded on different exchanges. Ticker goes XXX.YY where YY is the code of exchange
GER_STOCKS.to_csv("German_tickers.csv")

### Stock traded on many German exhances --------------------------------------

counts = GER_STOCKS['Family'].value_counts() # calculate how many times each family appears
On_Many_X = GER_STOCKS[GER_STOCKS['Family'].isin(counts.index[counts==7])] # ... Max was 7, i.e. a stock is traded on 7 german exchanges
# For example, "Merck" (ticker "6MK"):
Merck = pd.concat((hub.quote(x) for x in On_Many_X[On_Many_X.Family=="6MK"].symbol.to_list()),axis=1)




#### Dividends ##############################################################

GER_STOCKS = pd.concat((hub.symbols(exch).assign(source = exch) # pull all available symbols ...
                        for exch in German_exchanges), ignore_index = True) # ... for every german stock exhange

hub.symbols("indices")
hub.exchanges('stock')
hub.dividends('F')
hub.dividends('F',start='2010-01-01')
hub.dividends('6MK.F',start='2010-01-01')
Dividends = pd.concat((hub.dividends(x,start='2010-01-01') for x in ['AAPL','DELL','HPQ','WDC', 'HPE', 'NTAP']))

Dividends = pd.concat((hub.dividends(x,start='2010-01-01') for x in ['AAPL','DELL','HPQ']))




['finnhubIndustry','ipo','name']
hub.profile2('TSLA').transpose()['finnhubIndustry','ipo','name']
hub.executive('AAPL')
hub.peers('TSLA') # same country and GICS sub-industry


hub.metrics('AAPL',metric='price').loc['marketCapitalization']
hub.all_metrics('AAPL').to_csv("check_metricx.csv")
available_metrics = ['price','valuation','growth','margin','management','financialStrength','perShare']
# be careful with 30 API calls per min for free version..
# it is enough for 6 tickers for every metric separately
Potfolio_cars = pd.concat((hub.metrics(x,metric='price') for x in ['AAPL','DELL','HPQ','WDC', 'HPE', 'NTAP']),axis=1)
Potfolio_pc_valuation = pd.concat((hub.metrics(x,metric='valuation') for x in ['AAPL','DELL','HPQ','WDC', 'HPE', 'NTAP']),axis=1)
Potfolio_pc_margin = pd.concat((hub.metrics(x,metric='margin') for x in ['AAPL','DELL','HPQ','WDC', 'HPE', 'NTAP']),axis=1)
Potfolio_pc_management = pd.concat((hub.metrics(x,metric='management') for x in ['AAPL','DELL','HPQ','WDC', 'HPE', 'NTAP']),axis=1)
Potfolio_pc_perS = pd.concat((hub.metrics(x,metric='perShare') for x in ['AAPL','DELL','HPQ','WDC', 'HPE', 'NTAP']),axis=1)




hub.fund_ownership('TSLA') # Premium



hub.calendar_ipo(start='2020-02-01')





tsla = hub.candle('TSLA')

hub.candle('TSLA',start = '2020-01-01',resolution='1') # default is "adjusted". supported resolution: 1, 5, 15, 30, 60, D, W, M 
hub.quote('TSLA')
hub.quote('OANDA:XAU_NZD').describe()
hub.candle('fxcm:NGAS',kind='forex')
hub.candle('fxcm:NGAS')


hub.news()
# smth wrong with the dates
hub.company_news('TSLA',start="2020-04-01")
hub.major_development('TSLA',start="2020-04-01",end="2020-05-01")
hub.sentiment('TSLA')




x = hub.economic_code().name.unique()

hub.economic_calendar().to_csv('FH_Econ_Caledanr.csv')




##########################################################################################

from finnhub import client as Finnhub
client = Finnhub.Client(api_key=token)



pd.DataFrame(client.exchange()).to_csv("exchanges.csv") # List supported stock exchanges

# List supported forex symbols for every supported forex exchanges
exchg = pd.read_csv("..\\Tkrs_metadata\\exchanges.csv") # file was downloaded earlier with client.exchange()
df = pd.concat((json_normalize(client.stock_symbol(exchange=exch)).assign(source = exch) 
                for exch in exchg.FinnHub_code[exchg.Main==1].tolist()), ignore_index = True)




client.company_profile(symbol="NFLX") # PREMIUM: Get general information of a company 
json_normalize(client.company_profile_base(symbol="NFLX")) # Free: Get general information of a company 
pd.DataFrame(client.recommendation(symbol="NFLX")) # Get latest analyst recommendation trends
pd.DataFrame(client.upgrade_downgrade(symbol="NFLX")) # Get latest stock upgrade and downgrade
client.peers(symbol="NFLX") # Get company peers: same country and GICS sub-industry
pd.DataFrame(client.earnings(symbol="NFLX")) # Get company quarterly earnings
client.metric(symbol="NFLX", metric="margin")

pd.read_csv("https://static.finnhub.io/csv/metrics.csv")



price, valuation, growth, margin, management, financialStrength, perShare


json_normalize(client.quote(symbol="NFLX")) # Get quote data
pd.DataFrame(client.stock_candle(symbol="NFLX", resolution="D", count=200)) # Get candlestick data for stocks

json_normalize(client.stock_candle(symbol="NFLX", resolution="D", **{'from':'1575968404', 'to': '1575968424'}))
# client.stock_tick(symbol="NFLX", resolution="D", **{'from':'1575968404', 'to': '1575968424'}) # [PREMIUM] Get tick data


# List supported forex symbols for every supported forex exchanges
df = pd.concat((json_normalize(client.forex_symbol(exchange=exch)).assign(source = exch) for exch in client.forex_exchange()), ignore_index = True)


pd.DataFrame(client.forex_candle(symbol="OANDA:EUR_USD", resolution="D", count=200)) # Get candlestick data for forex symbols




pd.DataFrame(client.company_news(symbol="NFLX", **{'from':'2020-04-01', 'to': '2020-04-10'}))




json_normalize(client.news(category="general")) # Get latest market news
json_normalize(client.company_news(symbol="NFLX")) # List latest company news by symbol
json_normalize(client.news_sentiment(symbol="NFLX")) # Get company's news sentiment and statistics
pd.DataFrame(client.major(symbol="NFLX", **{'from':'2020-04-01', 'to': '2020-05-10'})["majorDevelopment"])

json_normalize(client.economic_code()) # List codes of supported economic data
json_normalize(client.economic(code="MA-USA-G")) # Get economic data



json_normalize(client.calendar_economic()["economicCalendar"]["result"]) # Get recent and coming economic releases
json_normalize(client.calendar_earnings()["earningsCalendar"]) # Get recent and coming earnings release
json_normalize(client.calendar_ipo(from="2020-04-01",to="2020-04-10")["ipoCalendar"]) # Get recent and coming IPO

###############
# Premium
client.merger_country() # List countries where merger and acquisitions take place
client.merger(country="United States") # List latest merger and acquisitions deal by country.
"United States","United Kingdom","Portugal","Switzerland","Australia","Norway","Sweden","Canada","France","Germany","Japan","Liechtenstein","Spain","Singapore"
client.company_profile(symbol="NFLX") # Get general information of a company 

