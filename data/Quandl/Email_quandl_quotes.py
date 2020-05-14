# Source: https://github.com/mrefermat/aws_fin_data


import quandl
token='QWe8iSbyAFzRuod2aroM' # quandl
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.sectorperformance import SectorPerformances
key ='B22889019-EABCFEE1' #AV

import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
plt.switch_backend('agg')
from mail import Email
sns.set_context("poster")
sns.set(font_scale=1)





# Simple Sharpe ratio calculation
def calc_Sharpe(pnl,N=12):
    return np.sqrt(N) * pnl.mean() / pnl.std()

# Calculate absolute value pairwise correlation for just a non-timeseries correlation matrix
def calc_pairwise_correlation(corr_matrix):
    n=corr_matrix.count().count()
    return (corr_matrix.abs().sum()-1).sum()/(n*(n-1))

# Calculate timeseries of pairwise correlation using days look back accros as many numbers as needed
def calc_ts_pairwise_correlation(data_pct,days=250):
	corrts=pd.ewmcorr(data_pct,days,min_periods=days)
	s = pd.Series()
	for i in data_pct.index:
		x=corrts.ix[i]
		x=x[x.count()!=0].T[x.count()!=0]
		s[i]=calc_pairwise_correlation(x)
	return s


mkts={'SP 500':'CHRIS/CME_SP1',
	  'Natural Gas':'CHRIS/CME_NG1',
      'US 10Y':'CHRIS/CME_TY1',
      'Crude Oil':'CHRIS/CME_CL1',
      'Eurostoxx 50':'CHRIS/EUREX_FESX1',
      'Dollar Index':'CHRIS/ICE_DX1',
      'Wheat':'CHRIS/CME_W7',
      'Corn':'CHRIS/CME_C1',
      'Dax':'CHRIS/EUREX_FDAX1',
      'FTSE100':'CHRIS/LIFFE_Z1',
      'Eurodollar':'CHRIS/CME_ED1',
      'Euro':'CHRIS/CME_EC1',
      'GBP':'CHRIS/CME_BP1',
      'Gold':'CHRIS/CME_EC1'
      'US 3M T-bills':'FRED/DTB3',
      'Fed Funds Effective Rate':'FRED/DFF',
      'US Investment Grade':'COM/CDXNAIG',
      'US High Yield':'COM/CDXNAHY',
      'US 20 Year Treasury':'FRED/DGS20',
      'TED Spread':'FRED/TEDRATE',
      #'EUR':'CURRFX/USDEUR',
      #'JPY':'CURRFX/USDJPY',
      #'GBP':'CURRFX/GBPUSD',
     # 'CHF':'CURRFX/USDCHF',
     # 'BRL':'CURRFX/USDBRL',
     # 'CNY':'CURRFX/CNYUSD',
     # 'RUB':'CURRFX/RUBUSD',
     # 'TRY':'CURRFX/TRYUSD'

      }

data_index=pd.DataFrame()
for m in mkts.keys():
    try:
        data_index[m]=quandl.get(mkts[m],authtoken=token).Last
    except:
        try:
            data_index[m]=quandl.get(mkts[m],authtoken=token).Settle
        except:
            try:
                data_index[m]=quandl.get(mkts[m],authtoken=token).Value
            except:
                try:
                    data_index[m]=quandl.get(mkts[m],authtoken=token).value
                except:
                    data_index[m]=quandl.get(mkts[m],authtoken=token).Rate
data_pct=data_index.pct_change()  



s=calc_ts_pairwise_correlation(data_pct)
s.dropna().to_csv('pairwise_c.csv')

s['2000':].plot(colormap='jet').get_figure().savefig('pairwise.png')

e=Email(subject='Morning Update: Pairwise Correlation')
e.add_attachment('pairwise.png')
e.send()


data_pct['2019':].cumsum().ffill().plot(colormap='brg').get_figure().savefig('YTD.png')

e=Email(subject='Morning Update: Macro YTD Email')
e.add_attachment('YTD.png')
e.send()




def get_sector_data():
      sp=SectorPerformances (key=key, output_format='pandas')
      data,_ = sp.get_sector()
      df = pd.DataFrame()
      df['1M Performane']=data['Rank D: Month Performance']
      df['YTD Performance']=data['Rank F: Year-to-Date (YTD) Performance']
      df['1Y Performance']=data['Rank G: Year Performance']
      df['3Y Performance']=(data['Rank H: Year Performance']+1)**.33333333-1
      df['10Y Performance']=(data['Rank J: Year Performance']+1)**.1-1
      return df

get_sector_data().plot(kind='bar',colormap='jet',title='Performance (Long term Annualized)').get_figure().savefig('sector.png',bbox_inches='tight')

e=Email(subject='Morning Update: Sector Performance')
e.add_attachment('sector.png')
e.send()



def get_sp_future():
    return quandl.get("CHRIS/CME_SP1", authtoken=token).resample(rule='d').last().Last.dropna()

sp=get_sp_future()
pct_returns=sp.pct_change()
short_days=1
long_days=60
z=(pd.Series.ewm(pct_returns,short_days).mean()-pd.Series.ewm(pct_returns,long_days).mean())/(pd.Series.ewm(pct_returns,long_days).std())

if z.abs().iloc[-1]>1:
	df=pd.DataFrame()
	df['SP']=sp
	df['Z score']=z
	ax=df['2018':].plot(secondary_y='SP')
	ax.get_figure().savefig('zscore_sp.png')
	e=Email(subject='Morning Update: S&P 500 1sd Move')
	e.add_attachments(['zscore_sp.png'])
	e.send()
else:
	print('No email')


