import quandl
import pandas as pd
import datetime
import zipfile
import glob
import os

quandl.ApiConfig.api_key="ZKWjVGUk-dp4sWoz5buP"

class StockPrice:
    """
    class for preprocessing data
    """
    def __init__(self, opt):
        #original csv
        self.stock_list = []

        #current date for unzip the file and read csv
        self.csv_filename = 'EOD_test2.csv'
        self.stock_data_folder = 'stock_data'

        self.stock_mat = None    

        self.is_liquid = opt.is_liquid
        self.is_daily = opt.is_daily
        
    def get_stock_prices(self):
        self.stock_list = pd.read_csv(self.csv_filename)
        self.stock_list.columns = [u'Name', u'Date', u'Open', u'High', u'Low', u'Close',u'Volume',u'Dividend',u'Split',u'Adj_Open',u'Adj_High',u'Adj_Low',u'Adj_Close',u'Adj_Volume']

        #align trading date, close price only
        self.stock_mat = self.stock_list.pivot(index=u'Date',columns=u'Name',values=u'Adj_Close')
        
        if self.is_liquid:
            liquid_list = pd.read_csv('liquidtickers.csv').values.reshape([-1]).tolist()
            #TODO
            liquid_list.remove('ECH')
            self.stock_mat = self.stock_mat[liquid_list]
        
        # pick weekly data based on the last trading day of the week
        if not self.is_daily:
            max_wd = 0
            max_wd_list = []
            for i in range(len(self.stock_mat)):
                # get date
                dt = self.stock_mat.iloc[i,0:1].name
                year, month, day = (int(x) for x in dt.split('-'))    
                # weekday: Monday = 0, Sunday = 6
                wd = datetime.date(year, month, day).weekday()
                if wd >= max_wd:
                    max_wd_date = dt
                else:
                    max_wd_list.append(i)
                max_wd = wd
            
            # append the last date if not already included
            if max_wd_date == dt:
                max_wd_list.append(i)
                
            self.stock_mat = self.stock_mat.iloc[max_wd_list]
            
    def update_data_csv(self):
        # clean previous files
        file_list = glob.glob('stock_data/*.csv')
        for filename in file_list:
            os.remove(filename)

        quandl.bulkdownload("EOD")
        print("Download succeeded")        

        zip_ref = zipfile.ZipFile('EOD.zip', 'r')
        zip_ref.extractall(self.stock_data_folder)
        zip_ref.close()
        print("Extract to", self.stock_data_folder)
        
        # get filename
        self.csv_filename = glob.glob('stock_data/*.csv')[0]


######################################################################################################


import pandas as pd
import numpy as np
import math
import quandl as q
token="Us3wFmXGgAj_1cUtHAAR"

# Simple Sharpe ratio calculation
def calc_Sharpe(pnl,N=12):
    return np.sqrt(N) * pnl.mean() / pnl.std()

def get_sp_future():
    return q.get("CHRIS/CME_SP1", authtoken=token).resample(rule='d').last().Last.dropna()


# Simple 50/50 risk parity calculation based on S&P 500 / US Treasuries
def calc_risk_parity(vol=.1,lookback=36):
    df=pd.DataFrame()
    df['SP500']=q.get("CHRIS/CME_SP1", authtoken=token).resample(rule='m').last().Last
    df['US10Y']=q.get("CHRIS/CME_US1", authtoken=token).resample(rule='m').last().Last
    data_pct=df.pct_change()
    rtns=((data_pct/pd.DataFrame.ewm(data_pct,lookback,min_periods=lookback/3.).std())*(vol/math.sqrt(12))).dropna()
    mat=pd.DataFrame.ewm(data_pct,lookback,min_periods=lookback/3.).corr().dropna()
    sf=pd.Series()
    for d,dd in mat.groupby(level=0):
        sf[d]=1/math.sqrt(dd.mean().mean())
    return rtns.multiply(sf,axis=0).dropna().mean(axis=1)

# Yield to return 
def yields_to_rtn_index(yld):
    s = pd.Series()
    last=4.06
    for timestamp,yi in yld.iteritems():
        interest = last/12.
        change=np.pv(yi/100.,10,-last,fv=-100)-100
        last=yi
        s[timestamp]=change+interest
    return s/100.

# time series of bond returns
def get_bond_time_series():
	yld=q.get("FRED/DGS10", authtoken=token).Value
	return yields_to_rtn_index(yld.resample(rule='m').last())

def get_sp():
    return q.get('MULTPL/SP500_REAL_PRICE_MONTH' , authtoken=token).Value.resample(rule='m').last()

# Has data only until November 2018.  Come on quandl!!!
def get_libor():
    return q.get("ECB/RTD_M_S0_N_C_USL3M_U", authtoken=token)['Percent per annum']

def get_futures_data():
    
	mkts={'SP 500':'CHRIS/CME_SP1',
	      'Crude Oil':'CHRIS/CME_CL1',
	      'Dollar Index':'CHRIS/ICE_DX1',
	      'Wheat':'CHRIS/CME_W1',
	      'Euro':'CHRIS/CME_EC1',
	      'GBP':'CHRIS/CME_BP1',
	      'Gold':'CHRIS/CME_EC1'
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
						try:
							data_index[m]=quandl.get(mkts[m],authtoken=token).Rate
						except:
							print(m)
	data_pct=data_index.pct_change()
	return data_index[['SP 500', 'Crude Oil', 'Dollar Index',  'Wheat',
       'Euro', 'GBP', 'Gold']]['2007':].dropna().pct_change().dropna()

####################################################################################################

df=pd.DataFrame()
df['SP500']=quandl.get("CHRIS/CME_SP1", authtoken=token).Settle
df['Gold']=quandl.get("CHRIS/CME_GC1", authtoken=token).Settle
df['USD Index']=quandl.get("CHRIS/ICE_DX1", authtoken=token).Settle
df['US 10Y']=quandl.get("CHRIS/CME_US1", authtoken=token).Settle
df['Wheat']=quandl.get("CHRIS/CME_W1", authtoken=token).Settle
df['Crude WTI']=quandl.get("CHRIS/CME_CL1", authtoken=token).Settle
df['US Treasury']=quandl.get("CHRIS/CME_TY1", authtoken=token).Settle


########### Extra Markets ###############################
#df['Eurostoxx 50']=quandl.get("CHRIS/EUREX_FESX1", authtoken=token).Settle
#df['Russel']=quandl.get("CHRIS/ICE_TF1", authtoken=token).Settle
#df['Russel']=quandl.get("CHRIS/ICE_TF1", authtoken=token).Settle
#df['Euro']=quandl.get("CHRIS/CME_EC1", authtoken=token).Settle
#df['GBP']=quandl.get("CHRIS/CME_BP1", authtoken=token).Settle
#df['Corn']=quandl.get("CHRIS/CME_C1", authtoken=token).Settle
#df['Eurodollar']=quandl.get("CHRIS/CME_ED1", authtoken=token).Settle
#df['Copper']=quandl.get("CHRIS/CME_HG1", authtoken=token).Settle

price=df.resample(rule='m',how='last')
pct_rtns = price.pct_change()
pct_rtns.cumsum().plot(colormap='jet',title='Markets (simple) cummulative return')


quandl.get_table('WIKI/PRICES', ticker='JCP')