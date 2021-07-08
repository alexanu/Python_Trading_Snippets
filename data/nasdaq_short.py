import logging
import inspect
import pandas as pd
import urllib2
class NasdaqWrapper():
	def __init__(self):
		return
	def get_short_ratio(self,symbol):
		url="http://www.nasdaq.com/symbol/"+symbol+"/short-interest"
		logging.info(url)
		try:
			response=urllib2.urlopen(url,timeout=10)
			html = response.read()
		except Exception as e:
			logging.error('Get short interest failed for:'+symbol)
			return None
		if len(html)==0:
			return None
		try:
			data=pd.read_html(html)
		except:
			logging.error("Pandas table read failed:"+symbol)
			return None
		i=-1
		foundframe=False
		for frame in data:
			i+=1
			if 'Short Interest' in frame.columns:
				foundframe=True
				break
		if foundframe==False:
			logging.error('Bad COLS for Short Interest, or short interest does not exist:'+str(symbol))
			return None
		else:
			return data[i]
	def download_exchange_information(self,exchanges=['NASDAQ','NYSE','AMEX']):
		df=pd.DataFrame()
		for exchange in exchanges:
			url="http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange="+exchange+"&render=download"
			try:
				frame=pd.read_csv(url)
				frame['Exchange']=exchange
				df=df.append(frame,ignore_index=True)
			except:
				continue
		if len(df)==0:
			return df
		df.columns=df.columns.str.lower()
		for column in df.columns:
			if pd.isnull(df[column]).all():
				df=df.drop(column,1)
		return df
	def get_company_earnings(self,symbol=None):
		return
if __name__ == "__main__":
	n=NasdaqWrapper()
	x=n.download_exchange_information()
	symbols=['AAPL','DSX','MSFT']
	for s in symbols:
		x=n.get_short_ratio(s)
		print x
	pass