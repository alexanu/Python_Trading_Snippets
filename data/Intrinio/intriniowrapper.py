import logging
import inspect #for argument logging https://stackoverflow.com/questions/10724495/getting-all-arguments-and-values-passed-to-a-python-function
import requestswrapper
import pandas as pd
import io
import time
from datetime import datetime


class IntrinioWrapper():
	def __init__(self,auth=None,proxies=None,timeout=300,max_retries=50,error_codes=[500,503],internal_error_codes=[401,403,404,429]):
		"""
		auth=the user,password for the intrinio account formated as a tuple (user,password)
		proxies=if you want to use a proxy for the auth,poxy='socks5://'+user+':'+password+'@'+SOCKS5_PROXY_HOST+':'+str(SOCKS5_PROXY_PORT),proxies=dict(http=proxy,https=proxy)
		timeout=how long you want to wait for a call to finish
		max_retries= how many times to retry a call
		error_codes=what codes are wrong
		internal_error_codes=error codes for this api that requries you to stop #http://docs.intrinio.com/#response-codes
		autoupgrade = if we want to autoupgrade the intrinio subscription if we are running out of calls
		"""
		frame=inspect.currentframe()
		args, _, _, values = inspect.getargvalues(frame)
		functionname=inspect.getframeinfo(frame).function
		logging.info(functionname+str([(i, values[i]) for i in args]))

		if auth is None:
			auth=()
		if proxies is None:
			proxies={}
		self.auth=auth
		self.proxies=proxies
		self.timeout=timeout
		self.max_retries=max_retries
		self.error_codes=error_codes
		self.internal_error_codes=internal_error_codes #http://docs.intrinio.com/#response-codes
		self.connector=requestswrapper.RequestsWrapper(max_retries=self.max_retries,timeout=self.timeout,proxies=self.proxies,auth=self.auth,error_codes=self.error_codes,internal_error_codes=self.internal_error_codes)
		# NOT IN USE, NEED TO IMPLEMENT FOR THREADING 061117 self.lock=threading.Lock() #only one thread will request something at one time http://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Lock_Objects_Acquire_Release.php 
		self.api_url="https://api.intrinio.com" #the default connector for intrinio
		return

	def json_to_df(self,jsondata):
		"""
		Converts json data to a pandas dataframe and returns
		"""
		df=pd.DataFrame(jsondata)
		return df    
	def single_page_request(self,endpoint,params=None,csv=False):
		if csv is True:
			endpoint+='.csv'
		if params is None:
			params={}
		resp=self.issue_request(endpoint,params) #ask for the fist page
		if resp is None:
			return None  #code that calls this needs to know to check None, if we get a None, it failed 		
		if csv is False:
			data=resp.json()
		if csv is True:
			data=resp.text
		return data
	def single_page_request_data(self,endpoint,params=None):
		data=self.single_page_request(endpoint,params)
		if data is None or 'data' not in data:
			return None
		return data['data']
	def single_page_request_csv(self,endpoint,params=None):
		if params is None:
			params={}
		#no one should manually request this, the only reason why we would call this is for a multi page request, so this is really just a helper function
		x=self.single_page_request(endpoint=endpoint,params=params,csv=True)
		if x is None:
			return None
		x=x.splitlines()
		metadata_header=x[0]
		info={} #contains info about the metadata
		headline_metadata=metadata_header.split(',')
		for item in headline_metadata:
			info[item.split(':')[0].strip().lower()]=int(item.split(':')[1].strip())
		datalines=x[1:]
		
		d='\n'.join(datalines)
		df=pd.read_csv(io.StringIO(d),dtype=str)
		df.columns=df.columns.str.lower()
		return df,info
	def multi_page_request_csv(self,endpoint,params=None):
		#we use a csv request simply to save calls
		if params is None:
			params={}
		resp=self.single_page_request_csv(endpoint,params)
		if resp is None:
			return None
		info=resp[1]
		df=resp[0]
		if 'total_pages' in info:
			for page in range(2,info['total_pages']+1):
				params['page_number']=page
				resp=self.single_page_request_csv(endpoint,params)
				if resp is None:
					return None
				df=df.append(resp[0],ignore_index=True)
		else:
			return df.iloc[0].to_dict() #for if someone accidentaly submitted a single page request via this function
		if len(df)==0:
			return None
		return df.T.to_dict().values()
	def multi_page_request_data(self,endpoint,params=None):
		if params is None:
			params={}
		"""
		Issues a requests that contains multiple pages, will then combine all pages, and just return the 'data' part of the request
		returns a list of dicts if sucessfull, or None if the call failed
		"""
		alldata=[]
		datalist=[]
		resp=self.issue_request(endpoint,params) #ask for the fist page
		if resp is None:
			return None  #code that calls this needs to know to check None, if we get a None, it failed 
		data=resp.json()['data']
		datalist.append(data)
		total_pages=resp.json()['total_pages']
		
		for page in range(2,total_pages+1):
			params['page_number']=page
			resp=self.issue_request(endpoint,params)
			if resp is None:
				return None  #code that calls this needs to know to check None, if we get a None, it failed 
			data=resp.json()['data']
			datalist.append(data)
		for data in datalist:

			for item in data:
				alldata.append(item)
		return alldata       
		
	def issue_request(self,endpoint,params=None):
		if params is None:
			params={}
		"""
		Issue a single request to Intrinio docs can be found here http://docs.intrinio.com/#introduction
		endpoint (string) = the endpiont you want to get (/companies,/prices)
		params (dict) = the paramaters you want to pass (?identifier="AAPL")
		"""
					  
		url=self.api_url+endpoint #combine the urls so it gets to the right place
		response=self.connector.issue_request(url=url,method='GET',params=params)

		returnvalue=None
		if response is None:
			packetresp=self.connector.responses_list[-1]
			status_code=packetresp.status_code
			if status_code in [429,401,404]:
				try:
					errormessage=packetresp.json()['errors'][0]['human']
				except:
					logging.error('No error message from intrinio')
					errormessage="No error message from intrinio"
				if errormessage=="Daily Call Limit Reached":
					logging.error('NO MORE CALLS FOR TODAY')
					returnvalue=None
					logging.error('we are now done for the day, no more calls')
					exit()
				elif errormessage=="Company not found":
					returnvalue=None
				elif errormessage=="10 Minute Call Limit Reached":
					logging.error('Sleeping for 10 min')
					time.sleep(60*11)
					returnvalue=self.issue_request(endpoint,params)
				elif errormessage=="No error message from intrinio":
					returnvalue=None
				else:
					logging.error(status_code)
					logging.error(packetresp.content)
					exit()
			elif status_code==200:
				returnvalue=None
			elif status_code==403:
				logging.error(status_code)
				logging.error(packetresp.content)
				returnvalue=None #we do not have permission to view this data
			else:
				logging.error(status_code)
				logging.error(packetresp.content)
				returnvalue = None
		else:  
			returnvalue=response
		return returnvalue #return the data back
	def get_access(self):
		endpoint='/usage/access'
		result=self.single_page_request(endpoint)
		return result
	def get_current_access(self,access_code='com_fin_data'):
		endpoint='/usage/current'
		params={}
		params['access_code']=access_code
		result=self.single_page_request(endpoint,params)
		return result
	def check_access(self,access_code='com_fin_data'):
		#return true if we are good to go
		#return false if we need to increase the subscription
		data=self.get_current_access(access_code)
		current=int(data['current'])
		limit=int(data['limit'])
		if current>=limit-1:
			return False
		else:
			return True
	def get_companies(self,identifier=None,query=None,latest_filing_date=None,page_size=None,page_number=None):
		"""
		Way to get to the intrinio companies endpoint http://docs.intrinio.com/#companies
	   will always return the query as a json file
	   
		"""
		endpoint='/companies'
		params={}
		params['identifier']=identifier
		params['query']=query
		params['latest_filing_date']=latest_filing_date
		params['page_size']=page_size
		params['page_number']=page_number
		if identifier is None and page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		elif identifier is not None and page_number is None:
			result=self.single_page_request(endpoint,params)
		elif identifier is None and page_number is not None:
			result=self.single_page_request_data(endpoint,params)
		else:
			logging.error('this shold never happen')
			exit()
		return result
	def get_exchanges(self):
		"""
		Returns stock exchange list and information for all stock exchanges covered by Intrinio.
		"""
		endpoint='/stock_exchanges'
		result=self.multi_page_request_data(endpoint)
		return result
	def get_securities(self,identifier=None,query=None,exch_symbol=None,last_crsp_adj_date=None,page_size=None,page_number=None):
		"""
		Way to get to the intrinio securities endpoint, need to do
		"""
		endpoint='/securities'
		params={}
		params['identifier']=identifier
		params['query']=query
		params['exch_symbol']=exch_symbol
		params['last_crsp_adj_date']=last_crsp_adj_date
		params['page_size']=page_size
		params['page_number']=page_number
		if identifier is None and page_number is None:
			result=self.multi_page_request_csv(endpoint,params)
		elif identifier is not None and page_number is None:
			result=self.single_page_request(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result
	def get_indices(self,identifier=None,query=None,type=None,page_size=None,page_number=None):
		"""
		Returns indices list and information for all indices covered by Intrinio.
		"""
		endpoint='/indices'
		params={}
		params['identifier']=identifier
		params['query']=query
		params['type']=type
		params['page_size']=page_size
		params['page_number']=page_number
		if identifier is None and page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result        
	def get_historical_data(self,identifier,item,start_date=None,end_date=None,frequency=None,type=None,sort_order='asc',page_size=None,page_number=None):
		"""
		Returns the historical data for for a selected identifier (ticker symbol or index symbol) for a selected tag.
		"""
		endpoint='/historical_data'
		params={}
		params['identifier']=identifier
		params['item']=item
		params['start_date']=start_date
		params['end_date']=end_date
		params['frequency']=frequency
		params['type']=type
		params['sort_order']=sort_order
		params['page_size']=page_size
		params['page_number']=page_number
		if page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result          
	def get_prices(self,identifier,start_date=None,end_date=None,frequency=None,sort_order='asc',page_size=None,page_number=None):    
		"""
		Returns professional-grade historical stock prices for a security or stock market index.
		"""
		endpoint='/prices'
		params={}
		params['identifier']=identifier
		params['start_date']=start_date
		params['end_date']=end_date
		params['frequency']=frequency
		params['sort_order']=sort_order
		params['page_size']=page_size
		params['page_number']=page_number
		if page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result       
	def get_exchange_prices(self,identifier,price_date=datetime.today().date().strftime('%Y-%m-%d'),page_size=None,page_number=None):
		"""
		returns all prices from that day from the specified index (^XNAS)
		"""
		endpoint='/prices/exchange'
		params={}
		params['identifier']=identifier
		params['price_date']=price_date
		params['page_size']=page_size
		params['page_number']=page_number
		
		if page_number is None:
			result=self.multi_page_request_csv(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result
		
	def get_filings(self,page_size=None,page_number=None):
		endpoint='/filings'
		params={}
		params['page_size']=page_size
		params['page_number']=page_number
		if page_number is None:
			result=self.multi_page_request_csv(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result
	
	def get_company_filings(self,identifier,report_type=None,start_date=None,end_date=None,page_size=None,page_number=None):
		"""
		Way to get to the intrinio company filings.  This is different then the filings for the past 30 days for all companies
		"""        
		endpoint='/companies/filings'
		params={}
		params['identifier']=identifier
		params['report_type']=report_type
		params['start_date']=start_date
		params['end_date']=end_date
		params['page_size']=page_size
		params['page_number']=page_number
		
		if page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result
	def get_company_fundamentals(self,identifier,statement,type=None,date=None,page_size=None,page_number=None):
		"""
		Returns a list of available standardized fundamentals (fiscal year and fiscal period) for a given ticker and statement. Also, you may add a date and type parameter to specify the fundamentals you wish to be returned in the response.
		"""
		endpoint='/fundamentals/standardized'
		params={}
		params['identifier']=identifier
		params['statement']=statement
		params['type']=type
		params['date']=date
		params['page_size']=page_size
		params['page_number']=page_number
		
		if page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result
	def get_company_financials(self,identifier,statement,fiscal_year,fiscal_period):
		"""
		Returns professional-grade historical financial data. 
		"""
		endpoint='/financials/standardized'
		params={}
		params['identifier']=identifier
		params['statement']=statement
		params['fiscal_year']=fiscal_year
		params['fiscal_period']=fiscal_period
		
		result=self.multi_page_request_data(endpoint,params)
		return result      
	def get_standardized_tags(self,statement,template=None,identifier=None,sequence=None,page_size=None,page_number=None):
		"""
		Returns the standardized tags and labels for a given ticker, statement, and date or fiscal year/fiscal quarter.
		"""
		endpoint='/tags/standardized'
		params={}
		params['statement']=statement
		params['template']=template
		params['identifier']=identifier
		params['sequence']=sequence
		params['page_size']=page_size
		params['page_number']=page_number
		if page_number is None:
			result=self.multi_page_request_data(endpoint,params)
		else:
			result=self.single_page_request_data(endpoint,params)
		return result            
		
		
if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s %(message)s',filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO)
	pass