
# Schema is here:
# https://bubbl.us/NDc3NDc4NC8zODMxNzgzLzJlMDZmNTliOTgwYjYyN2U3NGZmZDRmZWQwNWY5Zjg2@X?utm_source=shared-link&utm_medium=link&s=10033173

from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
import urllib.request
import html5lib
import ast
import re

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


"""
Fetch expenses and holdings for a single ETF.
Three potential sources (etfdailynews, Zacks, Bloomberg) are used to scrape data.
"""
class ETFData: 

	def __init__(self, ticker): 
		self.ticker = ticker
		self.holdings = None
		self.num_holdings = None
		self.expense_ratio = None

	@staticmethod
	def convert_percent(pct): 
		if type(pct)==float:
			return pct/100
		return float(pct.rstrip('%'))/100


	def get_expenses(self, ticker): 
		"""
		Fetch expense ratio from Bloomberg
		"""
		url = "http://bloomberg.com/quote/" + str(ticker) + ":US"
		html = urllib.request.urlopen(url).read()
		soup = bs(html, "lxml")
		ratio_pattern = re.compile(r'Expense Ratio')		
		percent_pattern = re.compile(r'%$')
		ratio = soup.find('div', text=ratio_pattern).find_next_sibling().text.rstrip('\n ').lstrip('\n ')
		self.expense_ratio = self.convert_percent(ratio)

		"""
		Y Finance version no longer working due to changes to site
		"""
	# def get_expenses(self, ticker): 
	# 	"""
	# 	Fetch expense ratio from Y Finance
	# 	"""
	# 	url = 'http://finance.yahoo.com/q/pr?s='+str(ticker)+'+Profile'
	# 	html = urllib.request.urlopen(url).read()
	# 	soup = bs(html, "lxml")
	# 	percent_pattern = re.compile(r'%$')		
	# 	ratio = soup.find('td', text='Annual Report Expense Ratio (net)').find_next_sibling('td', text=percent_pattern).text
	# 	self.expense_ratio = self.convert_percent(ratio)

	def holdings_first_parse(self, ticker): 
		"""
		Default API for fetching ETF holdings. Generates a pandas DataFrame with name, 
		ticker, allocation for each holding. Equity ETFs only.
		"""
		url='http://etfdailynews.com/tools/what-is-in-your-etf/?FundVariable=' + str(ticker)
		# decode to unicode, then re-encode to utf-8 to avoid gzip
		html = urllib.request.urlopen(url).read().decode('cp1252').encode('utf-8')
		soup = bs(html, "lxml")

		# Build Holdings Table - find the only tbody element on the page
		holdings_table = "<table>" + str(soup.tbody).lstrip('<tbody>').rstrip('</tbody') + "</table>"

		# Fetch expense ratio
		ratio_pattern = re.compile(r'Expense Ratio')		
		percent_pattern = re.compile(r'%$')		
		td = soup.find('td', text=ratio_pattern)		
		if not td: 		
			return False		
		# find_next_siblings returns a Result Set object - take first matching item and strip the tags
		expense_ratio = str(td.find_next_siblings('td', text=percent_pattern)[0]).lstrip('<td>').rstrip('</td>')		
		self.expense_ratio = self.convert_percent(expense_ratio)

		# convert to DataFrame
		df = pd.read_html(holdings_table)[0]
		df.columns = ['name', 'ticker', 'allocation']
		df['allocation'] = df.allocation.map(lambda x: self.convert_percent(x))
		self.holdings, self.num_holdings = df, len(df)

	def holdings_second_parse(self, ticker): 
		"""
		Backup source for holdings (zacks.com). Slower (data is parsed from string,
		not read into pandas from table element as in holdings_first_parse) and less reliable data. Output is in same format.
		Zacks may not have data for some ETFs.
		"""

		def clean_name(str_input): 
			if "<span" in str_input:
				soup = bs(str_input, "lxml")
				return soup.find('span')['onmouseover'].lstrip("tooltip.show('").rstrip(".');")
			return str_input

		def clean_ticker(str_input):
			soup = bs(str_input, "lxml")
			return soup.find('a').text

		def clean_allocation(str_input): 
			if str_input == "NA":
				return 0
			return float(str_input)/100

		url = 'https://www.zacks.com/funds/etf/' + str(ticker) + '/holding'
		html = urllib.request.urlopen(url).read().decode('cp1252')
		str_start, str_end = html.find('data:  [  [ '), html.find(' ]  ]')
		if str_start == -1 or str_end == -1: 
			# If Zacks does not have data for the given ETF
			print("Could not fetch data for {}".format(ticker))
			return
		list_str = "[["+html[(str_start+12):str_end]+"]]"
		holdings_list = ast.literal_eval(list_str)

		df = pd.DataFrame(holdings_list).drop(2,1).drop(4,1).drop(5,1)
		df.columns = ['name', 'ticker', 'allocation']
		df['allocation'] = df.allocation.map(lambda x: clean_allocation(x))
		df['name'] = df.name.map(lambda x: clean_name(x))
		df['ticker'] = df.ticker.map(lambda x: clean_ticker(x))
		self.holdings, self.num_holdings = df, len(df)

		self.get_expenses(ticker)
		# print(df['allocation'].sum())

	@classmethod
	def get(cls, ticker, parse_method): 
		ticker = ticker.upper()
		data = cls(ticker)
		result = data.holdings_second_parse(ticker) if parse_method=="second" else data.holdings_first_parse(ticker)
		return data



"""
Build a portfolio of ETFs and calculate the weight of individual securities within the portfolio 
and its overall expense ratio.
"""
class Portfolio: 
	def __init__(self, parse_method="first"): 
		self.parse_method = parse_method
		self.port_etfs = pd.DataFrame({
			'etf':[], 
			'allocation':[], 
			'true_weight':[], 
			'holdings':[], 
			'expenses':[], 
			'weighted_expenses':[]
			})
		self.num_etfs = 0
		self.num_holdings = 0
		self.port_holdings = None
		self.port_expenses = None

	def display_portfolio(self): 
		return self.port_etfs[['etf', 'allocation', 'true_weight', 'expenses', 'weighted_expenses']]

	def calculate_weight(self, allocation): 
		# current total user-entered allocation 
		total_allocation = self.port_etfs['allocation'].sum(axis=0) + allocation

		# Calculate current real allocation
		def true_weight(alloc_input):
			weight_factor = 1/total_allocation
			return weight_factor*alloc_input

		# Update real allocation and weighted expenses for each row
		self.port_etfs['true_weight'] = self.port_etfs.allocation.map(lambda x: true_weight(x))
		self.port_etfs['weighted_expenses'] = self.port_etfs['expenses']*self.port_etfs['true_weight']

		return true_weight(allocation)

	def add(self, ticker, allocation): 
		allocation = float(allocation)/100
		weight = self.calculate_weight(allocation)
		etf = ETFData.get(ticker, self.parse_method)
		# if information not found, break
		if not etf.expense_ratio:
			return
		weighted_expenses = etf.expense_ratio*weight

		# New ETF row
		self.port_etfs = self.port_etfs.append({'etf':ticker, 
			'allocation':allocation, 
			'true_weight': weight, 
			'holdings':etf.holdings, 
			'expenses':etf.expense_ratio, 
			'weighted_expenses':weighted_expenses}, ignore_index=True).sort_values(by='etf')
		self.num_etfs += 1
		print("{} added".format(ticker))

	def get_port_expenses(self): 
		self.port_expenses = self.port_etfs['weighted_expenses'].sum()
		return self.port_expenses

	def get_stock_allocation(self):
		"""
		Returns DataFrame with portfolio weight of all individual constituent stocks
		""" 
		all_holdings = pd.DataFrame({'name':[], 'ticker':[], 'allocation':[], 'portfolio_weight':[]})

		for each in zip(self.port_etfs['holdings'], self.port_etfs['true_weight']):
			each[0]['portfolio_weight'] = each[0].allocation * each[1]
			all_holdings = all_holdings.append(each[0], ignore_index=True)

		# Convert holdings with NaN or Cash tickers to same symbol to be grouped together in next step
		all_holdings.ix[all_holdings.ticker.isin(["CASH_USD", "USD", np.nan]), 'ticker'] = 'N/A'
		all_holdings.ix[all_holdings.ticker=="N/A", 'name'] = 'Cash/Other'

		# Drop rows with NaN portfolio weight by only saving finite values
		all_holdings = all_holdings[np.isfinite(all_holdings['portfolio_weight'])]

		# Group holdings by ticker and add respective weights, match with names
		names = all_holdings.drop_duplicates(subset='ticker')[['name','ticker']]
		grouped_holdings = pd.DataFrame(all_holdings.groupby('ticker')['portfolio_weight'].sum()).reset_index()
		grouped_holdings = pd.merge(grouped_holdings, names, left_on='ticker', right_on='ticker', how='inner').sort_values(by='portfolio_weight', ascending=False).reset_index()

		self.port_holdings, self.num_holdings = grouped_holdings, len(grouped_holdings)
		return grouped_holdings 


	"""
	Print all relevant information for the ETF portfolio
	"""
	def report(self): 
		print()
		print("Number of ETFs added to portfolio: {}".format(self.num_etfs))
		print("Overall portfolio expense ratio: {}".format(str(self.port_expenses*100)+'%'))
		print("Number of total individual holdings: {}".format(self.num_holdings))
		print("Top 50 holdings: ")
		print(self.port_holdings[['name', 'portfolio_weight']].head(50))



if __name__ == "__main__": 

	# a = Portfolio(parse_method="second")

	a = Portfolio()
	a.add('VTI', 20)
	a.add('SPY', 20)
	a.add('XLK', 10)
	a.add('IBB', 25)
	a.add('HDV', 15)
	a.add('MGK', 5)
	a.add('IYH', 5)
	a.get_stock_allocation()
	a.get_port_expenses()
a.report()
