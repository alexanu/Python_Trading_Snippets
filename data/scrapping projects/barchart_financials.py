import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_income_statement(ticker):
    url = "https://www.barchart.com/stocks/quotes/"+ticker+"/income-statement/annual"
    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find_all('table')[0]
        data = pd.read_html(table.prettify())
        data = data[0]
        data.iloc[0][0] = 'Year'
        data.columns = data.iloc[0]
        data = data.reindex(data.index.drop(0))
        data = data.transpose()
        data = data.reset_index()
        data.columns = data.iloc[0]
        data = data.reindex(data.index.drop(0))
        data['Ticker'] = ticker
        cols = data.columns.tolist()
        cols = ['Ticker']+cols[0:len(cols)-1]
        data = data[cols]
        data['Year'] = data['Year'].apply(lambda x: x.split('-')[1])
        data['Ebitda'] = data['Ebitda'].apply(lambda x: int(''.join(e for e in x if e.isalnum())))
        data['Sales'] = data['Sales'].apply(lambda x: int(x))
        data['Ebitda_Margin'] = data['Ebitda']/data['Sales']
        return data

def get_cashflow(ticker):
    url = "https://www.barchart.com/stocks/quotes/"+ticker+"/cash-flow/annual"
    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find_all('table')[0]
        data = pd.read_html(table.prettify())
        data = data[0]
        data[0][0] = 'Year'
        data.columns = data.iloc[0]
        data = data.reindex(data.index.drop(0))
        data = data.dropna()
        data = data.transpose()
        data = data.reset_index()
        data.columns = data.iloc[0]
        data = data.reindex(data.index.drop(0))
        data['Year'] = data['Year'].apply(lambda x: x.split('-')[1]) 
        data['Ticker'] = ticker
        cols = data.columns.tolist()
        cols = ['Ticker']+cols[0:len(cols)-1]
        data = data[cols]
        return data