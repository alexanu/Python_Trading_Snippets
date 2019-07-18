# you will need to know how to set up your postgresql database

from bs4 import BeautifulSoup
import requests
import re
import psycopg2 # to execute SQL statements
import urllib.request, urllib.error, urllib.parse


def get_table_headers():
	url = "http://www.finviz.com/screener.ashx?v=111&f=cap_nano&r=1"
	content = urllib.request.urlopen(url).read()
	soup = BeautifulSoup(content)
	table_headers = []
	for th in soup.select(".table-top"):
		table_headers.append(th.get_text())
	table_headers.insert(1, "Ticker")
	return table_headers


def get_rows_from_soup(soup, table_headers):
	table_row_data = []
	counter = 0
	row_data = {}
	for tr in soup.select(".screener-body-table-nw"):
		row_data[table_headers[counter]] = tr.get_text()
		counter += 1
		if counter >= len(table_headers):
			counter = 0
			table_row_data.append(row_data)
			row_data = {}
	return table_row_data


def get_data():
	headers = get_table_headers()
	all_data = []
	ended = False
	initial_number = 1
	while not ended:
		url = "http://www.finviz.com/screener.ashx?v=111&f=cap_nano&r={}".format(
			initial_number
		)
		content = urllib.request.urlopen(url).read()
		soup = BeautifulSoup(content)
		all_data += get_rows_from_soup(soup, headers)
		print(len(all_data))
		initial_number += 20
		print(type(content))
		print(initial_number)
		if not re.findall(b"<b>next</b>", content):
			ended = True
	return all_data


DB = psycopg2.connect(dbname="xx", user="xx")
cursor = DB.cursor()
table_name = "market_cap"
cursor.execute("DROP TABLE IF EXISTS " + table_name)
cursor.execute("""
	CREATE TABLE {} (
		no INT, ticket VARCHAR(10), company VARCHAR(255), sector VARCHAR(255), industry VARCHAR(255),
		country VARCHAR(255), market_cap FLOAT(4), price FLOAT(4), change FLOAT(4), volume INT, pe FLOAT(4),
	);
""".format(table_name))
DB.commit()

data = get_data()

from pprint import pprint
pprint(data)

for row in data:
	cursor.execute("""
	INSERT INTO {}(no, ticket, company, sector, industry, country, market_cap, price, change, volume, pe)
	VALUES({}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {})
	""".format(
		table_name, row['No.'], row['Ticker'], row['Company'], row['Sector'], row['Industry'], row['Country'],
		row['Market Cap'][:-1], row['Price'], row['Change'][:-1], row['Volume'].replace(',', ""),
		"NULL" if row['P/E'] == "-" else row['P/E'],	
	))
DB.commit()

avg = cursor.execute('select AVG(market_cap) from market_cap;')

#results = cursor.fetchall()
#print(results)
#DB.close()
