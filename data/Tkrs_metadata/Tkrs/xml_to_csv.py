
# Yahoo Finance Stocklist Scraper

'''
converting a fetched XML file of stocks/industries into a .csv file ...
... and mapping each stock to it's country/exchange.

The script requires a *full_stocklist_xml.xml* file 
can be fetched via: http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.industry%20where%20id%20in%20(select%20industry.id%20from%20yahoo.finance.sectors)&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys

JSON mapping file maps the various exchanges and their host countries to their Yahoo Finance extension 
as [Detailed Here](http://finance.yahoo.com/exchanges)
'''

import csv
import json
from lxml import etree

#Stocklist obtained from: http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.industry%20where%20id%20in%20(select%20industry.id%20from%20yahoo.finance.sectors)&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys
xml = open('full_stocklist_xml.xml', 'r').read()
tree = etree.fromstring(xml)

json_ex_map = open('exchange_symbol_map.json', 'r').read()
exchange_map = json.loads(json_ex_map)

print "Converting..."

count = 0
csv_output = []
for industry in tree.iter('industry'):
	industry_name = industry.attrib['name']
	for company in industry.findall('company'):
		co_name = company.attrib['name']
		co_symbol = company.attrib['symbol']

		co_ex_country = "United States of America" #NASDQ, DOW, NYSE don't have symbols, so assume it's one of those if no symbol found
		co_ex_name = ""
		for ex_country in exchange_map:
			for ex_name in exchange_map[ex_country]:
				ex_symbol = exchange_map[ex_country][ex_name]

				if '.' in co_symbol:
					if ('.' + co_symbol.split('.')[1].upper()) == ex_symbol.upper():
						co_ex_country = ex_country
						co_ex_name = ex_name

		csv_entry = (co_symbol.encode('utf-8'), co_name.encode('utf-8'), industry_name.encode('utf-8'), co_ex_country.encode('utf-8'), co_ex_name.encode('utf-8'))
		csv_output.append(csv_entry)
		count += 1

csv_file = open('yahoo_stocklist.csv', 'wb')
writer = csv.writer(csv_file)
writer.writerow(('YAHOO TICKER', 'COMPANY NAME', 'INDUSTRY', 'COUNTRY TRADED', 'EXCHANGE'))
for stock in csv_output:
	writer.writerow(stock)
csv_file.close()

print "Finished!", count, "stocks were converted to yahoo_stocklist.csv [Symbol, Name, Industry]"