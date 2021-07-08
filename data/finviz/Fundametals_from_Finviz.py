symbols = ['AAPL', 'MMM', 'INTC', 'JPM', 'DE', 'WFC', 'BK', 'PM', 'HD', 'GE']
root_path = "C:/..."


import bs4 as bs
import pandas as pd
import numpy as np
import datetime as dt
import calendar
import os
import requests
import inspect
import re


    def fundis(rate, method):
        method = method.lower()
            weights = pd.read_csv(root_path + '/Daily Data/Portfolio/Portfolio Weights.csv', index_col=0)

            #Create dataframe for fundamentals
            columns = ["Trailing P/E","Forward P/E","PEG","Price/Book","Beta","Dividend Yield"]
            fundamental_df = pd.DataFrame(columns=columns, index=symbols[:3])

            count = 0

            #Ensure proper notation
            fsymbols = []
            for symbol in symbols:
                symbol = symbol.replace('_', '-')
                fsymbols.append(symbol)

            #Web scrape data
            for symbol in fsymbols:
                f_data = []
                url = 'https://finviz.com/quote.ashx?t=' + symbol
                r = requests.get(url)
                html = r.text

                try:
                    string = html.split('P/E</td>')[1].split('</b></td>')[0]
                    pe = re.findall("(\d+\.\d{1,3})", string)
                    fundamental_df.at[symbols[count],"Trailing P/E"] = pe[0]
                except IndexError:
                    print 'Element not found for:' + symbol

                try:
                    string = html.split('Forward P/E')[ 1].split('</span></b>')[0]
                    fpe = re.findall("(\d+\.\d{1,3})", string)
                    fundamental_df.at[symbols[count],"Forward P/E"] = fpe[0]
                except IndexError:
                    print 'Element not found for:' + symbol

                try:
                    string = html.split('PEG<')[1].split('</b></td>')[0]
                    peg = re.findall("(\d+\.\d{1,3})", string)
                    fundamental_df.ix[symbols[count], "PEG"] = peg[0]
                except IndexError:
                    print 'Element not found for:' + symbol

                try:
                    string = html.split('P/B</')[1].split('</b></td>')[0]
                    pb = re.findall("(\d+\.\d{1,3})", string)
                    fundamental_df.ix[symbols[count], "Price/Book"] = pb[0]
                except IndexError:
                    print 'Element not found for:' + symbol

                try:
                    string = html.split('Beta<')[1].split('</b></td>')[0]
                    beta = re.findall("(\d+\.\d{1,3})", string)
                    fundamental_df.ix[symbols[count], "Beta"] = beta[0]
                except IndexError:
                    print 'Element not found for:' + symbol

                try:
                    string = html.split('Dividend %')[1].split('</b></td>')[0]
                    div = re.findall("(\d+\.\d{1,3}%)", string)
                    fundamental_df.ix[symbols[count], "Dividend Yield"] = beta[0]
                except IndexError:
                    print 'Element not found for:' + symbol

                count += 1

            fundis = fundamental_df.apply(pd.to_numeric)
            fundis["Dividend Yield"] = fundis["Dividend Yield"] / 100
            weights = weights["Weight"].tolist()
            temp = fundis.multiply(weights, axis=0)

            # Output
            weighted_metrics = temp.sum()

 
