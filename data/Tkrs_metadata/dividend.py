import requests
from bs4 import BeautifulSoup
import os

# load the tickers from the ticker file
ticker_list = [x.strip() for x in open("tickers.txt", "r").readlines()]

print("Number of equities: ", len(ticker_list))

for ticker in ticker_list:
    print(ticker)
    # URL to get dividend data
    url = "https://www.tickertech.net/bnkinvest/cgi/?n=2&ticker=" + ticker + "&js=on&a=historical&w=dividends2"
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "lxml")

    # find the one table we are looking for with dividend data, and get all table rows from that table
    dividend_rows = soup.find("table", attrs={"bgcolor": "#EEEEEE"}).find_all("tr")

    # set the output file to be `$ticker.csv` in the folder `dividends`
    output_filename = "dividends\{0}.csv".format(ticker)
    # Check if the folder exists, if not make it
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    # make/open the ticker file
    output_file = open(output_filename, 'w')

    for row in dividend_rows:
        # extract all the strings from the row
        columns = list(row.stripped_strings)
        # remove the lingering javascript rows
        columns = [x for x in columns if 'allow' not in x]
        # if there are only 2 columns (date, ratio), the data is correct and we can write it
        if len(columns) == 2:
            output_file.write("{0}, {1} \n".format(columns[0], columns[1]))

    output_file.close()
