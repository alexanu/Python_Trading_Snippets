import requests
from bs4 import BeautifulSoup
import os

# load the tickers from the ticker file
ticker_list = [x.strip() for x in open("tickers.txt", "r").readlines()]

print("Number of equities: ", len(ticker_list))

for ticker in ticker_list:
    print(ticker)
    # url to get split data
    url = "https://www.stocksplithistory.com/?symbol="+ticker
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "lxml")

    # the only table with these attributes is the table with the data, so we use them to find it
    table = soup.find('table', attrs={'border': '0', 'width': '208', 'style':'font-family: Arial; font-size: 12px'})
    # save the `$ticker split history, table` header
    header = table.find("td")
    # get all the table rows; the split data
    split_rows = table.find_all("tr")

    # set the output file to be `$ticker.csv` in the folder `splits`
    output_filename = "splits\{0}.csv".format(ticker)
    # Check if the folder exists, if not make it
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    # make/open the ticker file
    output_file = open(output_filename, 'w')

    # extract the strings from the header and write it
    columns = list(header.stripped_strings)
    if len(columns) == 2:
        output_file.write("{0}, {1} \n".format(columns[0], columns[1]))

    for row in split_rows:
        # extract the strings from thr row
        columns = list(row.stripped_strings)
        # if the length is 2, it is in the right format (date, split), so we write it
        if len(columns) == 2:
            output_file.write("{0}, {1} \n".format(columns[0], columns[1]))

    output_file.close()
