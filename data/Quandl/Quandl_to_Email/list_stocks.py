import quandl
import os

quandl.ApiConfig.api_key = os.environ["QUANDL_API_KEY"]

nse = quandl.Database('NSE')

# retrieve first page of 100 stocks in NSE
nse_stocks_page = nse.datasets()

pageCount = 1

# restricting the pageCount not to exceed daily call limit

while nse_stocks_page.has_more_results() and pageCount < 7:
    for nse_stock in nse_stocks_page:
        print("{0}\t\t{1}".format(nse_stock.code, nse_stock.name))

    pageCount = pageCount + 1
    nse_stocks_page = nse.datasets(params = {"page":pageCount})
