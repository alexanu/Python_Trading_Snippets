#!/usr/bin/python
# -*- coding: utf-8 -*-

#==========================================================
#========== quandl_Python_ContractsDownload.py ============
#==========================================================

# Purpose
#----------------------------------------------------------
# Using Quandl, Python and its requests package connect to Quandl's API and
# data sets to pull contracts on futures for a specific time frame. Note
# currently this script takes the ticker as the argument. Later we can adjust it
# to take more inputs and be more user friendly i.e. prompt for contract name,
# periods, delivery dates, etc.

from __future__ import print_function

import matplotlib.pyplot as plt
import pandas as pd
import requests as rq

def construct_Futures_Symbols(
        symbol, start_year=2010, end_year=2014
        ):
    """
    This function will generate the list of future contract codes for a
    particular symbol and timeframe for download
    """
    futures = []
    
    # March, June, September and December delivery codes
    months = 'HMUZ'
    for y in range(start_year, end_year+1):
            for m in months:
                    futures.append("%s%s%s" % (symbol, m, y))
    return futures

def quandl_Contract_Download(contract, dl_dir):
    """
    Download the individual futures contracts from Quandl and store in the
    dl_dir folder. Note that the auth_token is necessary for tracking call
    count (limit of 500/day for non-premium account) and can be obtained
    readily upon sign-up
    """
    # Format the API call from the contract and auth_token
    #https://www.quandl.com/api/v3/datasets/CME/ESZ2014.csv?api_key=xEijovVJdKK_nGUGzEMG
    api_call = "https://www.quandl.com/api/v3/datasets/"
    # Contracts desired
    api_call += "CME/%s.csv" % contract
    # Parameters (put in authentification key for 500 call account here
    params = "?auth_token=xEijovVJdKK_nGUGzEMG&sort_order=asc"
    # Put together:
    full_url = "%s%s" % (api_call, params)
        
    # Use requests to download the data from Quandl with the url
    data = rq.get(full_url).text

    # Store data to appropriate dir
    csv = open('%s/%s.csv' % (dl_dir, contract), 'w')
    csv.write(data)
    csv.close()

def download_AllHistoricalContracts(
    symbol, dl_dir, start_year=2010, end_year=2014
    ):
    """
    Using construct_Futures_Symbols and quandl_Contract_Download we can now
    assemble and download all desired contracts
    """
    contracts = construct_Futures_Symbols(symbol, start_year, end_year)
    
    for c in contracts:
        print("Downloading contract: %s" % c)
        quandl_Contract_Download(c, dl_dir)

# For commandline execution
if __name__ == "__main__":
    import sys

    # NOTE: Dirs must be created before we execute the automated download
    symbol = str(sys.argv[1])
    dl_dir = 'Futures/%s' % symbol

    # Designate start and end year. Change if necessary
    start_year = 2010
    end_year = 2014

    # Download the contracts into the dir
    download_AllHistoricalContracts(
            symbol, dl_dir, start_year, end_year)

    # Verification: open up a single contract with read_csv and plot the settle
    # price
    series = pd.io.parsers.read_csv(
            "%s/ESH2010.csv" % dl_dir, index_col="Date"
    )
    series["Settle"].plot()
    plt.show()

