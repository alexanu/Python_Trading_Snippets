# Source https://github.com/ilcardella/MarketDataCollector/blob/master/MarketDataCollector.py

'''
You can run the script manually with

python3 MarketDataCollector.py FUNCTION INTERVAL

where FUNCTION and INTERVAL must be replaced according to the AlphaVantage API documentation.

A better solution is to setup a cron job (i.e. crontab):

crontab -e

You can setup the cron job to call the script market_data_collector which accept as input argument start and stop. This will run the script at the market closure every week day:

35 16 * * 1-5 /path/to/script/market_data_collector start

Remember to define the FUNCTION and INTERVAL inside the shell script!!!


'''


import logging
import requests
import json
import time
import os
import datetime as dt
import sys

MARKET_JSON = 'markets.json'
DB_ROOT_FOLDER = 'markets_db'
MARKET_COUNTRY = 'LON' # See AlphaVantage for this
TIMEOUT = 10

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s: %(message)s")

def get_historic_price(marketId, function, interval, apiKey):
    intParam = '&interval={}'.format(interval)
    if interval == '1day':
        intParam = ''
    url = 'https://www.alphavantage.co/query?function={}&symbol={}{}&outputsize=full&apikey={}'.format(function, marketId, intParam, apiKey)
    data = requests.get(url)
    return json.loads(data.text)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logging.error("Wrong number of arguments")
        exit()

    # Read input arguments
    AV_FUNCTION = sys.argv[1]
    AV_INTERVAL = '1day'
    if AV_FUNCTION == 'TIME_SERIES_INTRADAY':
        AV_INTERVAL = sys.argv[2]
        if len(AV_INTERVAL) == 0 or AV_INTERVAL is None:
            logging.error("Wrong number of arguments")
            exit()

    AV_APIKEY = ''
    try:
        # Read AlphaVantage API KEY
        with open('.api_key', 'r') as f:
            key = f.readline()
            AV_APIKEY = key
    except IOError:
        logging.error(".api_key file not found!")
        exit()

    logging.info("MarketDataCollector started collection {} {}".format(AV_FUNCTION, AV_INTERVAL))
    try:
        # Create db root folder if does not exist
        os.makedirs(DB_ROOT_FOLDER, exist_ok=True)
        with open(MARKET_JSON, 'r') as fileReader:
            markets = json.load(fileReader)
            logging.info("Markets json loaded")
            for market in markets:
                time.sleep(TIMEOUT)
                # Extract market Id
                marketId = market['instrument']['marketId']
                # Convert the string for alpha vantage
                marketIdAV = '{}:{}'.format(MARKET_COUNTRY, marketId.split('-')[0])

                # Fetch historic price
                newData = get_historic_price(marketIdAV, AV_FUNCTION, AV_INTERVAL, AV_APIKEY)

                # Safety check
                if 'Error Message' in newData or 'Information' in newData:
                    logging.error("Skipping {}: {}".format(marketId, newData))
                    continue

                # Build market folder and file names
                marketFolder = '{}/{}'.format(DB_ROOT_FOLDER, marketId)
                marketFilename = '{}_{}.json'.format(marketId, AV_INTERVAL)
                marketFilepath = '{}/{}'.format(marketFolder, marketFilename)

                # If file exist update existing data with the new one datapoint
                if os.path.exists(marketFilepath):
                    with open(marketFilepath, 'r+') as fileHandler:
                        storedData = json.load(fileHandler)
                        # Update stored dictionary with the new data
                        storedData.update(newData)
                        # Write file
                        fileHandler.seek(0)
                        json.dump(storedData, fileHandler, indent=4, separators=(',', ': '), sort_keys=True)
                else:
                    # Create new file for the market with the fetched data
                    os.makedirs(marketFolder, exist_ok=True)
                    with open(marketFilepath, 'w') as fileWriter:
                        json.dump(newData, fileWriter, indent=4, separators=(',', ': '), sort_keys=True)
                logging.info("Market {} processed succesfully".format(marketId))
            logging.info("Process complete")
    except Exception as e:
        logging.error(e)