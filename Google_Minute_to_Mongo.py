import urllib.request
import datetime as dt
import pymongo
import pandas as pd


class StockTickerData:
    """
    Maintain a database of historical minute stock market data for later retrieval in pandas dataframe
    format.  The recommended usage is as follows:
     1) create a text file that contains a comma separated list of ticker symbols of interest
     2) create a daily scheduled task that calls 'load_ticker_data_to_db_from_list' function
     to load the local database. (see 'collectTickerDataSample.py' for an example)
     Local database will build over time
     3) Utilize 'get_quotes' to retrieve ticker data for a specific stock.
     (see 'useTickerDataSample.py' for an example)


### Development and DB setup
python -m pip install pymongo

install mongdo db as outlined in https://docs.mongodb.com/manual/installation/

create mongo config file then install mongodb service (example below)
"C:\Program Files\MongoDB\Server\3.2\bin\mongod.exe" --config "E:\Data\mongo_data\mongod.cfg" --install

to start service: 'net start MongoDB'
to stop service: 'net stop MongoDB'



    """

    def __init__(self, db_connection_str):
        """ Initialize the StockTickerData class and setup constraints on the database
        :param db_connection_str: mongo db connection string. ex: 'mongodb://localhost:27017/'
        """
        mongo_client = pymongo.MongoClient(db_connection_str)
        db = mongo_client['quant_database']
        self.quotes = db['quotes']
        self.quotes.create_index([("symbol", pymongo.ASCENDING), ("period", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], unique=True)

    @staticmethod
    def get_google_finance_ticker_data(symbol, period, days_history):
        """ retrieve historical stock quote data from the google finance site and return info as a list
        Historical data is only available for a maximum of 15 days.
        :param symbol: Ticker symbol of stock data to be retrieved
        :param period: Number of seconds in between ticker data readings. An entry of 60 will collect
        minute data
        :param days_history: number of previous days that stock data will be loaded for.  Currently the
         maximum historical days available from a free data source is 15
        :return: list containing historical ticker data info
        """
        print('get_google_finance_ticker_data(', symbol, ')')
        symbol = symbol.strip().upper()
        url_root = 'http://www.google.com/finance/getprices?i='
        url_root += str(period) + '&p=' + str(days_history)
        url_root += 'd&f=d,o,h,l,c,v&df=cpct&q=' + symbol
        response = urllib.request.urlopen(url_root)
        raw_data = response.read().decode()
        data = raw_data.split('\n')
        all_ticker_data = []
        end = len(data)
        day_ticker = None
        for i in range(7, end):
            cdata = data[i].split(',')
            if 'a' in cdata[0]:
                # first one record anchor timestamp
                if day_ticker:
                    all_ticker_data.append(day_ticker)
                anchor_stamp = cdata[0].replace('a', '')
                cts = int(anchor_stamp)
                day_ticker = {"symbol": symbol, "period": period, "date": dt.datetime.fromtimestamp(float(cts)), "ticks": []}
            elif len(cdata) >= 6:
                coffset = int(cdata[0])
                quote = {'i': coffset, 'o': float(cdata[1]), 'h': float(cdata[2]), 'l': float(cdata[3]), 'c': float(cdata[4]), 'v': float(cdata[5])}
                day_ticker['ticks'].append(quote)
        if day_ticker:
            all_ticker_data.append(day_ticker)
        return all_ticker_data

    def load_ticker_data_to_db(self, symbol, period, days_history):
        """ Loads historical stock market ticker data into a local database for the specified tciker symbol
        :param symbol:  Ticker symbol of stock data to be loaded
        :param period: Number of seconds in between ticker data readings. An entry of 60 will collect
        minute data
        :param days_history: number of previous days that stock data will be loaded for.  Currently the
         maximum historical days available from a free data source is 15, so putting a number larger that this will
         have no effect
        :return: None
        """
        print('load_ticker_data_to_db({},{},{})'.format(symbol, period, days_history))
        symbol = symbol.upper()
        for day_quote in self.get_google_finance_ticker_data(symbol, period, days_history):
            datetime_start = day_quote['date'].replace(hour=0, minute=0, second=0)
            datetime_end = datetime_start.replace(hour=17, minute=0, second=0)
            exists_in_db = self.quotes.find({'symbol': symbol, 'period': period, 'date': {'$gt': datetime_start, '$lt': datetime_end}}).count() > 0
            if not exists_in_db and dt.datetime.now() > datetime_end:
                try:
                    self.quotes.insert_one(day_quote)
                    print("success insert quote: ", day_quote['symbol'], '  ', day_quote['date'].date())
                except Exception as e:
                    print("Could not insert quote :", "Unexpected error:", e)

    def load_ticker_data_to_db_from_list(self, filename, period, days_history):
        """ Loads historical stock market ticker data into a local database for all ticker symbols
        in the specified text file.
        :param filename: filename of text file containing a comma separated list of stock market
        ticker symbols for which data is to be loaded into the database.
        :param period: Number of seconds in between ticker data readings. An entry of 60 will collect
        minute data
        :param days_history: number of previous days that stock data will be loaded for.  Currently the
         maximum historical days available from a free data source is 15, so putting a number larger that this will
         have no effect
        :return: None
        """
        file = open(filename, "r")
        for line in file.readlines():
            for symbol in line.split(','):
                symbol = symbol.strip()
                if symbol:
                    self.load_ticker_data_to_db(symbol, period, days_history)
        file.close()

    def get_quotes(self, symbol, period, date_start=dt.date(1900, 1, 1), date_end=dt.date.today()):
        """ Retrieve and return a pandas dataframe containing quotes for the specified
        ticker symbol and constraint parameters.  Data will only be returned if it has previously
        been collected and stored in the local database.  Data for a single day can be retrieved if
        both optional data parameters 'date_start' and 'date_end' are set to the same day. If no
        date parameters are set, then all available data will be returned.
        :param symbol: Ticker symbol of stock data to be retrieved (EX SPY, MSI, ...)
        :param period: number of seconds between quote readings.  For minute data this must be 60
        :param date_start: optional argument indicating first date for quote data
        If missing it will include first available quote data
        :param date_end: optional argument indicating last date for quote data.
        If missing it defaults to current date
        :return: pandas dataframe containing specified quotes
        """
        symbol = symbol.strip().upper()
        datetime_start = dt.datetime(date_start.year, date_start.month, date_start.day, 0, 0)
        datetime_end = dt.datetime(date_end.year, date_end.month, date_end.day, 17, 0, 0)
        day_quotes = self.quotes.find({'symbol': symbol, 'period': period, 'date': {'$gt': datetime_start, '$lt': datetime_end}})
        raw_quotes_list = []
        for quote in day_quotes:
            for tick in quote['ticks']:
                ts = quote['date'] + dt.timedelta(seconds=(tick['i'] * period))
                raw_quotes_list.append([ts, tick['o'], tick['h'], tick['l'], tick['c'], tick['v']])
        df = pd.DataFrame(raw_quotes_list)
        df.columns = ['ts', 'o', 'h', 'l', 'c', 'v']
        df.index = df.ts
        del df['ts']
        return df

"""
Example of Storing historical stock ticker data for a set of stocks as identified
in file 'ticker_symbols.txt' into a local mongo database for later retrieval.
In this example the previous 5 days worth of minute data are stored
This routine is best run as a daily scheduled task to collect historical data
over an extended period of time.
"""
stockTickerData = stockTickerData.StockTickerData('mongodb://localhost:27017/')
stockTickerData.load_ticker_data_to_db_from_list("ticker_symbols.txt", 60, 5)

"""
Example usage of ticker data that extracts minute ticker data for 'SPY' for
a single day and then prints the entire pandas dataframe result to the console
"""
stockTickerData = stockTickerData.StockTickerData('mongodb://localhost:27017/')
date_start = dt.date(2017, 1, 6)
date_end = dt.date(2017, 1, 6)
minute_quotes_df = stockTickerData.get_quotes('SPY', 60, date_start=date_start, date_end=date_end)
print(minute_quotes_df)

