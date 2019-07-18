import numpy as np
import pandas as pd
import zipfile
from pytz import timezone
from codecs import decode
from datetime import datetime
from bytespec import ByteSpec
from sklearn.feature_extraction import DictVectorizer

# A collection of tools to process financial time-series data   

class TaqDataFrame:

    # Attributes users should be able to access
    loaded = False

    def __init__(self, fname, type, chunksize=10000000, process=True):
        '''
        Initializes an empty pandas dataframe for storing tick data.
        fname: file name (and directory) of original TAQ file
        type: 'master', 'quotes', 'trades', or 'bbo'
        chunksize: number of lines to read in at once
        '''
        self.fname = fname
        assert type in ['mtr', 'qts', 'trd', 'bbo']
        self.type = type
        self.chunksize = chunksize
        self.process = process
        self.df = pd.DataFrame()

    def load(self):
        '''
        Loads in data from fname.
        '''
        self.loaded = True
        with zipfile.ZipFile(self.fname) as zf:
            with zf.open(zf.namelist()[0]) as infile:
                header = infile.readline()
                datestr, record_count = header.split(b':')
                self.month = int(datestr[2:4])
                self.day = int(datestr[4:6])
                self.year = int(datestr[6:10])
                self.record_count = int(record_count)
                self.line_len = len(header)
                # Computes the time at midnight starting at date specified in spec
                # given in milliseconds since epoch
                utc_base_time = datetime(self.year, self.month, self.day)
                self.base_time = timezone('US/Eastern').\
                                                localize(utc_base_time).\
                                                timestamp()
                # Type checks the given TAQ file
                if self.type == 'mtr':
                    self.dtype = ByteSpec().mtr_col_dt
                elif self.type == 'qts':
                    self.dtype = ByteSpec().qts_col_dt
                elif self.type == 'trd':
                    self.dtype = ByteSpec().trd_col_dt
                else:
                    self.dtype = ByteSpec().bbo_col_dt

                # Iterate through infile by chunksize

                while True:
                    bytes = infile.read(self.chunksize*self.line_len)
                    if not bytes:
                        break
                    rows = len(bytes) // self.line_len
                    records = np.ndarray(rows, dtype=self.dtype, buffer=bytes)

                    # With smaller chunksizes, the constant reassignment of self.df may be slow
                    self.df = self.df.append(pd.DataFrame(records), ignore_index=True)

        assert not self.df.empty
        self.df = self.df.drop('Line_Change', axis=1)

        if self.process:
            self.df = self.df.applymap(decode)

            # This produces a copy, won't modify self.df at all
            numer_df = self.df[ByteSpec().dict[self.type+'_numericals']]
            strings_df = self.df[ByteSpec().dict[self.type+'_strings']]

            # Add in argument errors='coerce' if ValueError comes up
            numer_df = numer_df.apply(pd.to_numeric)
            self.df[ByteSpec().dict[self.type+'_numericals']] = numer_df

            # Unwanted millisecond rounding (fixed?)
            self.df['Timestamp'] = self.base_time + self.df['Hour']*3600. +\
                                                    self.df['Minute']*60.+\
                                                    self.df['Second']*1.+\
                                                    self.df['Milliseconds']/1000.
            # Move Timestamp to first column
            cols = self.df.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            self.df = self.df[cols]

            # Bid/Ask price conversion
            self.df[['Bid_Price','Ask_Price']] = self.df[['Bid_Price','Ask_Price']]/10000.

            # Drop unneeded columnns
            self.df = self.df.drop(['Hour','Minute','Second','Milliseconds'], axis=1)

            # TODO: update self.dtype

        return self

    def column_load(self):
        '''
        Loads data into a column-oriented database.
        '''
        return self

    def query(self, stock, time, volume, price):
        if self.df.empty:
            raise ValueError('Must load in tick data into TaqDataFrame first')

        return None

    def aggregate(self):
        '''
        Returns a new DataFrame where equal symbols are grouped into one record,
        and mean/median/mode/total bid/ask price is provided as well as the same
        statistics for volume (bid/ask size). IQR is reported as a tuple as well.
        Min(high), max(low), max-min/min, open-close/close, total outstanding.
        Should this basically be a .featurize() for all stocks?
        '''
        assert self.process

        aggr_df = pd.DateFrame()

        self.stocks = self.df['Symbol_Root'].unique()
        for stock in self.stocks:
            # Do aggregation
            stock_rows = self.df.loc[self.df['Symbol_Root'] == stock]
            
            # continue

        # Some columns may be irrelevant depending on type of file used
        return None

    def featurize(self, stock):
        '''
        Extracts important features for a particular stock.
        Should probably use forward/backward stepwise selection for
        particular features.
        '''
        if self.df.empty:
            raise ValueError('Must load in tick data into TaqDataFrame first')
        assert self.process
        assert self.loaded

        # Make sure this makes a copy of the columns // is .copy() redundant?
        training = self.df[['Timestamp', 'Symbol_Root', 'Bid_Price',
                            'Bid_Size', 'Ask_Price', 'Ask_Size']]

        training = training.loc[training['Symbol_Root'] == stock]

        if training.empty:
            print ('No data for this stock, or stock is misspelled.')
            return

        # Select rows where Bid_Price>0 and Ask_Price>0 and Bid_Size>0 and Ask_Size>0
        training = training.ix[(training['Bid_Price']>0) | (training['Ask_Price']>0)]
        training = training.ix[(training['Bid_Size']>0) | (training['Ask_Size']>0)]

        # Sort records by timestamp
        training = training.sort_values('Timestamp', axis=0, ascending=True)

        # Add in columns: BA_Spread, BA_Mid, Total_A_Size, Total_B_Size
        training['Bid_Ask_Spread'] = training['Ask_Price'] - training['Bid_Price']
        training['Bid_Ask_Midpoint'] = (training['Ask_Price'] + training['Bid_Price'])/2.
        training['Bid_Ask_Size_Diff'] = training['Ask_Size'] - training['Bid_Size']


        # Perhaps percentage of total volume traded for that day?
        

        # Add in time-dependent features: d(AskPrice)/dt, d(BidPrice)/dt

        # Rescale row indices to [0, ...]
        print (training)

        training_dict = training.to_dict('records')
        vec = DictVectorizer(sparse=False)
        xtrain = vec.fit_transform(training_dict)
        ytrain = self.label_training(xtrain)
        return xtrain, ytrain

    def label_training(self, X):
        '''
        Should I be looking at price movement (direction) or fair value?
        '''
        return None

    def to_csv(self, fname):
        '''
        Writes current self.df to output csv.
        '''
        if fname[-4:] == '.csv':
            fname = fname[:len(fname)-4]

        return None

    def to_pickle(self, fname):
        self.df.to_pickle(fname)

    def read_pickle(self, fname):
        self.df = pd.read_pickle(fname)


class CRSPDataFrame:

    def __init__(self):
        self.load()

    def load(self):
        return None
