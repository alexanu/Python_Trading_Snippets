import os.path
import numpy as np
import pandas as pd
import eikon as ek
ek.set_app_key('48f17fdf21184b0ca9c4ea8913a840a92b338918')
from .future_root_factory import FutureRootFactory

from pandas import read_hdf
from pandas import HDFStore,DataFrame

import os
dirname = os.path.dirname(__file__)

#run through loop to get data for all, keeping in mind 5 year limit to history
def put_to_hdf(df):
    hdf = HDFStore(dirname + "\_InstrumentData.5")
    hdf.put('InstrumentData', df, format='table', data_columns=True)
    hdf.close() # closes the file

columns =[
        'exchange_symbol',
        'root_symbol',
        'instrument_name',
        'underlying_name',
        'underlying_asset_class_id',
        'settle_start',
        'settle_end',
        'settle_method',
        'settle_timezone',
        'final_settle_start',
        'final_settle_end',
        'final_settle_method',
        'final_settle_timezone',
        'last_trade_time',
        'quote_currency_id',
        'multiplier',
        'tick_size',
        'start_date',
        'end_date',
        'first_trade',
        'last_trade',
        'first_position',
        'last_position',
        'first_notice',
        'last_notice',
        'first_delivery',
        'last_delivery',
        'settlement_date',
        'volume_switch_date',
        'open_interest_switch_date',
        'auto_close_date',
        'exchange_id',
        'parent_calendar_id',
        'child_calendar_id',
        'average_pricing',
        'deliverable',
        'delivery_month',
        'delivery_year',
]

future_instrument_df = pd.DataFrame(columns = columns)

def get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date,end_date):
    """
    Fetch daily open, high, low close, open interest data for "platform_symbol".
    """
    assert type(start_date) is str, "start_date is not a string: %r" % start_date
    assert type(end_date) is str, "start_date is not a string: %r" % end_date

    try:
        tmp_ohlcv = ek.get_timeseries(eikon_symbol,["open","high","low","close","volume"],start_date=str(start_date), end_date=str(end_date))
    except ek.EikonError:
        return pd.DataFrame()
    tmp_ohlcv.insert(0,'exchange_symbol',exchange_symbol)
    e = ek.get_data(eikon_symbol, ['TR.OPENINTEREST.Date', 'TR.OPENINTEREST'], {'SDate':str(start_date),'EDate':str(end_date)})
    tmp_oi = pd.DataFrame({'open_interest': e[0]['Open Interest'].values}, index = pd.to_datetime(e[0]['Date'].values))
    tmp = pd.merge(tmp_ohlcv,tmp_oi,left_index=True,right_index=True)
    return tmp

def eikon_ohlcvoi_batch_retrieval(eikon_symbol,exchange_symbol,start_date,end_date):
    """
    Fetch daily data for "platform_symbol". Eikon API limits one-time retrievals,
    therefore we retrieve the data in 5 year batches.
    """
    assert type(start_date) is str, "start_date is not a string: %r" % start_date
    assert type(end_date) is str, "start_date is not a string: %r" % end_date

    data_df = pd.DataFrame()
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    counter = 0

    while int((end_date - start_date).days) > 1827:
        temp_end_date = start_date + pd.DateOffset(years=5)
        tmp = get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date.strftime("%Y-%m-%d"),temp_end_date.strftime("%Y-%m-%d"))
        data_df = data_df.append(tmp)
        counter += 1
        start_date = temp_end_date

    tmp = get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
    return data_df.append(tmp)


def write_future(factory,root_symbol):
        """
        write new future instruments to table.
        """
        # Construct futures instruments data
        root_chain_df = factory.make_root_chain(root_symbol)
        root_info_dict = factory.retrieve_root_info(root_symbol)

        # Convert pandas to dict for ease of extraction and indexing
        root_chain_dict = root_chain_df.set_index('platform_symbol').to_dict()
        platform_symbol_list = list(root_chain_dict['exchange_symbol'].keys())

        # Loop through symbols and save in data frame
        data_df = pd.DataFrame()
        for platform_symbol in platform_symbol_list:
            print(platform_symbol)
            exchange_symbol = root_chain_dict['exchange_symbol'][platform_symbol]
            start = root_chain_dict['first_trade'][platform_symbol].strftime("%Y-%m-%d")
            end = root_chain_dict['last_trade'][platform_symbol].strftime("%Y-%m-%d")
            tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
            data_df = data_df.append(tmp)

        # Change default column names to lower case
        data_df.columns = ['exchange_symbol','open','high','low','close','volume','open_interest']

        data_df.index.name = 'date'
        data_df.set_index(['exchange_symbol'], append=True, inplace=True)

        # Append data to hdf, remove duplicates, and write to both hdf and csv
            instrument_data_hdf = read_hdf(dirname +'\_InstrumentData.h5')
            instrument_data_hdf = instrument_data_hdf.append(data_df,sort=False).drop_duplicates()
            instrument_data_hdf.to_hdf(dirname +'\_InstrumentData.h5', 'InstrumentData', mode = 'w',
               format='table', data_columns=True)
            instrument_data_hdf.to_csv(dirname + "\_InstrumentData.csv")

        # Combine futures instrument information and calculated dates
        root_info_and_chain = pd.concat([pd.DataFrame.from_dict(root_info_dict),root_chain_df],axis=1).fillna(method='ffill')

        # Reset index to columns
        data_df.reset_index(level=[0,1], inplace=True)

        # Calculate start and end dates
        start_end_df = pd.DataFrame(
            {'start_date': data_df.groupby(['exchange_symbol']).first()['date'],
            'end_date': data_df.groupby(['exchange_symbol']).last()['date']
            })

        # inner join with future_instrument_df to enforce column structure, merge start and end
        metadata_df = pd.concat([future_instrument_df,root_info_and_chain], join = "inner")
        metadata_df = pd.merge(metadata_df,start_end_df, on='exchange_symbol')
        metadata_df = pd.concat([future_instrument_df,metadata_df], sort=False)
        metadata_df['delivery_month'] = metadata_df['delivery_month'].astype(str).astype(int)
        metadata_df['delivery_year'] = metadata_df['delivery_year'].astype(str).astype(int)

        metadata_df.set_index(['exchange_symbol'], append=True, inplace=True)

        # Assumes that hdf exists
        # Append data to hdf, remove duplicates, and write to both hdf and csv
        if os.path.isfile(dirname + "\_FutureInstrument.h5"):
            future_instrument_hdf = read_hdf(dirname +'\_FutureInstrument.h5')
            future_instrument_hdf = future_instrument_hdf.append(metadata_df,sort=False).drop_duplicates()
            future_instrument_hdf.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
               format='table', data_columns=True)
            future_instrument_hdf.to_csv(dirname + "\_FutureInstrument.csv")
        else:
            metadata_df.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
               format='table', data_columns=True)
            metadata_df.to_csv(dirname + "\_FutureInstrument.csv")


        # Assumes that hdf exists
        # Assign instrument routing information to table
        instrument_router_df = pd.DataFrame({'instrument_type': ['Future']}, index=metadata_df.index)

        if os.path.isfile(dirname + "\_InstrumentRouter.h5"):
            instrument_router_hdf = read_hdf(dirname +'\_InstrumentRouter.h5')
            instrument_router_hdf = instrument_router_hdf.append(instrument_router_df,sort=False).drop_duplicates()
            instrument_router_hdf.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
               format='table', data_columns=True)
            instrument_router_hdf.to_csv(dirname + "\_InstrumentRouter.csv")
        else:
            instrument_router_df.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
               format='table', data_columns=True)
            instrument_router_df.to_csv(dirname + "\_InstrumentRouter.csv")

        return data_df

#        if os.path.isfile(dirname + "\_FutureInstrument.csv"):
#            with open(dirname + "\_FutureInstrument.csv", 'a') as f:
#                     metadata_df.to_csv(f, index = False, header=False)
#        else:
#            metadata_df.to_csv(dirname + "\_FutureInstrument.csv", index = False)


        #update function
        #one for us, one for eu, one for asia, or separated by exchange close times
        #generate currently listed contracts
        #check against thomson
        #dump into today update list
        #run through list download and append latest data to df
        #check to see that we are getting data that we expect to get

        #read first and last date for each symbol in isntrument table
        #insert those dates into instrument table
        #write missing to dashboard or log or email for daily check

        #create continuous futures from the data daily
        #dump this into a csv file, either locally or ftp to blackbox
        #read the data in R and save to RData file in compatible format

        #setup account for each client
        #have algorithm run incrementally, day by day
        #have algorithm spit out orders with equity and vol info to file or shiny

        #have R trade on real prices rather than continuous prices
        #track roll dates

        #migrate all R code into python and integrate

        #track pnl and have program set up to take actual fills against benchmark

        #clean up code and start testing new strategies
