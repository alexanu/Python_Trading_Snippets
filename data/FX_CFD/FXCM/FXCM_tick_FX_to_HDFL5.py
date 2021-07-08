
import datetime as dt
import numpy as np
import pandas as pd
import tables as tb
import requests
import fxcmpy
from fxcmpy import fxcmpy_tick_data_reader as tdr
import os
import gzip
from io import StringIO


# Scrapes FX data from FXCM, then checks all the files downloaded

# Construct the years, weeks and symbol lists required for the scraper.
years = [2017, 2018]
weeks = list(range(53))
symbols = []
for pair in tdr.get_available_symbols():
    if pair not in symbols:
        symbols.append(pair)

# Scrape time
directory = "/location_of/scraped/files/"

for symbol in symbols:    
    for year in years:
        for week in weeks: # 2000 files 5-10Mb each
            url = f"https://tickdata.fxcorporate.com/{symbol}/{year}/{week}.csv.gz"
            r = requests.get(url, stream=True)
            with open(f"{directory}{symbol}_{year}_w{week}.csv.gz", 'wb') as file:
                for chunk in r.iter_content(chunk_size=1024):
                    file.write(chunk)

# Check all the files for each currency pair was downloaded (should be 104 for each)
total = 0
for symbol in symbols:
    count = 0
    for file in os.listdir(directory):
        if file[:6] == symbol:
            count+=1
    total += count
    print(f"{symbol} files downloaded = {count} ")
print(f"\nTotal files downloaded = {total}")


# Extract, Transform and Load FXCM gzip files into a HDF5 data store
# ETL script

directory = "/location_of/scraped/files/"
hdf5_file = '/Volumes/external_hd/FxTickData.h5'

for file in os.listdir(directory):
    if file.endswith('.gz'):
        print(f"\nExtracting: {file}")
        
        # extract gzip file and assign to Dataframe
        codec = 'utf-16'
        f = gzip.GzipFile(f'{directory}{file}')
        data = f.read()
        data_str = data.decode(codec)
        data_pd = pd.read_csv(StringIO(data_str))
        
        # pad missing zeros in microsecond field
        data_pd['DateTime'] = data_pd.DateTime.str.pad(26, side='right', fillchar='0')
        
        # assign Datetime column as index
        data_pd.set_index('DateTime', inplace=True)
        
        # sample start and end to determine date format
        sample1 = data_pd.index[1]
        sample2 = data_pd.index[-1]
        
        # determine datetime format and supply srftime directive
        for row in data_pd:
            if data_pd.index[3] == '/':
                if sample1[0:2] == sample2[0:2]:
                    data_pd.index = pd.to_datetime(data_pd.index, format="%m/%d/%Y %H:%M:%S.%f")
                elif sample1[3:5] == sample2[3:4]:
                    data_pd.index = pd.to_datetime(data_pd.index, format="%d/%m/%Y %H:%M:%S.%f")
            elif data_pd.index[5] == '/':
                if sample1[9:11] == sample2[9:11]:
                    data_pd.index = pd.to_datetime(data_pd.index, format="%Y/%d/%m %H:%M:%S.%f")
                elif sample[6:8] == sample2[6:8]:
                    data_pd.index = pd.to_datetime(data_pd.index, format="%Y/%m/%d %H:%M:%S.%f")
        
        print("\nDATA SUMMARY:")
        print(data_pd.info())
        
        # Load data into database
        store = pd.HDFStore(hdf5_file) # open the hdf5 file
        symbol = file[:6]
        store.append(symbol, data_pd, format='t') 
        store.flush() # write it to disk
        print("\nH5 DATASTORE SUMMARY:")
        print(store.info()+"\n"+"-"*75)
        store.close()

