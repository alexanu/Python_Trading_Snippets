
''' File download.txt contains list of links like this:
http://ratedata.gaincapital.com/2018/09%20September/AUD_JPY_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/AUD_NZD_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/AUD_USD_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/CAD_CHF_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/CAD_JPY_Week3.zip

'''

import urllib.request
import csv
import os
import zipfile
import shutil
import pandas as pd

downloadfile = open("download.txt", 'r', newline='')
dlreader = csv.reader(downloadfile, delimiter=',')

for row in dlreader:
    #download and extract the 1 week data
    urlparts = row[0].split("/")

    try:
        urllib.request.urlretrieve(row[0], urlparts[-1])
    except:
        print("Inalid URL", row[0])
        continue

    fxnames = urlparts[-1].split("_")
    fxname = fxnames[0] + fxnames[1]
    destfolder = "C:\\Users\wyatt\Documents\ForexData\\" + fxname

    with zipfile.ZipFile(urlparts[-1],"r") as zip_ref:
            zip_ref.extractall(destfolder)

    tick_data_file = destfolder + "\\" + urlparts[-1][:-3]  + "csv"

    #convert tick data to 15 minute data
    data_frame = pd.read_csv(tick_data_file, names=['id', 'deal', 'Symbol', 'Date_Time', 'Bid', 'Ask'], index_col=3, parse_dates=True, skiprows= 1)
    ohlc_M15 =  data_frame['Bid'].resample('15Min').ohlc()
    ohlc_H1 = data_frame['Bid'].resample('1H').ohlc()
    ohlc_H4 = data_frame['Bid'].resample('4H').ohlc()
    ohlc_D = data_frame['Bid'].resample('1D').ohlc()

    #append 15 minute data to big file
    with open(destfolder + "\\" + fxname + "_M15.csv", 'a') as outfile:
        ohlc_M15.to_csv(outfile, header=False)

    #recalculate M15 data as H1, H4, and D
    with open(destfolder + "\\" + fxname + "_H1.csv", 'a') as outfile:
        ohlc_H1.to_csv(outfile, header=False)

    with open(destfolder + "\\" + fxname + "_H4.csv", 'a') as outfile:
        ohlc_H4.to_csv(outfile, header=False)

    with open(destfolder + "\\" + fxname + "_D.csv", 'a') as outfile:
        ohlc_D.to_csv(outfile, header=False)

    os.remove(tick_data_file)
    os.remove(urlparts[-1])
    print('.', end='', flush=True)
