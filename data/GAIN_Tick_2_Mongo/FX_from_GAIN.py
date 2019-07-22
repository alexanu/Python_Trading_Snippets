import urllib.request
import csv
import os
import zipfile
import shutil
import pandas as pd

FOREX_DATA_PATH = "C:\\...\\ForexData"
OUTFILE = "download.txt"


def get_current_month_num(current_month):
    if current_month < 10:
        return "0" + str(current_month)
    else:
        return str(current_month)

def get_current_month_name(current_month):
    if current_month is 1:
        return "January"
    elif current_month is 2:
        return "February"
    elif current_month is 3:
        return "March"
    elif current_month is 4:
        return "April"
    elif current_month is 5:
        return "May"
    elif current_month is 6:
        return "June"
    elif current_month is 7:
        return "July"
    elif current_month is 8:
        return "August"
    elif current_month is 9:
        return "September"
    elif current_month is 10:
        return "October"
    elif current_month is 11:
        return "November"
    elif current_month is 12:
        return "December"


outfile = open(OUTFILE, 'w')

FX_pairs = os.listdir(FOREX_DATA_PATH)

start_month = int(input("Enter Start month (as a number 1-12): "))
start_week = int(input("Enter Start Week (as a number 1-5): "))
start_year = int(input("Enter Start year (as a number 10-18): "))
end_month = int(input("Enter End Month (as a number 1-12): "))
end_week = int(input("Enter End Week (as a number 1-5): "))
end_year = int(input("Enter End Year (as a number 10-18: )"))

for pair in FX_pairs:

    current_month = start_month
    current_year = start_year
    current_week = start_week
    downloading = True

    while downloading:
        lineout = "http://ratedata.gaincapital.com/" + "20" + str(current_year) + "/" 
                    + get_current_month_num(current_month) + "%20" 
                    + get_current_month_name(current_month) + "/" 
                    + pair[0:3] + "_" + pair[3:6] + "_Week" + str(current_week) + ".zip"
        outfile.write(lineout + "\n")

        if current_year is end_year and current_month is end_month and current_week is end_week:
            downloading = False

        if current_month is 12 and current_week is 5:
            current_year = current_year + 1
        if current_week is 5:
            current_month = (current_month % 12) + 1
        current_week = (current_week % 5) + 1

outfile.close()
''' File download.txt contains list of links like this:
http://ratedata.gaincapital.com/2018/09%20September/AUD_JPY_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/AUD_NZD_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/AUD_USD_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/CAD_CHF_Week3.zip
http://ratedata.gaincapital.com/2018/09%20September/CAD_JPY_Week3.zip

'''

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
    destfolder = "C:\\....\ForexData\\" + fxname

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
