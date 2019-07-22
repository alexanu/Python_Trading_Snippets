import quandl
import pandas as pd
import datetime
import zipfile
import glob
import os

quandl.ApiConfig.api_key="ZKWjVGUk-dp4sWoz5buP"

class StockPrice:
    """
    class for preprocessing data
    """
    def __init__(self, opt):
        #original csv
        self.stock_list = []

        #current date for unzip the file and read csv
        self.csv_filename = 'EOD_test2.csv'
        self.stock_data_folder = 'stock_data'

        self.stock_mat = None    

        self.is_liquid = opt.is_liquid
        self.is_daily = opt.is_daily
        
    def get_stock_prices(self):
        self.stock_list = pd.read_csv(self.csv_filename)
        self.stock_list.columns = [u'Name', u'Date', u'Open', u'High', u'Low', u'Close',u'Volume',u'Dividend',u'Split',u'Adj_Open',u'Adj_High',u'Adj_Low',u'Adj_Close',u'Adj_Volume']

        #align trading date, close price only
        self.stock_mat = self.stock_list.pivot(index=u'Date',columns=u'Name',values=u'Adj_Close')
        
        if self.is_liquid:
            liquid_list = pd.read_csv('liquidtickers.csv').values.reshape([-1]).tolist()
            #TODO
            liquid_list.remove('ECH')
            self.stock_mat = self.stock_mat[liquid_list]
        
        # pick weekly data based on the last trading day of the week
        if not self.is_daily:
            max_wd = 0
            max_wd_list = []
            for i in range(len(self.stock_mat)):
                # get date
                dt = self.stock_mat.iloc[i,0:1].name
                year, month, day = (int(x) for x in dt.split('-'))    
                # weekday: Monday = 0, Sunday = 6
                wd = datetime.date(year, month, day).weekday()
                if wd >= max_wd:
                    max_wd_date = dt
                else:
                    max_wd_list.append(i)
                max_wd = wd
            
            # append the last date if not already included
            if max_wd_date == dt:
                max_wd_list.append(i)
                
            self.stock_mat = self.stock_mat.iloc[max_wd_list]
            
    def update_data_csv(self):
        # clean previous files
        file_list = glob.glob('stock_data/*.csv')
        for filename in file_list:
            os.remove(filename)

        quandl.bulkdownload("EOD")
        print("Download succeeded")        

        zip_ref = zipfile.ZipFile('EOD.zip', 'r')
        zip_ref.extractall(self.stock_data_folder)
        zip_ref.close()
        print("Extract to", self.stock_data_folder)
        
        # get filename
        self.csv_filename = glob.glob('stock_data/*.csv')[0]
