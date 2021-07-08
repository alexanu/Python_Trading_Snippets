import os, glob
import pandas as pd

Zorro_directory = 'C:\\Users\\oanuf\\Data\\Zorro_data\\'
Tickers_minute=[]
Tickers_minute.append([x.split('.')[0] for x in os.listdir(Zorro_directory) if x.endswith(".t6")])
len(Tickers_minute[0])
Tickers_tick=[]
Tickers_tick.append([x.split('.')[0] for x in os.listdir(Zorro_directory) if x.endswith(".t1")])
len(Tickers_tick[0])
hist_data = pd.DataFrame(Tickers_minute[0],columns=['File_Name'])
hist_data[['Asset','Year']] = hist_data['File_Name'].str.split('_',expand=True)

# hist_data.groupby('Asset').apply(lambda x: x['Year'].unique()).to_csv("Zo_data.csv")

import numpy as np
hist_data.pivot_table(index=['Asset'],columns='Year',aggfunc=np.count_nonzero).to_csv("Zo_data2.csv")