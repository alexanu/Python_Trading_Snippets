
import pandas as pd
import numpy as np

import datetime as dt


result=pd.read_csv("C:\\Users\\oanuf\\Zorro\\Log\\gapsize.csv",skipinitialspace=True)
result['TradeReturn'] = result['TradeReturn'].round(4)*100
result['EntryTime'] = pd.to_datetime(result['EntryTime'], format= '%Y/%m/%d %H:%M')
result['ExitTime'] = pd.to_datetime(result['ExitTime'], format= '%Y/%m/%d %H:%M')
result['Strategy'] = np.where(result['EntryTime'].dt.dayofweek == 6, 'Mom', 'Revers')
result['End_Month'] = result.apply(lambda row : sum([item.is_month_end for item in pd.date_range(row['EntryTime'],row['ExitTime'])]), axis = 1) 
result['GapSize_Rank']=pd.qcut(abs(result["GapSize"]), q = 3, labels = ["small", "avg", "big"])



################################################################################################

result.dtypes
result.columns
result["Asset"].nunique() # number of assets
result["Asset"].unique().tolist()
result["Asset"].value_counts(normalize = True)

result.loc[result['Asset'].isin(['AUD/USD','USD/JPY'])]
result[result["Asset"] == "USD/JPY"].groupby("Strategy").mean()



result[result["GapSize"] > 0.01].pivot_table(index="Asset", 
											columns="Strategy", 
											values="TradeReturn", 
											aggfunc=np.mean).fillna(0)
                                    
result.groupby(["GapSize_Rank","End_Month","Strategy"]).agg({'TradeReturn': 'mean'})
result.groupby(["GapSize_Rank","End_Month","Strategy"]).agg({'TradeReturn': 'mean'})




pd.cut(result["GapSize"], bins = [0, 25, 50, 75, 99]).head() # Using cut you can specify the bin edges
pd.qcut(result["GapSize"], q = 6) # specify the number of bins
pd.qcut(result["GapSize"], q = 4, labels = ["awful", "bad", "average", "good"])


training = training.ix[(training['Bid_Price']>0) | (training['Ask_Price']>0)]
mask = (stock_data['Date'] > start_date) & (stock_data['Date'] <= end_date) # filter our column based on a date range   
stock_data = stock_data.loc[mask] # rebuild our dataframe


