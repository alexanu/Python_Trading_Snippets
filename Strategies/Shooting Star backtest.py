# Source: https://github.com/je-suis-tm/quant-trading/blob/master/Shooting%20Star%20backtest.py
# details of shooting star: https://www.investopedia.com/terms/s/shootingstar.asp
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import yfinance


#criteria of shooting star
def shooting_star(data,lower_bound,body_size):

    df=data.copy()
    df['condition1']=np.where(df['Open']>=df['Close'],1,0)
    df['condition2']=np.where((df['Close']-df['Low']) < lower_bound*abs(df['Close']-df['Open']),1,0)
    df['condition3']=np.where(abs(df['Open']-df['Close'])<abs(np.mean(df['Open']-df['Close']))*body_size,1,0)
    df['condition4']=np.where((df['High']-df['Open'])>=2*(df['Open']-df['Close']),1,0)
    df['condition5']=np.where(df['Close']>=df['Close'].shift(1),1,0)
    df['condition6']=np.where(df['Close'].shift(1)>=df['Close'].shift(2),1,0)
    df['condition7']=np.where(df['High'].shift(-1)<=df['High'],1,0) # next candle's high must stay below the high of the shooting star 
    df['condition8']=np.where(df['Close'].shift(-1)<=df['Close'],1,0) # next candle's close below the close of the shooting star
    
    return df


def signal_generation(df,method,
                      lower_bound=0.2,body_size=0.5,
                      stop_threshold=0.05, # set stop loss/profit at +-5%
                      holding_period=7): # maximum holding period at 7 workdays

    data=method(df,lower_bound,body_size)
    data['signals']=data['condition1']*data['condition2']*data['condition3']*data['condition4']*data['condition5']*data['condition6']*data['condition7']*data['condition8']
    data['signals']=-data['signals'] # shooting star is a short signal
    
    #find exit position
    idxlist=data[data['signals']==-1].index
    for ind in idxlist:
        entry_pos=data['Close'].loc[ind] #entry point
        stop=False
        counter=0
        while not stop:
            ind+=1
            counter+=1
            if abs(data['Close'].loc[ind]/entry_pos-1)>stop_threshold:
                stop=True
                data['signals'].loc[ind]=1
            if counter>=holding_period:
                stop=True
                data['signals'].loc[ind]=1

    data['positions']=data['signals'].cumsum()     #create positions    
    return data

def main():
    
    stdate='2000-01-01'
    eddate='2021-11-04'
    name='Vodafone'
    ticker='VOD.L'

    df=yfinance.download(ticker,start=stdate,end=eddate)
    df.reset_index(inplace=True)
    df['Date']=pd.to_datetime(df['Date'])

    new=signal_generation(df,shooting_star)


if __name__ == '__main__':
    main()

