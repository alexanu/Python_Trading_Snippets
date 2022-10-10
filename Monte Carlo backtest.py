# https://datascienceplus.com/how-to-apply-monte-carlo-simulation-to-forecast-stock-prices-using-python/

# the weakness of monte carlo, perhaps in every forecast methodology
# is that our pseudo random number is generated via empirical distribution
# in another word, we use the past to predict the future

#the idea presented here is very straight forward
#we construct a model to get mean and variance of its residual (return)
#we generate the next possible price by geometric brownian motion
#we run this simulations as many times as possible
#naturally we should acquire a large amount of data in the end
#we pick the forecast that has the least std against the original data series
#we would check if the best forecast can predict the future direction (instead of actual price)
#and how well monte carlo catches black swans
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import fix_yahoo_finance as yf
import random as rd
from sklearn.model_selection import train_test_split


# testsize denotes how much percentage of dataset would be used for testing
# simulation denotes the number of simulations
def monte_carlo(data,testsize=0.5,simulation=100,**kwargs):    
    
    #train test split as usual
    df,test=train_test_split(data,test_size=testsize,shuffle=False,**kwargs)
    forecast_horizon=len(test)
    
    #we only care about close price
    #if there has been dividend issued
    #we use adjusted close price instead
    df=df.loc[:,['Close']]
        
    #here we use log return
    returnn=np.log(df['Close'].iloc[1:]/df['Close'].shift(1).iloc[1:])
    drift=returnn.mean()-returnn.var()/2
    
    #we use dictionary to store predicted time series
    d={}
    
    #we use geometric brownian motion to compute the next price
    # https://en.wikipedia.org/wiki/Geometric_Brownian_motion
    for counter in range(simulation):
        d[counter]=[df['Close'].iloc[0]]
      
        #we dont just forecast the future
        #we need to compare the forecast with the historical data as well
        #thats why the data range is training horizon plus testing horizon
        for i in range(len(df)+forecast_horizon-1):
         
            #we use standard normal distribution to generate pseudo random number
            #which is sufficient for our monte carlo simulation
            sde=drift+returnn.std()*rd.gauss(0,1)
            temp=d[counter][-1]*np.exp(sde)
        
            d[counter].append(temp.item())
    
    #to determine which simulation is the best fit
    #we use simple criterias, the smallest standard deviation
    #we iterate through every simulation and compare it with actual data
    #the one with the least standard deviation wins
    std=float('inf')
    pick=0
    for counter in range(simulation):
    
        temp=np.std(np.subtract(
                    d[counter][:len(df)],df['Close']))
        if temp<std:
            std=temp
            pick=counter
    
    return forecast_horizon,d,pick


#we also gotta test if the surge in simulations increases the prediction accuracy
#simu_start denotes the minimum simulation number
#simu_end denotes the maximum simulation number
#sim_delta denotes how many steps it takes to reach the max from the min
#its kinda like range(simu_start,simu_end,simu_delta)
def test(df,ticker,simu_start=100,simu_end=1000,simu_delta=100,**kwargs):
    
    table=pd.DataFrame()
    table['Simulations']=np.arange(simu_start,simu_end+simu_delta,simu_delta)
    table.set_index('Simulations',inplace=True)
    table['Prediction']=0

    #for each simulation
    #we test if the prediction is accurate
    #for instance
    #if the end of testing horizon is larger than the end of training horizon
    #we denote the return direction as +1
    #if both actual and predicted return direction align
    #we conclude the prediction is accurate
    #vice versa
    for i in np.arange(simu_start,simu_end+1,simu_delta):
        print(i)
        
        forecast_horizon,d,pick=monte_carlo(df,simulation=i,**kwargs)
        
        actual_return=np.sign( \
                              df['Close'].iloc[len(df)-forecast_horizon]-df['Close'].iloc[-1])
        
        best_fitted_return=np.sign(d[pick][len(df)-forecast_horizon]-d[pick][-1])
        table.at[i,'Prediction']=np.where(actual_return==best_fitted_return,1,-1)
        
    #we plot the horizontal bar chart 
    #to show the accuracy does not increase over the number of simulations
    ax=plt.figure(figsize=(10,5)).add_subplot(111)
    ax.spines['right'].set_position('center')
    ax.spines['top'].set_visible(False)

    plt.barh(np.arange(1,len(table)*2+1,2),table['Prediction'], \
             color=colorlist[0::int(len(colorlist)/len(table))])

    plt.xticks([-1,1],['Failure','Success'])
    plt.yticks(np.arange(1,len(table)*2+1,2),table.index)
    plt.xlabel('Prediction Accuracy')
    plt.ylabel('Times of Simulation')
    plt.title(f"Prediction accuracy doesn't depend on the numbers of simulation.\nTicker: {ticker}\n")
    plt.show()

#lets try something extreme, pick ge, the worst performing stock in 2018
#see how monte carlo works for both direction prediction and fat tail simulation
#why the extreme? well if we are risk quants, we care about value at risk, dont we
#if quants only look at one sigma event, the portfolio performance would be devastating
def main():
    
    stdate='2016-01-15'
    eddate='2019-01-15'
    ticker='GE'

    df=yf.download(ticker,start=stdate,end=eddate)
    df.index=pd.to_datetime(df.index)
    
    forecast_horizon,d,pick=monte_carlo(df)
    plot(df,forecast_horizon,d,pick,ticker)
    test(df,ticker)


if __name__ == '__main__':
    main()
