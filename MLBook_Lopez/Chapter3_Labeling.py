__author__ = 'cgomezfandino@gmail.com'

import pandas as pd
import numpy as np
import multiprocessing as mp
import matplotlib.pyplot as plt
from MultiprocessingObject.mpEngine import mpPandasObj
from API_connections.HistoricalData import *
from multiprocessing import cpu_count



#Page 43
# Daily Volatility Estimator [3.1]

def getDailyVol(close,span0=100):
    # daily vol, reindexed to close
    days = 1
    df0 = close.index.searchsorted(close.index-pd.Timedelta(days=days))
    df0 = np.array(df0[df0>0])
    df0 = pd.Series(close.index[df0 - days], index = close.index[close.shape[0]- df0.shape[0]:])
    df0 = close.loc[df0.index]/close.loc[df0.values].values-1 # daily returns
    df0 = df0.ewm(span=span0).std()
    return df0

# TRIPLE-BARRIER LABELING METHOD
# Triple-Barrier Labeling Method [3.2]

def applyPtSlOnT1(close,events,ptSl,molecule):

    '''

    :param close: A pandas series of prices.
    :param events: A pandas dataframe, with columns,
                    ◦ t1: The timestamp of vertical barrier. When the value is np.nan, there will
                            not be a vertical barrier.
                    ◦ trgt: The unit width of the horizontal barriers.
    :param ptSl: A list of two non-negative float values:
                    ◦ ptSl[0]: The factor that multiplies trgt to set the width of the upper barrier.
                        If 0, there will not be an upper barrier.
                    ◦ ptSl[1]: The factor that multiplies trgt to set the width of the lower barrier.
                        If 0, there will not be a lower barrier.
    :param molecule: A list with the subset of event indices that will be processed by a
                        single thread. Its use will become clear later on in the chapter.
    :return:
    '''
    # apply stop loss/profit taking, if it takes place before t1 (end of event)
    events_=events.loc[molecule]
    out=events_[['t1']].copy(deep=True)
    if ptSl[0]>0:pt=ptSl[0]*events_['trgt']
    else:pt=pd.Series(index=events.index) # NaNs
    if ptSl[1]>0:sl=-ptSl[1]*events_['trgt']
    else:sl=pd.Series(index=events.index) # NaNs
    for loc,t1 in events_['t1'].fillna(close.index[-1]).iteritems():df0=close[loc:t1] # path prices
    df0=(df0/close[loc]-1)*events_.at[loc,'side'] # path returns
    out.loc[loc,'sl']=df0[df0<sl[loc]].index.min() # earliest stop loss.
    out.loc[loc,'pt']=df0[df0>pt[loc]].index.min() # earliest profit taking.
    return out

# GETTING THE TIME OF FIRST TOUCH
# Gettting Time of First Touch (getEvents) [3.3], [3.6]
def getEvents(close,tEvents,ptSl,trgt,minRet,numThreads,t1=False):
    '''

    :param close: A pandas series of prices.
    :param tEvents: The pandas timeindex containing the timestamps that will seed every
                    triple barrier. These are the timestamps selected by the sampling procedures
                    discussed in Chapter 2, Section 2.5.
    :param ptSl: A non-negative float that sets the width of the two barriers. A 0 value
                    means that the respective horizontal barrier (profit taking and/or stop loss) will
                    be disabled.
    :param trgt: A pandas series of targets, expressed in terms of absolute returns.
    :param minRet: The minimum target return required for running a triple barrier search.
    :param numThreads: The number of threads concurrently used by the function.
    :param t1: A pandas series with the timestamps of the vertical barriers. We pass a
                    False when we want to disable vertical barriers.
    :return:
    '''
    #1) get target
    trgt=trgt.loc[tEvents]
    trgt=trgt[trgt>minRet] # minRet
    #2) get t1 (max holding period)
    if t1 is False:t1=pd.Series(pd.NaT,index=tEvents)
    #3) form events object, apply stop loss on t1
    side_=pd.Series(1.,index=trgt.index)
    events=pd.concat({'t1':t1,'trgt':trgt,'side':side_}, axis=1).dropna(subset=['trgt'])
    df0= mpPandasObj(func=applyPtSlOnT1,pdObj=('molecule',events.index), numThreads=numThreads,close=close,events=events,ptSl=[ptSl,ptSl])
    events['t1']=df0.dropna(how='all').min(axis=1) # pd.min ignores nan
    events=events.drop('side',axis=1)
    return events


# ADDING A VERTICAL BARRIER
# def getT1(close,tEvents,numDays):
#     t1=close.index.searchsorted(tEvents+pd.Timedelta(days=numDays))
#     t1=t1[t1<close.shape[0]]
#     t1=pd.Series(close.index[t1],index=tEvents[:t1.shape[0]]) # NaNs at end
#     return t1

# LABELING FOR SIDE AND SIZE
# Labeling for side and size [3.5]
def getBinsOld(events,close):
    #1) prices aligned with events
    events_=events.dropna(subset=['t1'])
    px=events_.index.union(events_['t1'].values).drop_duplicates()
    px=close.reindex(px,method='bfill')
    #2) create out object
    out=pd.DataFrame(index=events_.index)
    out['ret']=px.loc[events_['t1'].values].values/px.loc[events_.index]-1
    out['bin']=np.sign(out['ret'])
    return out

# Expanding getBins to Incorporate Meta-Labeling [3.7]
def getBins(events, close):
    '''
    Compute event's outcome (including side information, if provided).
    events is a DataFrame where:
    -events.index is event's starttime
    -events['t1'] is event's endtime
    -events['trgt'] is event's target
    -events['side'] (optional) implies the algo's position side
    Case 1: ('side' not in events): bin in (-1,1) <-label by price action
    Case 2: ('side' in events): bin in (0,1) <-label by pnl (meta-labeling)
    '''
    #1) prices aligned with events
    events_=events.dropna(subset=['t1'])
    px=events_.index.union(events_['t1'].values).drop_duplicates()
    px=close.reindex(px,method='bfill')
    #2) create out object
    out=pd.DataFrame(index=events_.index)
    out['ret']=px.loc[events_['t1'].values].values/px.loc[events_.index]-1
    if 'side' in events_:out['ret']*=events_['side'] # meta-labeling
    out['bin']=np.sign(out['ret'])
    if 'side' in events_:out.loc[out['ret']<=0,'bin']=0 # meta-labeling
    return out

# Symmetric CUSUM Filter [2.5.2.1] - (snippet 2.4)
def getTEvents(gRaw,h):
    tEvents,sPos,sNeg=[],0,0
    diff=gRaw.diff()
    for i in diff.index[1:]:
        sPos,sNeg=max(0,sPos+diff.loc[i]),min(0,sNeg+diff.loc[i])
        if sNeg<-h:
            sNeg=0;tEvents.append(i)
        elif sPos>h:
            sPos=0;tEvents.append(i)
    return pd.DatetimeIndex(tEvents)

# Adding Vertical Barrier [3.4]
def addVerticalBarrier(tEvents, close, numDays=1):
    t1=close.index.searchsorted(tEvents+pd.Timedelta(days=numDays))
    t1=t1[t1<close.shape[0]]
    t1=(pd.Series(close.index[t1],index=tEvents[:t1.shape[0]]))
    return t1

def dropLabels(events, minPct=.05):
    # apply weights, drop labels with insufficient examples
    while True:
        df0=events['bin'].value_counts(normalize=True)
        if df0.min()>minPct or df0.shape[0]<3:break
        print('dropped label: ', df0.argmin(),df0.min())
        events=events[events['bin']!=df0.argmin()]
    return events

if __name__ == '__main__':


    symbol = 'EUR_USD'
    start = '2015-01-01'
    end = '2017-01-01'
    timeFrame = 'H4'
    price = 'B'

    rd = retrieveHistricalData()
    data = rd.rtv_candle_data_oanda(symbol, start, end, timeFrame, price)
    # data['minRet'] = np.log(data.close / data.close.shift())

    close = data.close.copy()
    # close = pd.Series(data.close, index = data.index)
    dailyVol = getDailyVol(data.close) #set default profit takin and stop-loss limit. Estimated Volatility.

    # Get tEvents
    h = dailyVol.mean()
    #CUMSUM FILTER
    tEvents = getTEvents(data.close, h)

    # Run in single-threaded mode on Windows
    import platform

    if platform.system() == "Windows":
        cpus = 1
    else:
        cpus = cpu_count() - 1

    #  Add vertical barrier
    t1 = addVerticalBarrier(tEvents, close, numDays=1)
    ptSl = [1, 2]
    target = dailyVol
    minRet = 0.01

    events = getEvents(close, tEvents, ptSl = ptSl, trgt = target, minRet = minRet, numThreads=cpus, t1=t1 )

    # plt.plot(close, 'b-')
    # plt.plot(close[tEvents], 'r.', )
    # plt.show()


    data

