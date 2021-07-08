# Source: https://github.com/TheRockXu/aifin

"""
Major Functions from Section 1
"""

import numpy as np
import pandas as pd
from scipy.stats import *




def get_cumsum_events(g_raw, h):
    """
    The CUSUM filter is quality control method, designed to detect a shift in the mean value of a measured quantity away from a target value.
    :param g_raw: raw dataframe indexed with dates
    :param h: the target threhold
    :return: DatetimeIndex when threshold crossed
    """

    t_events = [] # when threshold h is crossed
    s_pos = 0 # cumulative positive
    s_neg = 0 # cumulative negative

    diff = g_raw.diff().dropna() # daily price change
    for i in diff.index:
        # if positive cumulative sum goes below zero,
        # set it to 0. Otherwise, add  price change to it.
        s_pos = max(0, s_pos + diff.loc[i])
        # if negative cumulative sum goes above zero, set it to 0
        s_neg  = min(0, s_neg + diff.loc[i])
        # if the cumumlative sum crosses the threhold, record the event.
        if s_neg < -h:
            s_neg=0
            t_events.append(i)
        if s_pos > h:
            s_pos = 0
            t_events.append(i)
    return pd.DatetimeIndex(t_events)



def get_daily_vol(close, span0=100):
    """
    Compute daily volatility at intraday estimation points.
    :param close: dataframe
    :param span0: the span of days
    :return: pandas Series
    """
    df0 = close.index.searchsorted(close.index - pd.Timedelta(days=1))  # a list of index
    df0 = df0[df0 > 0]
    df0 = pd.Series(close.index[df0 - 1], index=close.index[close.shape[0] - df0.shape[0]:])
    df0 = close.loc[df0.index] / close.loc[df0.values].values - 1  # daily returns
    df0 = df0.ewm(span=span0).std()
    return df0




def get_t1(close, t_events, num_days):
    """
    For each index in t_events, find the timestamp of the next price bar or immediately after a number of days.
    :param close: raw price dataframe
    :param t_events:  pd.Datatime Index
    :param num_days: number of days to for t1
    :return: a pd.Series of dates
    """
    # find dates within the close dataframe
    t1 = close.index.searchsorted(t_events + pd.Timedelta(days=num_days))
    t1 = t1[t1 < close.shape[0]]
    t1 = pd.Series(close.index[t1], index=t_events[:t1.shape[0]])

    return t1


def apply_ptsl_on_t1(close,  events, pt=1, sl=1):
    """
    Apply profit taking and stop loss on events dataframe
    :param close: raw price dataframe
    :param events: events that include t1, and trgt for profit taking and stop loss threhold
    :return:
    """
    out = events[['t1']].copy(deep=True)
    pt = pt*events['trgt']
    sl = - sl*events['trgt']
    for loc, t1 in events['t1'].iteritems():
        df0 = close[loc:t1]  # path prices
        df0 = (df0 / close[loc] - 1) * events.at[loc, 'side']
        out.loc[loc, 'sl'] = df0[df0 < sl[loc]].index.min()
        out.loc[loc, 'pt'] = df0[df0 > pt[loc]].index.min()
    return out


def get_events(close, t_events, trgt, min_ret, t1, side=None, pt_sl=(1,1)):
    """
    Find the first barrier touch with triple barrier method
    :param close: raw price
    :param t_events: event dataindex
    :param trgt: volatility threhold for profit taking and stop loss
    :param min_ret: minimum return to label
    :param t1: t1, vertical barrier
    :return: a event dataframe
    """
# Find the first barrier touch
    trgt = trgt.loc[t_events]
    trgt = trgt[trgt > min_ret]

    side_ = pd.Series(1., index=trgt.index)
    
    # form events object, apply stop loss on t1
    
    events = pd.concat({'t1':t1, 'trgt':trgt, 'side':side_}, axis=1).dropna(subset=['trgt'])
    
    df0 = apply_ptsl_on_t1(close, events, pt=pt_sl[0], sl=pt_sl[1])

    events['t1'] = df0.dropna(how='all').min(axis=1)
    events = events.drop('side', axis=1)
    
    return events



# Finally we can label the observation
def get_bins(events, close):

    """
    Use the event dataframe produced by get_events to label data

    :param events: events dataframe that include t1, trgt
    :param close: raw data
    :return: a labeled data
    """
    # 1) prices aligned with events
    
    events_ = events.dropna(subset=['t1'])
    
    px = events_.index.union(events_['t1'].values).drop_duplicates() # get all the dates from both event starts and ends. 
    px = close.reindex(px, method='bfill') # reindex close with all the dates of event, meaning to get the price at these dates.
    
    # 2) create out object
    
    out = pd.DataFrame(index=events_.index) # create a df with the index of events
    # find price of at end point of event. find price at beg of event. calculate return
    out['ret'] = px.loc[events_['t1'].values].values / px.loc[events_.index] - 1 
    out['bin'] = np.sign(out['ret'])
    
    return out


def map_num_coevents(close_idx, t1):

    """
    compute number of concurrent events per bar.
    :param close_idx:  data index of price dataframe
    :param t1: vertical barrier
    :return: the amount of events from close_idx and t1
    """
    t1 = t1.fillna(close_idx[-1])
    # count events spanning bar
    iloc = close_idx.searchsorted(np.array([t1.index[0], t1.max()]))
    count = pd.Series(0, index=close_idx[iloc[0]:iloc[1] + 1])

    for t_in, t_out in t1.iteritems():
        count.loc[t_in:t_out] += 1

    return count

def map_sample_tw(t1, num_co_events):
    """
    The average uniquess is the reciprocal of harminoc average of  count over the event's lifespan

    Use the function below

    :param t1: vertical barrier
    :param num_co_events: count for each price
    :return: a pd.Series of weights indexed by t1
    """
    wght = pd.Series(index=t1.index)
    for t_in, t_out in t1.iteritems():
        wght.loc[t_in] = (1.0 / num_co_events.loc[t_in:t_out]).mean()
    return wght

def map_sample_w(t1, num_co_events, close):
    """
    Return adjusted weights. It is prefered
    :param t1:
    :param num_co_events:
    :param close:
    :return:
    """

    ret = np.log(close).diff() # log returns
    wght = pd.Series(index=t1.index)
    for t_in,t_out in t1.iteritems():
        wght.loc[t_in] = (ret.loc[t_in:t_out]/ num_co_events.loc[t_in:t_out]).sum()
    return wght.abs()





def frac_diff(dataframe, d, thres=0.01):

    """
    Perform Standard Fracdiff - Expanding Window
    :param dataframe:
    :param d:
    :param thres:
    :return:
    """

    # Get the weights for the longgest series

    def get_weights(d, size):
        w = [1.]

        for k in range(1, size):
            w_ = -w[-1] / k * (d - k + 1)
            w.append(w_)
        w = np.array(w[::-1]).reshape(-1, 1)
        return w
    w = get_weights(d, dataframe.shape[0])

    # Determine initial calcs to be skipped based on weight loss threshold
    w_ = np.cumsum(abs(w))
    w_ /= w_[-1]
    skip = w_[w_ > thres].shape[0]

    # Apply weights to values
    df = {}
    series.index.tz = None

    for name in dataframe.columns:

        series_f = dataframe[[name]].fillna(method='ffill').dropna()
        df_ = pd.Series()

        for iloc in range(skip, series_f.shape[0]):
            loc = series_f.index[iloc]
            if not np.isfinite(series.loc[loc, name]):
                continue
            df_[loc] = np.dot(w[-(iloc + 1):, :].T, series_f.loc[:loc])[0, 0]
        df[name] = df_.copy(deep=True)
    df = pd.concat(df, axis=1)
    return df


# Chow-Type Dickey-Fuller Test

def lag_df(df0, lags):
    """
    apply lags to dataframe
    :param df0:
    :param lags:
    :return:
    """
    df1 = pd.DataFrame()

    if isinstance(lags, int):
        lags = range(lags + 1)
    else:
        lags = [int(lag) for lag in lags]

    for lag in lags:
        df_ = df0.shift(lag).copy(deep=True)
        df_.columns = [str(i) + '_' + str(lag) for i in df_.columns]
        df1 = df1.join(df_, how='outer')
    return df1


def get_yx(series, constant, lags):
    series_ = series.diff().dropna()
    x = lag_df(series_, lags).dropna()
    x.iloc[:, 0] = series.values[-x.shape[0] - 1:-1, 0]  # lagged level
    y = series_.iloc[-x.shape[0]:].values

    if constant != 'nc':
        x = np.append(x, np.ones((x.shape[0], 1)), axis=1)
        if constant[:2] == 'ct':
            trend = np.arange(x.shape[0]).reshape(-1, 1)
            x = np.append(x, trend, axis=1)
        if constant == 'ctt':
            x = np.append(x, trend ** 2, axis=1)

    return y, x


def get_betas(y, x):
    """
    fitting the adf specification
    :param y: 
    :param x: 
    :return: 
    """
    xy = np.dot(x.T, y)
    xx = np.dot(x.T, x)
    xxinv = np.linalg.inv(xx)

    b_mean = np.dot(xxinv, xy)

    #     b_mean = np.linalg.lstsq(x, y)[0]

    err = y - np.dot(x, b_mean)
    b_var = np.dot(err.T, err) / (x.shape[0] - x.shape[1]) * xxinv
    return b_mean, b_var


def get_bsadf(log_p, min_sl, constant, lags):
    """
    log_p: a pd series of log-prices
    min_sl: minimum sample length tau
    constant:
    'nc': no time trend,
    'ct': time trend,
    'ctt': a constant plus second degress plynomial time trend

    lags: the number of lags used in the ADF
    """
    y, x = get_yx(log_p, constant, lags)

    start_points = range(0, y.shape[0] + lags - min_sl + 1)

    bsadf = 0.05
    all_adf = []

    for start in start_points:
        y_, x_ = y[start:], x[start:]
        b_mean_, b_std_ = get_betas(y_, x_)
        b_mean_, b_std_ = b_mean_[0, 0], b_std_[0, 0] ** .5
        all_adf.append(b_mean_ / b_std_)
        if all_adf[-1] > float(bsadf):
            bsadf = all_adf[-1]

    out = {'Time': log_p.index[-1], 'gsadf': bsadf}
    return out

