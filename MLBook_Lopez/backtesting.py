# Source: https://github.com/TheRockXu/aifin

"""
Some functions to help with prepare for backtesting
"""
import numpy as np
import pandas as pd
from scipy.stats import *

def avg_active_signals(signals):
    """
    compute the average signal among those active
    :param signals:
    :return:
    """
    # 1) time points where singal change

    t_pnts = set(signals['t1'].dropna().values)  # get all the unique datetimes
    t_pnts = t_pnts.union(signals.index.values)  # the union of beg dates and t1 dates
    t_pnts = list(t_pnts)
    t_pnts.sort()  # we get a ordered list of all the dates from t1
    out = pd.Series()

    for loc in t_pnts:  # for every date points

        # find singals begins earlier than this dates, ends later than this date
        df0 = (signals.index.values <= loc) & ((loc < signals['t1'])) | pd.isnull(signals['t1'])
        act = signals[df0].index  # get the signals within this date range
        if len(act) > 0:
            out[loc] = signals.loc[act, 'signal'].mean()
        else:
            out[loc] = 0  # no active sigal at this time

    return out


def discrete_signal(signal0, step_size):
    """
    Discretize the bet size, it is likely that small trades will be triggered with every prediction
    :param signal0:
    :param step_size:
    :return:
    """
    signal1 = (signal0/step_size).round()*step_size
    signal1[signal1>1] = 1 # cap
    signal1[signal1<-1] = -1 # floor
    return signal1


def get_signal(events, step_size, prob, pred, num_classes, **kargs):

    """
    the translation from probability to bet size.

    :param events: event df
    :param step_size: step size for discretization of signals
    :param prob: the predicted probabilities
    :param pred: the predictions
    :param num_classes: number of classes
    :param kargs:
    :return:
    """
    if prob.shape[0] == 0:
        return pd.Series()
    # general signals

    signal0 = (prob - 1. / num_classes) / (prob * (1. - prob)) ** -.5
    signal0 = pred * (2 * norm.cdf(signal0) - 1)
    df0 = signal0.to_frame('signal').join(events[['t1']], how='left')
    df0 = avg_active_signals(df0)
    signal1 = discrete_signal(signal0=df0, step_size=step_size)
    return signal1

def bin_hr(sl, pt, freq, t_sr):
    """
    Given a training rule characterized by the parameters (sl, pt, freq), what is the min precision p required to achieve a Sharpe ratio t_sr?
    :param sl: stop loss
    :param pt: profit taking
    :param freq: frequency = bets/per year
    :param t_sr: target sharpe ratio
    :return:
    """

    a = (freq+t_sr**2)*(pt-sl)**2
    b = (2*freq*sl-t_sr**2*(pt-sl))*(pt-sl)
    c = freq*sl**2
    p=(-b+(b**2-4*a*c)**.5)/(2.*a)
    return p


def bin_freq(sl, pt, p, t_sr):
    """
    Given a trading rule characterized by the parameters {sl, pt, freq}, what's the number of bets/year needed to achieve a sharpe ratio
    :param sl:
    :param pt:
    :param p:
    :param t_sr:
    :return:
    """
    freq = (t_sr * (pt - sl)) ** 2 * p * (1 - p) / ((pt - sl) * p + sl) ** 2

    return freq


def prob_failure(ret, freq, t_sr):
    """
    The Probability of Strategy Failure
    :param ret: return series
    :param freq: trades per year
    :param t_sr: target sharpe ratio
    :return:
    """
    r_pos = ret[ret > 0].mean()
    r_neg = ret[ret < 0].mean()

    p = ret[ret > 0].shape[0] / float(ret.shape[0])
    thres_p = bin_hr(r_neg, r_pos, freq, t_sr)

    risk = norm.cdf(thres_p, p, p * (1 - p))

    return risk