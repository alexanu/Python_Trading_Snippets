from multiprocessing import cpu_count
from multiprocessing.pool import Pool

import numpy as np
import pandas as pd

"""
Chapter 5 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


def get_weights_ffd(d, threshold=1e-5):
    """
    Obtain the weights for the binomial representation of time series
    :param d: coefficient
    :param threshold: threshold
    """

    w, k = [1.], 1
    while abs(w[-1]) > threshold:
        w_ = -w[-1] / k * (d - k + 1)
        w.append(w_)
        k += 1

    w = np.array(w[::-1])

    return w


def _frac_diff_ffd(data: pd.Series, d: float, threshold=1e-5):
    """
    Fractionally Differentiated Features Fixed Window
    :param data: data
    :param d: difference coefficient
    :param threshold: threshold
    """
    # 1) Compute weights for the longest series
    w = get_weights_ffd(d, threshold)
    # 2) Apply weights to values
    return data.rolling(w.size, min_periods=w.size).apply(lambda x: np.dot(x, w), raw=True).dropna()


def frac_diff_ffd(data: pd.Series, d: float, threshold=1e-5, parallel=False):
    """
    Fractionally Differentiated Features Fixed Window
    :param data: data
    :param d: difference coefficient
    :param threshold: threshold
    :param parallel: run in parallel
    """
    if isinstance(data.index, pd.MultiIndex):
        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.starmap(_frac_diff_ffd, [(group, d, threshold) for name, group in data.groupby(level='symbol', group_keys=False, sort=False)])
                return pd.concat(ret_list)
        else:
            return data.groupby(level='symbol', group_keys=False, sort=False).apply(_frac_diff_ffd, d=d, threshold=threshold)
    else:
        return _frac_diff_ffd(data=data, d=d, threshold=threshold)

# def plot_weights(d_range, n_plots):
#     import matplotlib.pyplot as plt
#
#     w = pd.DataFrame()
#     for d in np.linspace(d_range[0], d_range[1], n_plots):
#         w_ = get_weights_ffd(d)
#         w_ = pd.DataFrame(w_, index=range(w_.shape[0])[::-1], columns=[d])
#         w = w.join(w_, how='outer')
#     ax = w.plot()
#     ax.legend(loc='upper left');
#     plt.show()
#     return
#
#
# if __name__ == '__main__':
#     plot_weights(d_range=[0, 1], n_plots=11)
#     plot_weights(d_range=[1, 2], n_plots=11)
