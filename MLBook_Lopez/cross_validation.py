import numpy as np
import pandas as pd

"""
Chapter 6 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


def cv_split(data: pd.DataFrame, splits: int, current_split: int, embargo: float = 0.01):
    """
    cross validation data split, as implemented in chapter 6 of the book
    :param data: single/multiindex dataframe with boolean values for each timestamp - True - include, False - exclude.
    :param splits: number of splits
    :param current_split: current split (0 based)
    :param embargo: current split (0 based)
    :return timestamps that exclude the purged areas and all other splits except the specified
    """

    if isinstance(data.index, pd.MultiIndex):
        data = data.reset_index(level=data.index.names.index('symbol'), drop=True).sort_index()

    result = pd.Series(True, index=data.index, dtype=np.bool)

    purge = cv_purge(data['interval_end'], splits=splits, current_split=current_split, embargo=embargo)

    if purge.size > 0:
        if current_split == 0:
            result.loc[purge[-1]:] = False
        elif current_split == splits - 1:
            result.loc[:purge[0]] = False
        else:
            result.loc[:purge[0]] = False
            result.loc[purge[-1]:] = False

        result.loc[purge] = False

    return result[result].index


def cv_split_reverse(data: pd.DataFrame, splits: int, current_split: int, embargo: float = 0.01):
    """
    cross validation data split, as implemented in chapter 6 of the book
    :param data: single/multiindex dataframe with boolean values for each timestamp - True - include, False - exclude.
    :param splits: number of splits
    :param current_split: current split (0 based)
    :param embargo: current split (0 based)
    :return timestamps that exclude the purged areas and the specified split, but including all other splits
    """

    if isinstance(data.index, pd.MultiIndex):
        data = data.reset_index(level=data.index.names.index('symbol'), drop=True).sort_index()

    result = pd.Series(True, index=data.index, dtype=np.bool)

    purge = cv_purge(data['interval_end'], splits=splits, current_split=current_split, embargo=embargo)

    if purge.size > 0:
        if current_split == 0:
            result.loc[:purge[-1]] = False
        elif current_split == splits - 1:
            result.loc[purge[0]:] = False
        else:
            result.loc[purge[0]:purge[-1]] = False

        result.loc[purge] = False

    return result[result].index


def cv_purge(data: pd.Series, splits: int, current_split: int, embargo: float = 0.01):
    """
    cross validation data split purge areas, as implemented in chapter 6 of the book
    :param data: single index series with pd.DateTimeIndex as index (interval start) and pd.DateTimeIndex as value (interval end).
    This series is produced by the labeling method
    :param splits: number of splits
    :param current_split: current split (0 based)
    :param embargo: current split (0 based)
    :return boolean series indicating the purged data marked as false
    """
    result = pd.Series(True, index=data.index, dtype=np.bool)

    timedelta = data.index.max() - data.index.min()
    split_length = timedelta / splits
    current_split_start, current_split_end = data.index.min() + split_length * current_split, data.index.min() + split_length * (current_split + 1)

    result.loc[((data.index < current_split_start) & (data >= current_split_start))
               | ((data.index <= current_split_end) & (data > current_split_end))] = False

    if current_split < splits - 1 and embargo > 0:
        embargo_start = result[~result]
        embargo_start = embargo_start.index[-1] if embargo_start.size > 0 else current_split_end
        result[embargo_start:embargo_start + timedelta * embargo] = False

    return result[~result].index
