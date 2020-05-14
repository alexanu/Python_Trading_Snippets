import datetime

import numpy as np
import pandas as pd


def adjust_df(data: pd.DataFrame, adjustments: pd.DataFrame):
    """
    IMPORTANT !!! This method supports MultiIndex dataframes
    :param data: dataframe with data.
    :param adjustments: list of adjustments in the form of [(date, split_factor/dividend_amount, 'split'/'dividend'), ...]
    :return adjusted data
    """
    if not data.empty and not adjustments.empty:
        idx = pd.IndexSlice
        start = data.iloc[0].name[0] if isinstance(data.iloc[0].name, tuple) else data.iloc[0].name
        end = data.iloc[-1].name[0] if isinstance(data.iloc[0].name, tuple) else data.iloc[-1].name

        adjustments = adjustments.loc[idx[start:end, list(adjustments.index.levels[1]), :, :], :].sort_index(ascending=False)

        if isinstance(data.index, pd.MultiIndex):
            for (_, row) in adjustments.iterrows():
                if row.name[2] == 'split':
                    adjust_split_multiindex(data=data, symbol=row.name[1], split_date=row.name[0], split_factor=row[0])
                elif row.name[2] == 'dividend':
                    adjust_dividend_multiindex(data=data, symbol=row.name[1], dividend_date=row.name[0], dividend_amount=row[0])
        else:
            for (_, row) in adjustments.iterrows():
                if row.name[2] == 'split':
                    adjust_split(data=data, split_date=row.name[0], split_factor=row[0])
                elif row.name[2] == 'dividend':
                    adjust_dividend(data=data, dividend_date=row.name[0], dividend_amount=row[0])

    return data


def adjust(data, adjustments: pd.DataFrame):
    """
    IMPORTANT !!! This method supports single index df
    :param data: dataframe with data.
    :param adjustments: list of adjustments in the form of [(date, split_factor/dividend_amount, 'split'/'dividend'), ...]
    :return adjusted data
    """
    adjustments.sort(key=lambda x: x[0], reverse=True)

    for (_, row) in adjustments.iterrows():
        if row.name[2] == 'split':
            adjust_split(data=data, split_date=row.name[0], split_factor=row[0])
        elif row.name[2] == 'dividend':
            adjust_dividend(data=data, dividend_date=row.name[0], dividend_amount=row[0])

    return data


def adjust_dividend(data, dividend_amount: float, dividend_date: datetime.date):
    if isinstance(data, pd.DataFrame):
        if len(data) > 0:
            dividend_date = datetime.datetime.combine(dividend_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0]['timestamp'].tz)

            if dividend_date > data.iloc[0]['timestamp']:
                for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                    data[c] -= dividend_amount
            elif dividend_date > data.iloc[-1]['timestamp']:
                for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                    data.loc[data['timestamp'] < dividend_date, c] -= dividend_amount
    elif dividend_date > data['timestamp']:
        for c in [c for c in {'close', 'high', 'open', 'low', 'ask', 'bid', 'last'} if c in data.keys()]:
            data[c] -= dividend_amount


def adjust_split(data, split_factor: float, split_date: datetime.date):
    if split_factor > 0:
        if isinstance(data, pd.DataFrame):
            if len(data) > 0:
                split_date = datetime.datetime.combine(split_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0]['timestamp'].tz)

                if split_date > data.iloc[-1]['timestamp']:
                    for c in [c for c in ['volume', 'total_volume', 'last_size'] if c in data.columns]:
                        data[c] = (data[c] * (1 / split_factor)).astype(np.uint64)

                    for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                        data[c] *= split_factor
                elif split_date > data.iloc[0]['timestamp']:
                    for c in [c for c in ['volume', 'total_volume', 'last_size'] if c in data.columns]:
                        data.loc[data['timestamp'] < split_date, c] *= (1 / split_factor)
                        data[c] = data[c].astype(np.uint64)

                    for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                        data.loc[data['timestamp'] < split_date, c] *= split_factor
        elif split_date > data['timestamp']:
            for c in [c for c in {'close', 'high', 'open', 'low', 'volume', 'total_volume', 'ask', 'bid', 'last', 'last_size'} if c in data.keys()]:
                if c in ('volume', 'total_volume', 'last_size'):
                    data[c] = int(data[c] * (1 / split_factor))
                else:
                    data[c] *= split_factor


def adjust_dividend_multiindex(data: pd.DataFrame, symbol: str, dividend_amount: float, dividend_date: datetime.date):
    if len(data) > 0 and symbol in data.index.levels[1]:
        dividend_date = datetime.datetime.combine(dividend_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0].name[0].tz)

        idx = pd.IndexSlice

        if data.loc[idx[:, symbol], :].iloc[0].name[0] <= dividend_date <= data.loc[idx[:, symbol], :].iloc[-1].name[0]:
            for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                data.loc[idx[:dividend_date, symbol], c] -= dividend_amount


def adjust_split_multiindex(data: pd.DataFrame, symbol: str, split_factor: float, split_date: datetime.date):
    if split_factor > 0 and len(data) > 0 and symbol in data.index.levels[1]:
        split_date = datetime.datetime.combine(split_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0].name[0].tz)

        idx = pd.IndexSlice

        if data.loc[idx[:, symbol], :].iloc[0].name[0] <= split_date <= data.loc[idx[:, symbol], :].iloc[-1].name[0]:
            for c in [c for c in ['volume', 'total_volume', 'last_size'] if c in data.columns]:
                data.loc[idx[:split_date, symbol], c] *= (1 / split_factor)
                data.loc[idx[:split_date, symbol], c] = data[c].astype(np.uint64)
            for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                data.loc[idx[:split_date, symbol], c] *= split_factor


def exclude_splits(data: pd.Series, splits: pd.Series, quarantine_length: int):
    """
    exclude data based on proximity to split event
    :param data: single/multiindex series with boolean values for each timestamp - True - include, False - exclude.
    :param splits: splits dataframe
    :param quarantine_length: number of moments to exclude
    :return adjusted data
    """

    if isinstance(data.index, pd.MultiIndex):
        def tmp(datum):
            symbol = datum.index[0][datum.index.names.index('symbol')]
            datum_index = datum.xs(symbol, level='symbol').index

            try:
                for _s in splits.xs(symbol, level='symbol').index:
                    _i = datum_index.searchsorted(_s[0])
                    if 0 < _i < datum_index.size:
                        datum.iloc[_i: _i + quarantine_length] = False
            except KeyError:
                pass

            return datum

        data = data.groupby(level='symbol').apply(tmp)
    else:
        for s in splits.index:
            i = data.index.searchsorted(s[0])
            if 0 < i < data.size:
                data.iloc[i: i + quarantine_length] = False

    return data
