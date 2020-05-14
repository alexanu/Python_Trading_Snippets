import functools
import typing
from multiprocessing import Pool, cpu_count

import numba
import numpy as np
import pandas as pd

"""
Chapter 3 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


def _vertical_barrier(timestamps: typing.Union[pd.DatetimeIndex, pd.MultiIndex], t_events: typing.Union[pd.DatetimeIndex, pd.MultiIndex], delta: pd.Timedelta):
    """
    Compute the vertical barrier from the triple barrier labeling method based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param timestamps: index of timestamps that represent bars
    :param t_events: index of timestamps that represent interesting points in the above timestamps
    :param delta: width (in time) of the label (the distance between beginning and the end of the label)
    :return events
    """
    tmp = timestamps.searchsorted(t_events + delta)
    tmp = tmp[tmp < timestamps.shape[0]]
    tmp = pd.Series(timestamps[tmp], index=t_events[:tmp.shape[0]], name='t_end').to_frame().set_index('t_end', append=True, drop=True).index

    return tmp


def vertical_barrier(timestamps: typing.Union[pd.DatetimeIndex, pd.MultiIndex], t_events: typing.Union[pd.DatetimeIndex, pd.MultiIndex], delta: pd.Timedelta):
    """
    For multiindex Dataframes (symbol and timestamp): Compute the vertical barrier from the triple barrier labeling method based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param timestamps: index of timestamps that represent bars
    :param t_events: index of timestamps that represent interesting points in the above timestamps
    :param delta: width (in time) of the label (the distance between beginning and the end of the label)
    :return events
    """
    if isinstance(timestamps, pd.MultiIndex):
        def vertical_barrier_tmp(_timestamps, _t_events):
            _timestamps = _timestamps.index

            if isinstance(_t_events, pd.MultiIndex):
                symbol_ind = _timestamps.names.index('symbol')
                symbol = _timestamps[0][symbol_ind]
                _t_events = pd.Series(index=_t_events).loc[pd.IndexSlice[:, symbol]].index if symbol_ind == 1 else pd.Series(index=_t_events).loc[symbol].index

            if isinstance(_timestamps, pd.MultiIndex):
                symbol_ind = _timestamps.names.index('symbol')
                symbol = _timestamps[0][symbol_ind]
                _timestamps = pd.Series(index=_timestamps).loc[pd.IndexSlice[:, symbol]].index if symbol_ind == 1 else pd.Series(index=_timestamps).loc[symbol].index
            else:
                symbol_ind, symbol = -1, None

            tmp = pd.DataFrame(index=_vertical_barrier(timestamps=_timestamps, t_events=_t_events, delta=delta))

            if symbol_ind > -1:
                tmp['symbol'] = symbol
                tmp.set_index('symbol', append=True, inplace=True, drop=True)
                if symbol_ind == 0:
                    tmp = tmp.reorder_levels(['symbol', 'timestamp', 't_end'])
                elif symbol_ind == 1:
                    tmp = tmp.reorder_levels(['timestamp', 't_end', 'symbol'])

            return pd.DataFrame(index=tmp.index)

        vertical_barrier_tmp = functools.partial(vertical_barrier_tmp, _t_events=t_events)

        return pd.DataFrame(index=timestamps).groupby(level='symbol', group_keys=False, sort=False).apply(vertical_barrier_tmp).index
    else:
        return _vertical_barrier(timestamps=timestamps, t_events=t_events, delta=delta)


def _triple_barriers(data: pd.Series, pt: pd.Series, sl: pd.Series, vb_delta: pd.Timedelta, seed: pd.Series = None, side: pd.Series = None):
    """
    Triple barrier labeling for single index series
    :param data: data to be labeled
    :param pt: profit taking barrier
    :param sl: stop loss barrier
    :param vb_delta: vertical barrier delta
    :param seed: True/False array. If True, the given moment will be used to seed one triple barrier. If false, the given moment will be omitted
    :param side: if side is specified, then meta-labeling is enabled
    :return dataframe with first timestamp of threshold crossing, type of threshold crossing and return
    """
    result = data.to_frame()
    result['interval_end'] = data.index.copy()
    result['interval_end'].values.fill(np.datetime64('NaT'))
    result['returns'] = data.copy()
    result['returns'].values.fill(np.nan)

    result.drop(data.name, axis=1, inplace=True)

    seed = seed.values if seed is not None else np.full(data.size, True)

    if side is None:
        result['side'] = np.zeros(data.shape, dtype=np.int8)

        __triple_barriers_side_jit(data=data.values,
                                   timestamps=data.index.values,
                                   seed=seed,
                                   vb_delta=vb_delta.to_timedelta64(),
                                   pt=pt.values,
                                   sl=sl.values,
                                   interval_end=result['interval_end'].values,
                                   side=result['side'].values,
                                   returns=result['returns'].values)
    else:
        result['size'] = np.zeros(data.shape, dtype=np.int8)

        __triple_barriers_size_jit(data=data.values,
                                   timestamps=data.index.values,
                                   seed=seed,
                                   vb_delta=vb_delta.to_timedelta64(),
                                   pt=pt.values,
                                   sl=sl.values,
                                   interval_end=result['interval_end'].values,
                                   side=side.values,
                                   returns=result['returns'].values,
                                   size=result['size'].values)

    return result


@numba.jit(nopython=True)
def __triple_barriers_side_jit(data: np.array, timestamps: np.array, seed: np.array, vb_delta: np.timedelta64, pt: np.array, sl: np.array, interval_end: np.array, side: np.array, returns: np.array):
    """This function cannot be local to _triple_barriers, because it will be compiled on every groupby"""

    for i in range(data.size):
        if seed[i] is True:
            end = timestamps[i] + vb_delta

            current = data[i]

            pt_delta = pt[i]
            sl_delta = -sl[i]

            for i_end in range(i + 1, data.size):
                if timestamps[i_end] > end:
                    if i_end - 1 > i:
                        interval_end[i], side[i], returns[i] = timestamps[i_end - 1], 0, 0

                    break

                ret = current / data[i_end] - 1

                if ret > pt_delta != 0 or ret < sl_delta != 0:
                    interval_end[i], side[i], returns[i] = timestamps[i_end], np.sign(ret), ret
                    break


@numba.jit(nopython=True)
def __triple_barriers_size_jit(data: np.array, timestamps: np.array, seed: np.array, vb_delta: np.timedelta64, pt: np.array, sl: np.array, interval_end: np.array, side: np.array, returns: np.array, size: np.array):
    """This function cannot be local to _triple_barriers, because it will be compiled on every groupby"""

    for i in range(data.size):
        if seed[i] is True:
            end = timestamps[i] + vb_delta

            current = data[i]

            pt_delta = pt[i]
            sl_delta = -sl[i]

            for i_end in range(i + 1, data.size):
                if timestamps[i_end] > end:
                    if i_end - 1 > i:
                        interval_end[i], size[i], returns[i] = timestamps[i_end - 1], 0, 0

                    break

                ret = current / data[i_end] - 1

                if ret > pt_delta != 0 or ret < sl_delta != 0:
                    interval_end[i], returns[i] = timestamps[i_end], ret
                    size[i] = 1 if np.sign(ret) == side[i] else 0
                    break


def __triple_barriers_groupby(data: pd.DataFrame, vb: pd.Timedelta):
    """
    Triple barrier labeling for single index series, specific for groupby
    :param data: dataframe with 3 (or 4) columns - the data itself, pt (profit take) and sl (stop loss) to be labeled.
        If 'side' is specified, then meta-labeling is enabled
    :param vb: vertical barrier delta
    :return dataframe with first timestamp of threshold crossing, type of threshold crossing and return
    """
    if isinstance(data.index, pd.MultiIndex):
        symbol_ind = data.index.names.index('symbol')
        symbol = data.index[0][symbol_ind]
        data = data.loc[pd.IndexSlice[:, symbol]] if symbol_ind == 1 else data.loc[symbol]
    else:
        symbol_ind, symbol = -1, None

    result = _triple_barriers(data['data'],
                              pt=data['pt'] if 'pt' in data else None,
                              sl=data['sl'] if 'sl' in data else None,
                              vb_delta=vb,
                              seed=data['seed'] if 'seed' in data else None,
                              side=data['side'] if 'side' in data else None)

    if symbol_ind > -1:
        result['symbol'] = symbol
        result.set_index('symbol', append=True, inplace=True, drop=True)
        if symbol_ind == 0:
            result = result.reorder_levels(['symbol', 'timestamp'])

    return result


def triple_barriers(data: pd.Series, pt: pd.Series, sl: pd.Series, vb: pd.Timedelta, seed: pd.Series = None, side: pd.Series = None, parallel=True):
    """
    Triple barrier labeling for single multiindex series
    :param data: data to be labeled
    :param pt: profit taking barrier
    :param sl: stop loss barrier
    :param vb: vertical barrier delta
    :param seed: True/False array. If True, the given moment will be used to seed one triple barrier. If false, the given moment will be omitted
    :param side: if side is specified, then meta-labeling is enabled
    :param parallel: run in parallel
    :return dataframe with first timestamp of threshold crossing, type of threshold crossing and return
    """
    if isinstance(data.index, pd.DatetimeIndex):
        return _triple_barriers(data=data, pt=pt, sl=sl, side=side, seed=seed, vb_delta=vb)
    else:
        data = data.astype(np.float32).to_frame().rename(columns={data.name: 'data'}) if data.dtype != np.float32 else data.to_frame().rename(columns={data.name: 'data'})

        if pt is not None:
            data['pt'] = pt.astype(np.float32) if pt.dtype != np.float32 else pt

        if sl is not None:
            data['sl'] = sl.astype(np.float32) if sl.dtype != np.float32 else sl

        if side is not None:
            data['side'] = side.astype(np.int8) if side.dtype != np.int8 else side

        if seed is not None:
            data['seed'] = seed

        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.starmap(__triple_barriers_groupby, [(group, vb) for name, group in data.groupby(level='symbol', group_keys=False, sort=False)])
                return pd.concat(ret_list)
        else:
            return data.groupby(level='symbol', group_keys=False, sort=False).apply(__triple_barriers_groupby, vb=vb)
