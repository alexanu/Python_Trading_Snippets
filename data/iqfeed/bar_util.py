import pandas as pd


def merge_snapshots(s1: pd.DataFrame, s2: pd.DataFrame) -> pd.DataFrame:
    """
    merge two bar dataframes (with multiindex [symbol, timestamp]
    :param s1: DataFrame
    :param s2: DataFrame
    :return: DataFrame
    """
    if s1.empty and not s2.empty:
        return s2
    elif s2.empty and not s1.empty:
        return s1
    elif s1.empty and s2.empty:
        return

    return pd.concat([s1, s2]).sort_index()


def reindex_and_fill(df: pd.DataFrame, index) -> pd.DataFrame:
    """
    reindex DataFrame using new index and fill the missing values
    :param df: DataFrame
    :param index: new index to use
    :return:
    """

    df = df.reindex(index)
    df.drop(['symbol', 'timestamp'], axis=1, inplace=True)
    df.reset_index(inplace=True)
    df.set_index(index, inplace=True)

    for c in [c for c in ['volume', 'number_of_trades'] if c in df.columns]:
        df[c].fillna(0, inplace=True)

    if 'close' in df.columns:
        df['close'] = df.groupby(level=0)['close'].fillna(method='ffill')
        df['close'] = df.groupby(level=0)['close'].fillna(method='backfill')
        op = df['close']

        for c in [c for c in ['open', 'high', 'low'] if c in df.columns]:
            df[c].fillna(op, inplace=True)

    df = df.groupby(level=0).fillna(method='ffill')

    df = df.groupby(level=0).fillna(method='backfill')

    return df


def expand(df: pd.DataFrame, steps: int, max_length: int=None) -> pd.DataFrame:
    """
    expand DataFrame with steps at the end
    :param df: DataFrame
    :param steps: number of steps to expand at the end
    :param max_length: if max_length reached, truncate from the beginning
    :return:
    """

    if len(df.index.levels[1]) < 2:
        return df

    diff = df.index.levels[1][1] - df.index.levels[1][0]
    new_index = df.index.levels[1].append(pd.date_range(df.index.levels[1][len(df.index.levels[1]) - 1] + diff, periods=steps, freq=diff))

    multi_index = pd.MultiIndex.from_product([df.index.levels[0], new_index], names=['symbol', 'timestamp']).sort_values()

    result = df.reindex(multi_index)

    if max_length is not None:
        result = df.groupby(level=0).tail(max_length)

    return result


def synchronize_timestamps(df) -> pd.DataFrame:
    """
    synchronize the timestamps for all symbols in a DataFrame. Fill the values automatically
    :param df: DataFrame
    :return: df
    """
    multi_index = pd.MultiIndex.from_product([df.index.levels[0].unique(), df.index.levels[1].unique()], names=['symbol', 'timestamp']).sort_values()
    return reindex_and_fill(df, multi_index)
