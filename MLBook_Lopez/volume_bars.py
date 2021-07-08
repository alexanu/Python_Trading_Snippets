def get_volume_bar_indices(volume, threshold):
    '''
    Sample bars with equal volume. This function can use dollar value exchanged (`df["close"] * df["volume"]`)
    as the volume parameter to get dollar bars since the threshold logic is the same. The sampling is done when the sum
    of the bars' volume exceeds the threshold. Then, the sum is reset and continues over the remaining bars.

    :param volume: Series of the volume for each sample point
    :param threshold: Integer threshold value
    :return: List of indices to sample
    '''
    total_volume = 0
    idx = []
    for i, sample_volume in enumerate(volume):
        total_volume += sample_volume
        if total_volume >= threshold:
            idx.append(i)
            total_volume = 0
    return idx


def get_volume_bars(to_sample, threshold, volume_column="volume"):
    """
    Samples bars with equal volume roughly equal to the threshold.
    :param to_sample: DataFrame or Series
    :param threshold: Integer threshold value
    :param volume_column: Name of the column with the volume.
    :return: The rows of to_sample that were sampled.
    """
    idx = get_volume_bar_indices(to_sample[volume_column], threshold)
    return to_sample.iloc[idx]


def get_dollar_bars(to_sample, threshold, value_column="close", volume_column="volume"):
    """
    Calculates the value exchanged and samples bars with equal value exchanged roughly equal to the threshold.
    This method can be superior to the volume bars method when the value of one unit of the asset changes over time.
    Note how buying $10,000 of a stock takes half as much volume after the price of a stock doubles because each stock
    is twice as expensive, despite the value exchanged being the same.

    :param to_sample: DataFrame or Series
    :param threshold: Integer threshold value
    :param value_column: Name of the column with the value of one unit of the asset exchanged.
    :param volume_column: Name of the column with the volume.
    :return: The rows of to_sample that were sampled.
    """
    value_exchanged = to_sample[value_column] * to_sample[volume_column]
    idx = get_volume_bar_indices(value_exchanged, threshold)
    return to_sample.iloc[idx]
