# Source: harttraveller/roughsr2

import pandas as pd
import numpy as np

def find_maximums(data,increment):
    start = 0
    end = increment
    maximums = pd.Series([])
    for i in range(int(len(data)/increment)):
        maximums = maximums.append(pd.Series(int(data[start:end].max())))
        start += increment
        end += increment
    maximums = list(maximums)
    maximums.sort()
    return maximums

def find_minimums(data,increment):
    start = 0
    end = increment
    minimums = pd.Series([])
    for i in range(int(len(data)/increment)):
        minimums = minimums.append(pd.Series(int(data[start:end].min())))
        start += increment
        end += increment
    minimums = list(minimums)
    minimums.sort()
    return minimums

def find_resistance(data):
    cutoff = 2
    increment = 10
    maximums = find_maximums(data=data,increment=increment)

    histogram = np.histogram(maximums,bins=(int(len(maximums)/increment*increment)))
    histogram_occurences = pd.DataFrame(histogram[0])
    histogram_occurences.columns = ['occurence']
    histogram_splits = pd.DataFrame(histogram[1])
    histogram_splits.columns = ['bins']

    histogram_bins = []
    for x in histogram_splits.index:
        element = []
        if x < len(histogram_splits.index)-1:
            element.append(int(histogram_splits.iloc[x]))
            element.append(int(histogram_splits.iloc[x+1]))
            histogram_bins.append(element)

    histogram_bins = pd.DataFrame(histogram_bins)
    histogram_bins['occurence'] = histogram_occurences
    histogram_bins.columns = ['start','end','occurence']

    histogram_bins = histogram_bins[histogram_bins['occurence'] >= cutoff]
    histogram_bins.index = range(len(histogram_bins))

    data = list(data)
    data.sort()
    data = pd.Series(data)

    lst_maxser = []
    for i in histogram_bins.index:
        lst_maxser.append(data[(data > histogram_bins['start'][i]) & (data < histogram_bins['end'][i])])

    lst_maxser = pd.Series(lst_maxser)

    lst_resistance=[]

    for i in lst_maxser.index:
        lst_resistance.append(lst_maxser[i].mean())

    resistance_df = pd.DataFrame(lst_resistance)
    resistance_df.columns = ['resistance']
    resistance_df.dropna(inplace=True)
    resistance_df.index = range(len(resistance_df))
    resistance_ser = pd.Series(resistance_df['resistance'])

    return resistance_ser

def find_support(data):
    cutoff = 2
    increment = 10

    minimums = find_minimums(data=data,increment=increment)

    histogram = np.histogram(minimums,bins=(int(len(minimums)/increment*increment)))
    histogram_occurences = pd.DataFrame(histogram[0])
    histogram_occurences.columns = ['occurence']
    histogram_splits = pd.DataFrame(histogram[1])
    histogram_splits.columns = ['bins']

    histogram_bins = []
    for x in histogram_splits.index:
        element = []
        if x < len(histogram_splits.index)-1:
            element.append(int(histogram_splits.iloc[x]))
            element.append(int(histogram_splits.iloc[x+1]))
            histogram_bins.append(element)

    histogram_bins = pd.DataFrame(histogram_bins)
    histogram_bins['occurence'] = histogram_occurences
    histogram_bins.columns = ['start','end','occurence']

    histogram_bins = histogram_bins[histogram_bins['occurence'] >= cutoff]
    histogram_bins.index = range(len(histogram_bins))

    data = list(data)
    data.sort()
    data = pd.Series(data)

    lst_minser = []
    for i in histogram_bins.index:
        lst_minser.append(data[(data > histogram_bins['start'][i]) & (data < histogram_bins['end'][i])])

    lst_minser = pd.Series(lst_minser)

    lst_support=[]

    for i in lst_minser.index:
        lst_support.append(lst_minser[i].mean())

    support_df = pd.DataFrame(lst_support)
    support_df.columns = ['support']
    support_df.dropna(inplace=True)
    support_df.index = range(len(support_df))
    support_ser = pd.Series(support_df['support'])

    return support_ser
