#!/usr/bin/python
# -*- coding: utf-8 -*-

#==========================================================
#=== perpetualSeries_ContinuousFuturesContractModel.py ====
#==========================================================

# Purpose
#----------------------------------------------------------
# Instead of a one day transition scheme like the absolute,
# proportional, or Panama Adjustment scheme, we will define a transition
# interval of n days. Over that n days the Forward contract price will
# consist of a weighted linear combination of the near contract and the far
# contract. In mathematical terms the price over an n day period with F=Far
# contract and N= Near contract, would look like:
#       P_1 = (1-1/n)F_1 + (1/n)N_1
#       P_2 = (1-2/n)F_2 + (2/n)N_2
#           .
#           .
#           .
#       P_n = (1-n/n)F_n + (n/n)N_n = N_n
#
# At which point we've successfully smoothed/merged into the near futures 
# contract.

from __future__ import print_function

import datetime as dt
import numpy as np
import pandas as pd
import quandl

def futures_rollover_weights(start_date, expiry_dates, contracts, transition_interval=5):
    """
    This function creates the weights that we will apply to the Far and
    Near contracts when creating the linear combination of the
    current price. The resulting (constant) matrix can be multiplied with
    another DataFrame containing settle prices of each contract resulting 
    in the vector of our continues time series roll over priced
    contracts
    """

    # Create the sequence of dates from the earliest contract start date
    # to the end date of the final contract
    dates = pd.date_range(start_date, expiry_dates[-1], freq='B')

    # Initialize the transition period weights that will store the weights for
    # each contract calculation (note, normalized between 0.0 and 1.0)
    transition_weights = pd.DataFrame(np.zeros((len(dates), len(contracts))),
            index=dates, columns=contracts)
    prev_date = transition_weights.index[0]

    # For each contract take the settlement date and transition_interval
    # and calculate the specific weights for that time period
    for i, (item, ex_date) in enumerate(expiry_dates.iteritems()):
        if i < len(expiry_dates) -1:
            transition_weights.ix[prev_date:ex_date-pd.offsets.BDay(), item] = 1
            interval_rng = pd.date_range(end=ex_date - pd.offsets.BDay(),
                    periods=transition_interval+1, freq='B')
            # Now create the consecutive weights ([0, 1/n, 2/n, ... , n/n=1) and
            # create the daily linear combination pair weights
            decay_weights = np.linspace(0,1, transition_interval+1)
            transition_weights.ix[interval_rng, item] = 1- decay_weights
            transition_weights.ix[interval_rng, expiry_dates.index[i+1]] = decay_weights
        else:
            transition_weights.ix[prev_date:, item] =1
        prev_date = ex_date
    return transition_weights

# The usual python code to execute the desired program from commandline. The
# input will be symbol. The main function will download the near and far
# contracts creating a single DataFrame for both, construct the
# transition_weights matrix, then calculate the continuous series of both
# prices with the weights applied
#TODO: this should be more functional and less hardcoded
if __name__ == "__main__":
    import sys
    symbol = str(sys.argv[1])

    # Download the Front and Back (near and far) futures contracts
    nearString = "CME/%sF2014" % symbol
    near = quandl.get(nearString, authtoken="xEijovVJdKK_nGUGzEMG")

    farString = "CME/%sG2014" % symbol
    far = quandl.get(farString, authtoken="xEijovVJdKK_nGUGzEMG")

    df = pd.DataFrame({'%sF2014' % symbol: near['Settle'], '%sG2014' % symbol: far['Settle']}, index = far.index)

    # Create the dict of expiration dates for each contract
    # TODO: this also is entirely too hard coded
    expiry_dates = pd.Series(
            {'%sF2014' % symbol: dt.datetime(2013, 12, 19),
             '%sG2014' % symbol: dt.datetime(2014, 2, 21)}).order()
    
    # Construct the transition weighting matrix
    weights = futures_rollover_weights(near.index[0], expiry_dates,
            df.columns)

    # Lastly calculate the continuous future time series (including the now
    # smoothed transition interval) for the contracts
    cts = (df*weights).sum(1).dropna()

    # Display last 60 data points
    print(cts.tail(60))

