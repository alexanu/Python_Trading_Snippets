#==========================================================
#================= hurstExponentCalc.py ===================
#==========================================================

# Purpose
#----------------------------------------------------------
# The Hurst Exponent is a very useful scalar quantity to calculate when
# determining if a time series is truly behaving like a random walk, 
# mean-reverting, or trending in a given direction. In short, for an 
# arbitrary time lag \tau the variance of \tau is given by
#   Var(\tau)=<|log(t+\tau)-log(t)|^2>
# where < ... > refers to the average over all values of t.
# For Gaussian Brownian Motion (GBM) this quantity var(\tau) is proportional to
# \tau. We therefor can modify the relation with an exponent of 2H (H being the
# Hurst Exponent) to calculate if H is equal to .5 (GBM), less than .5 (mean
# reverting), or greater than .5 (trending). The magnitude of |H-.5| does
# indicate how strong the connection is:
#       <|log(t+\tau)-log(t)|^2> ~\tau^{2H}

from __future__ import print_function

from numpy import cumsum, log, polyfit, sqrt, std, subtract
from numpy.random import randn

def hurst(ts):
    """
    Returns the Hurst Exponent of the time series vector ts
    """
    # Create the range of lag values
    lags = range(2, 100)

    # Compute the array of variances of the lagged differences
    tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags]

    # Use a numpy built-in linear fit to estimate the Hurst Exponent
    poly = polyfit(log(lags), log(tau), 1)

    # Return the calculated Hurst Exponent
    return poly[0]*2.0

# Test on the three clear categories: GBM, MR, and TR
def test():
    gbm = log(cumsum(randn(100000))+1000)
    mr = log(randn(100000)+1000)
    tr = log(cumsum(randn(100000)+1)+1000)

    print ("Hurst(GBM): %s" % hurst(gbm))
    print ("Hurst(MR): %s" % hurst(mr))
    print ("Hurst(TR): %s" % hurst(tr))

if __name__ == "__main__":
    # test()
    # Now calculate the Hurst Exponent from a time series of an actual ticker
    # TODO: accept dates as commandline input
    import sys
    import pandas.io.data as web
    from datetime import datetime as dt
    ticker = str(sys.argv[1])
    series = web.DataReader(ticker, "yahoo", dt(2000,1,1,), dt(2015,1,1))
    print("Hurst(%s): %s" % (ticker, hurst(series['Adj Close'])))



