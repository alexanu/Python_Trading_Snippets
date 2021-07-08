# Based on : qstrader/blob/master/qstrader/scripts/generate_simulated_prices.py

import calendar
import copy
import datetime
import os
import numpy as np


def month_weekdays(year_int, month_int):
    """
    Produces a list of datetime.date objects representing the
    weekdays in a particular month, given a year.
    """
    cal = calendar.Calendar()
    return [
        d for d in cal.itermonthdates(year_int, month_int)
        if d.weekday() < 5 and d.year == year_int
    ]


ticker='GOOG'
init_price=700
seed=42 # Fix the randomness by default but use a negative value for true randomness
s0=1.5000
year=2017
month=1
days=-1 # Number days to process




def run(outdir, ticker, init_price, seed, s0, spread, mu_dt, sigma_dt, year, month, nb_days, config):
    if seed >= 0:
        np.random.seed(seed)

    if config is None:
        config = settings.DEFAULT

    if outdir == '':
        outdir = os.path.expanduser(config.CSV_DATA_DIR)
    else:
        outdir = os.path.expanduser(outdir)

    s0 = float(init_price)
    spread = 0.02
    mu_dt = 1400  # Milliseconds
    sigma_dt = 100  # Millseconds
    ask = copy.deepcopy(s0) + spread / 2.0
    bid = copy.deepcopy(s0) - spread / 2.0
    days = month_weekdays(year, month)
    current_time = datetime.datetime(
        days[0].year, days[0].month, days[0].day, 0, 0, 0,
    )

    # Loop over every day in the month and create a CSV file
    # for each day, e.g. "GOOG_20150101.csv"
    for i, d in enumerate(days):
        print("Create '%s' data for %s" % (ticker, d))
        current_time = current_time.replace(day=d.day)
        fname = os.path.join(
            outdir,
            "%s_%s.csv" % (ticker, d.strftime("%Y%m%d"))
        )
        outfile = open(fname, "w")
        print("Save data to '%s'" % fname)
        outfile.write("Ticker,Time,Bid,Ask\n")

        # Create the random walk for the bid/ask prices
        # with fixed spread between them
        # for i in range(0, 10000):
        while True:
            dt = abs(np.random.normal(mu_dt, sigma_dt))
            current_time += datetime.timedelta(0, 0, 0, dt)
            if current_time.day != d.day:
                outfile.close()
                break
            else:
                W = np.random.standard_normal() * dt / 1000.0 / 86400.0
                ask += W
                bid -= W
                line = "%s,%s,%s,%s\n" % (
                    ticker,
                    current_time.strftime(
                        "%d.%m.%Y %H:%M:%S.%f"
                    )[:-3],
                    "%0.5f" % bid,
                    "%0.5f" % ask
                )
                outfile.write(line)
        if nb_days > 0 and i >= nb_days - 1:
            break
