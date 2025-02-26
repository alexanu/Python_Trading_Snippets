import pandas as pd
import numpy as np
import pandas_market_calendars as mcal


def trading_holidays_in_range(start, end):
    """Returns a list of dates where CME holidays fall on a weekday
    Allow certain days, like 2018 National Day of Mourning to trade
    Holiday schedule: https://www.cmegroup.com/tools-information/holiday-calendar.html
    Args:
        start (str): YYYY-MM-DD
        end (str): YYYY-MM-DD
    Returns:
        list: list of dates in YYYY-MM-DD corresponding to holidays
    """

    # Add a week of buffer to catch any holidays upcoming
    end_with_buffer = (pd.to_datetime(end) + pd.Timedelta("7 days")).strftime(
        "%Y-%m-%d"
    )

    ALLOWED_HOLIDAYS_FOR_TRADING = [
        "2018-12-05",  # National Day of Mourning
    ]

    INVALID_HOLIDAYS_FOR_TRADING = []

    # get valid trading days per the module
    all_weekdays = pd.date_range(start, end_with_buffer, freq="B")
    cme = mcal.get_calendar("CME")
    schedule = cme.schedule(start_date=all_weekdays[0], end_date=all_weekdays[-1])
    valid_trading_days = pd.to_datetime(schedule["market_close"].dt.date)

    # remove invalid days (corrections to the module)
    for day in INVALID_HOLIDAYS_FOR_TRADING:
        try:
            valid_trading_days.drop(pd.to_datetime(day), inplace=True)
        except KeyError:
            pass

    # Create holidays
    holidays = np.setdiff1d(all_weekdays, valid_trading_days)
    holidays = [pd.to_datetime(h).strftime("%Y-%m-%d") for h in holidays]

    # add early closes to holidays, which is true except for Christmas Eve and Thanksgiving Eve
    early_closes = [
        x.strftime("%Y-%m-%d")
        for x in cme.early_closes(schedule=schedule).index.to_list()
    ]
    # Allow some trading on the day before or after Thanksgiving (i.e. if
    # it is in November and not Thursday)
    for date in early_closes:
        year, month, day = date.split("-")
        if month == "11":
            if pd.to_datetime(date).day_name() != "Thursday":
                early_closes.remove(date)
    for date in early_closes:
        year, month, day = date.split("-")
        if month == "12" and day == "24":
            early_closes.remove(date)

    # combine them
    holidays.extend(early_closes)
    holidays.sort()

    # Allow some trading on Christmas Eve
    dates_to_remove = []
    for holiday in holidays:
        year, month, day = holiday.split("-")
        if month == "12" and day == "24":
            logger.debug(f"found {holiday}")
            dates_to_remove.append(date)
    for date in dates_to_remove:
        try:
            holidays.remove(date)
        except ValueError:
            pass

    # add missing holidays (corrections to the module)
    for day in ALLOWED_HOLIDAYS_FOR_TRADING:
        try:
            holidays.remove(day)
        except ValueError:
            pass

    return holidays


def tradingday_offset(start, end, **kwargs):
    """Uses a trading day offset with CME calendar instead of business day offset
    Args:
        start (str): YYYY-MM-DD
        end (str): YYYY-MM-DD
        **kwargs: passed to np.busday_offset, requires "dates" "offsets" and optional is "roll"
    Returns:
        list of pd.Timestamps: dates YYYY-MM-DD timestamps
    """
    holidays = trading_holidays_in_range(start, end)
    offsets = np.busday_offset(
        **kwargs, holidays=holidays, weekmask="Mon Tue Wed Thu Fri"
    )
    offsets = pd.to_datetime(offsets)
    return offsets