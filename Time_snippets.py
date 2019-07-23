import datetime

def utc_to_local(self, utc_dt):
    utc_dt = datetime.strptime(utc_dt, "%Y-%m-%d %H:%M:%S")
    local = utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return local.strftime("%Y-%m-%d %H:%M:%S")

def utc_to_local(utc_dt):
    local = utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return local.strftime("%Y-%m-%d %H:%M:%S")

utc_dt = datetime.strptime("2018-11-01 01:45:00", "%Y-%m-%d %H:%M:%S")
print(utc_to_local(utc_dt))


def days_ago(d, start_date=None):
    """
    get the date string for d days ago
    """
    #today is August 13, 10 days ago means August 3
    if start_date==None:
        date = datetime.datetime.today() - datetime.timedelta(days=d)
    else:
        date = str_to_date(start_date) - datetime.timedelta(days=d)
    return date.strftime("%Y-%m-%d")

def str_to_date(dt):
    """
    convert date string to datetime.date object
    """
    year, month, day = (int(x) for x in dt.split('-'))    
    return datetime.date(year, month, day)


# -------------------------------------------------------------------------------------------------------------
start_trading_day = datetime.date(2017, 6, 8)
end_trading_day = datetime.date(2017, 6, 20) #end_trading_day包括在所有交易日期内
trading_days = []
while start_trading_day <= end_trading_day:
    trading_days.append(start_trading_day)
    start_trading_day += datetime.timedelta(days=1)

for trading_d in trading_days:
    print(u"calculate volatility in date:%r" % trading_d)

    #构建查询条件
    start_datetime1 = datetime.datetime(trading_d.year, trading_d.month, trading_d.day, 9, 0, 0)
    end_datetime1 = datetime.datetime(trading_d.year, trading_d.month, trading_d.day, 10, 15, 0)
    start_datetime2 = datetime.datetime(trading_d.year, trading_d.month, trading_d.day, 10, 30, 0)
    end_datetime2 = datetime.datetime(trading_d.year, trading_d.month, trading_d.day, 11, 30, 0)
    start_datetime3 = datetime.datetime(trading_d.year, trading_d.month, trading_d.day, 13, 30, 0)
    end_datetime3 = datetime.datetime(trading_d.year, trading_d.month, trading_d.day, 15, 0, 0)
    time_query = {
        '$or':[{'snapshot_time':{'$gte':start_datetime1, '$lte':end_datetime1}},
        {'snapshot_time':{'$gte':start_datetime2, '$lte':end_datetime2}},
        {'snapshot_time':{'$gte':start_datetime3, '$lte':end_datetime3}}
        ]}


# -------------------------------------------------------------------------------------------------------------
now   = dt.date.today()
year  = str(now.year)
m     = str(now.month)
month = '0'+m

day_5 = now - 5 * BDay()
day_4 = now - 4 * BDay()
day_3 = now - 3 * BDay()
day_2 = now - 2 * BDay()
day_1 = now - 1 * BDay()
# Add current day_0
day_0 = now - 0 * BDay()

days  = [ day_5.day, day_4.day, day_3.day, day_2.day, day_1.day, day_0.day ]
months = [ day_5.month, day_4.month, day_3.month, day_2.month, day_1.month, day_0.month ]
years = [ day_5.year, day_4.year, day_3.year, day_2.year, day_1.year, day_0.year ]
days  = [ str(d) for d in days ]
months  = [ str(ms) for ms in months ]
years  = [ str(ys) for ys in years ]

    #for day in days:
    for (day, month, year) in itertools.izip(days, months, years):
        try:

# -------------------------------------------------------------------------------------------------------------


def trading_start(d):
    mkt_open = dt.datetime( int(year), int(month), int(d), 9, 30 )
    return mkt_open

def trading_end(d):
    mkt_close = dt.datetime( int(year), int(month), int(d), 16, 00 )
    return mkt_close

def trading_hours(data):
    test = []
    for d in days:
        dat = data[ ( data.index > trading_start(d) ) & ( data.index < trading_end(d) ) ]
        test.append( dat )
    return test

# -------------------------------------------------------------------------------------------------------------

def closest_business_day_in_past(date=None):
    if date is None:
        date = dt.datetime.today()
    return date + BDay(1) - BDay(1)

# -------------------------------------------------------------------------------------------------------------
    def prepare_date_strings(date):

        date_yr = date.year.__str__()
        date_mth = date.month.__str__()
        date_day = date.day.__str__()

        if len(date_mth) == 1: date_mth = '0' + date_mth
        if len(date_day) == 1: date_day = '0' + date_day
        
        return date_yr, date_mth, date_day


# -------------------------------------------------------------------------------------------------------------



# -------------------------------------------------------------------------------------------------------------



# -------------------------------------------------------------------------------------------------------------

