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




