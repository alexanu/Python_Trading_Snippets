
# -------------------------------------------------------------------------------------------------------------

day = re.compile('[1-3][0-9]?') # day regex
year = re.compile('20[0-9]{2}') # year regex
    
tmp = self.soup.find('small', text=re.compile('market', re.IGNORECASE)).text.split('Market')[0].strip()
self.year = year.search(tmp).group(0)
self.day = day.search(tmp).group(0)
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
for ii, mo in enumerate(months, 1): # iterate over months and flag if match found
    more = re.compile(mo, re.IGNORECASE)
    if more.search(tmp):
        self.month = ii
        break


# -------------------------------------------------------------------------------------------------------------

# http://dateutil.readthedocs.io/en/stable/rrule.html
from dateutil.rrule import DAILY, rrule, MO, TU, WE, TH, FR
def daterange(start_date, end_date):
    # automate a range of business days between two dates
    return rrule(DAILY, dtstart=start_date, until=end_date, byweekday=(MO,TU,WE,TH,FR))

for tr_date in daterange(start_date, end_date):

# -------------------------------------------------------------------------------------------------------------    

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

# We want to transform the value to a valid date. 
# value "09/2007" to date 2007-09-01. 
# value "2006" to date 2016-01-01.

import datetime

def parse_thisdate(text: str) -> datetime.date:
    parts = text.split('/')
    if len(parts) == 2:
        return datetime.date(int(parts[1]), int(parts[0]), 1)
    elif len(parts) == 1:
        return datetime.date(int(parts[0]), 1, 1)
    else:
        assert False, 'Unknown date format'

# -------------------------------------------------------------------------------------------------------------

# https://github.com/alexanu/market_calendars
# Chinese and US trading calendars with date math utilities 
# based on pandas_market_calendar
# Speed is achieved via Cython

import sys
sys.path.append("../") 
import market_calendars as mcal
from market_calendars.core import Date, Period, Calendar, Schedule

current_date = Date(2015, 7, 24) # create date object
two_days_later = current_date + 2 # => Date(2015, 7, 26)
str(two_days_later) # => '2015-07-26'
current_date + '1M' # => Date(2015, 8, 24)
current_date + Period('1M') # same with previous line # => Date(2015, 8, 24)

current_date.strftime("%Y%m%d") # => '20150724'
Date.strptime('20160115', '%Y%m%d') # => Date(2016, 1, 15)
Date.strptime('2016-01-15', '%Y-%m-%d') # => Date(2016, 1, 15)
import datetime as dt
Date.from_datetime(dt.datetime(2015, 7, 24)) # => Date(2015, 7, 24)



cal_sse = mcal.get_calendar('China.SSE') # create chinese shanghai stock exchange calendar
cal_nyse = mcal.get_calendar('NYSE') # create nyse calendar
cal_sse.name, cal_sse.tz # return name and time zone => ('China.SSE', <DstTzInfo 'Asia/Shanghai' LMT+8:06:00 STD>)

cal_sse.holidays('2018-09-20', '2018-10-10') # return holidays in datetime format
cal_sse.holidays('2018-09-20', '2018-10-10', return_string=True) # return holidays in string format
cal_sse.holidays('2018-09-20', '2018-10-10', return_string=True, include_weekends=False) # return holidays excluding weekends
cal_sse.biz_days('2015-05-20', '2015-06-01') # return biz days in datetime format



cal_sse.is_biz_day('2014-09-22'), cal_sse.is_biz_day('20140130') # => (True, True)
cal_sse.is_holiday('2016-10-01'), cal_sse.is_holiday('2014/9/21') # => (True, True)
cal_sse.is_weekend('2014-01-25'), cal_sse.is_weekend('2011/12/31') # => (True, True)
cal_sse.is_end_of_month('2011-12-30'), cal_sse.is_end_of_month('20120131') # => (True, True)


cal_sse.adjust_date('20130131') # => datetime.datetime(2013, 1, 31, 0, 0)
cal_sse.adjust_date('20130131', return_string=True) # => '2013-01-31'
cal_sse.adjust_date('2017/10/01') # => datetime.datetime(2017, 10, 9, 0, 0)
cal_sse.adjust_date('2017/10/01', convention=2) # => datetime.datetime(2017, 9, 29, 0, 0)

cal_sse.advance_date('2017-04-27', '2b') # => datetime.datetime(2017, 5, 2, 0, 0)
cal_sse.advance_date('20170427', '2b', return_string=True) # => '2017-05-02'
cal_sse.advance_date('20170427', '1w', return_string=True) # => '2017-05-04'
cal_sse.advance_date('20170427', '1m', return_string=True) # => '2017-05-31'
cal_sse.advance_date('20170427', '-1m', return_string=True) # => '2017-03-27'

cal_sse.schedule('2018-01-05', '2018-02-01', '1w', return_string=True, date_generation_rule=2) # => ['2018-01-05', '2018-01-12', '2018-01-19', '2018-01-26', '2018-02-01']









# -------------------------------------------------------------------------------------------------------------

