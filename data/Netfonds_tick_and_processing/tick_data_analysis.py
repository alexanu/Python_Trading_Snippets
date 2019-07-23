import pandas as p

import matplotlib.pyplot as plt
from matplotlib import style
from matplotlib .finance import candlestick_ohlc
import matplotlib.dates as mdates

import datetime
import file_access as file_rw
import time_dmy_state as dmy_state

data_frame = p.DataFrame
data_frame = []

df_tick_data = p.DataFrame
df_tick_data = []

dt_tick_data_now = datetime.datetime.now()

def remove_spike_pre_open( df_tick_data ):
    
    #df_tick_data.iloc[106,0] = 159.49
    #df_tick_data.iloc[107,0] = 159.49
    #  find pre-open start, with gap after previous business day after market: T100001
    #  typically T100001 spikes low with T100003 and T100005 possibly low as well...
    index_pre_market_start = m_find_pre_market_start(df_tick_data)
    
    if index_pre_market_start == -1:
        return df_tick_data
    bid_three_ticks_away = df_tick_data['bid'][(index_pre_market_start+3)] # Get 'bid' three ticks ahead of pre market open
    #  Replace prior three bids; 'bid' is column o
    df_tick_data.iloc[index_pre_market_start,0] = bid_three_ticks_away
    df_tick_data.iloc[(index_pre_market_start+1),0] = bid_three_ticks_away
    df_tick_data.iloc[(index_pre_market_start+2),0] = bid_three_ticks_away
    
    return df_tick_data

def return_open_datetime_index( df_tick_data ):
    open_index = -1
    dt_from_pts_idx_0 = df_tick_data.index[0].to_datetime()    
    market_open_dt = m_construct_market_open_datetime_from_now(dt_from_pts_idx_0)
    
    for i in range(len(df_tick_data.index)):
        dt_from_pts = df_tick_data.index[i].to_datetime()
        if dt_from_pts.hour == m_open_hour:
            if dt_from_pts.minute == m_open_minute:
                if(open_index == -1):
                    open_index = i
                    m_market_open_index = i
    
    return open_index


def gap_analysis_bid_ask_tick_data( df_tick_data, symbol ):
    #  Read previous close from prior business data file (local file)
    #  YYYY-MM_DDT220000 => hour = 22, minute = 00, second = 00
    #  df_tick_data['bid'][len(df_tick_data.index)] == closing price
    if dmy_state.is_non_business_day() :
        file_name_bid_ask = file_rw.get_2nd_to_last_business_day_bid_ask_filename(symbol)
    else:
        file_name_bid_ask = file_rw.get_last_business_day_bid_ask_filename(symbol)
    
    sym_posdump  = p.DataFrame()
    cols_posdump = [ 'bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total' ]
    try:
        #
        sym_posdump = sym_posdump.append( p.read_csv( file_name_bid_ask, index_col=0, header=0, parse_dates=True ) ) 
    except Exception as e:
        print( "Error reading posdump file: {} ".format( file_name_bid_ask ) )
   
    print ("Successful read of previous business day data file: {}".format(file_name_bid_ask) )
    len_sym_posdump = len(sym_posdump)
    print len_sym_posdump
    
    prev_close = sym_posdump['bid'][(len_sym_posdump-1)]
    print ('previous business day closing price: {}'.format(prev_close) )
    
    #  Current business day open:
    #  df_tick_data
    m_market_open_index = return_open_datetime_index( df_tick_data )
    if m_market_open_index == -1:
        print 'Still in Pre-market, market is not open, current open is last print...'
    
    current_open = df_tick_data['bid'][m_market_open_index]
    gap_open = current_open - prev_close
    print ('Open: {}, Previous Close: {}, Gap = {}'
           .format( current_open, prev_close, gap_open ) )
    
    #  Pre-market Gaps:
    post_market_open = df_tick_data['bid'][m_post_market_open_index]
    m_pre_market_open_index = m_find_pre_market_start(df_tick_data)
    pre_market_open = df_tick_data['bid'][m_pre_market_open_index]
    pre_maket_gap = pre_market_open - post_market_open
    print ('Post Market Open: {}, Pre Market Open: {}, Market Open: {}, Pre Market Gap: {}'
           .format( post_market_open, pre_market_open, current_open, pre_maket_gap ))
    
    return

def resample_5min_ohlc(data_frame):
    df_resample = data_frame.resample( '5min').ohlc()
    
    return df_resample
    
#------------------------------------------------------------------------------
#
#    Private data and methods
#
#------------------------------------------------------------------------------
    
def m_construct_market_open_datetime_from_tick_dump( df_tick_data ):
    #pts_index_0 = df_tick_data.index[0]
    #dt_pts_index_0 = pts_index_0.to_datetime()
    #m_year = dt_pts_index_0.year
    #m_month = dt_pts_index_0.month
    #m_day = dt_pts_index_0.day
    market_open_datetime = datetime.datetime(m_year, m_month, m_day, m_open_hour, m_open_minute, m_open_second)
    print (market_open_datetime)
    
    
    return market_open_datetime
    
def m_construct_market_open_datetime_from_now(dt_tick_data_now):
    dt_now = dt_tick_data_now
    m_year = dt_now.year
    m_month = dt_now.month
    m_day = dt_now.day
    market_open_datetime = datetime.datetime(m_year, m_month, m_day, m_open_hour, m_open_minute, m_open_second)
    print (market_open_datetime)
    
    #delta_time = market_open_datetime - market_open_datetime
   # print (delta_time)
    
    
    return market_open_datetime
    
    
def m_find_pre_market_start( df_tick_data ):
    index_pre_mark_start = -1
    
    for i in range(len(df_tick_data.index)):
        dt_from_pts = df_tick_data.index[i].to_datetime()
        if dt_from_pts.hour == m_pre_market_start_hour:
            if dt_from_pts.minute == m_pre_market_start_minute:
                #print(dt_from_pts)
                if(index_pre_mark_start == -1):
                    index_pre_mark_start = i
                    m_pre_market_open_index = i
                    print 'found pre-market start time'
                    print(dt_from_pts)
    
    return index_pre_mark_start

m_open_hour = 15
m_open_minute = 30
m_open_second = 0
m_open_microsecond = 0

m_pre_market_start_hour = 10
m_pre_market_start_minute = 0
m_pre_market_start_second = 1
m_year = 2018
m_month = 1
m_day = 1
m_construct_market_open_datetime_from_tick_dump( df_tick_data )
m_construct_market_open_datetime_from_now( dt_tick_data_now )

m_market_open_index = -1
m_pre_market_open_index = -1
m_post_market_open_index = 0
    

    
