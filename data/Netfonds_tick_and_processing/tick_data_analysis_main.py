import datetime
import file_access as file_rw
import net_fonds as net_fds
import tick_data_analysis as tda
import time_dmy_state as dmy_state

import pandas as p

b_non_business_day = dmy_state.is_non_business_day()

symbol = 'iwm'
str_full_url_trade = dmy_state.get_trade_dump_url_five_conseq_days(symbol)
str_full_url_trade = dmy_state.get_trade_dump_url_current_day(symbol)
str_full_url_bid_ask = dmy_state.get_bid_ask_dump_url_five_conseq_days(symbol)
str_full_url_bid_ask = dmy_state.get_bid_ask_dump_url_current_day(symbol)

sym_bid_ask_p_Dataframe = p.DataFrame()
str_full_url_bid_ask_LtoStr = ''.join(str_full_url_bid_ask)
sym_bid_ask_p_Dataframe = net_fds.netfonds_p_bid_ask(str_full_url_bid_ask_LtoStr)

if ( (len(sym_bid_ask_p_Dataframe) == 0) | b_non_business_day ):
    str_bid_ask_filename = file_rw.get_current_day_bid_ask_filename(symbol)
    if b_non_business_day:
        str_bid_ask_filename = file_rw.get_last_business_day_bid_ask_filename(symbol)
    sym_posdump  = p.DataFrame()
    cols_posdump = [ 'bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total' ]
    try:
        #
        sym_posdump = sym_posdump.append( p.read_csv( str_bid_ask_filename, index_col=0, header=0, parse_dates=True ) ) 
    except Exception as e:
        print( "Error reading posdump file: {} ".format( str_bid_ask_filename ) )
   
    sym_posdump.columns = cols_posdump
    sym_bid_ask_p_Dataframe = sym_posdump

else:
    print ("Successful read of URL csv page: {}".format(str_full_url_bid_ask_LtoStr))


sym_bid_ask_p_Dataframe = tda.remove_spike_pre_open( sym_bid_ask_p_Dataframe )
open_index = tda.return_open_datetime_index( sym_bid_ask_p_Dataframe )
tda.gap_analysis_bid_ask_tick_data( sym_bid_ask_p_Dataframe, symbol )
    
#------------------------------------------------------------------------------
#    Trade tick data dump
#------------------------------------------------------------------------------
b_retrieve_analyze_trade_dump_data = False

    sym_trade_p_Dataframe = p.DataFrame()
    str_full_url_trade = dmy_state.get_trade_dump_url_current_day(symbol)
    str_full_url_trade_LtoStr = ''.join(str_full_url_trade)
    sym_trade_p_Dataframe = net_fds.netfonds_t_trade_dump(str_full_url_trade_LtoStr)

    if ( (len(sym_trade_p_Dataframe) == 0) | b_non_business_day ):
        str_trade_filename = file_rw.get_current_day_trade_filename(symbol)
        if b_non_business_day:
            str_trade_filename = file_rw.get_last_business_day_trade_filename(symbol)
    
        sym_posdump  = p.DataFrame()
        cols_posdump = [ 'price', 'quantity', 'source', 'offer', 'buyer', 'initiator' ]
        try:
            sym_posdump = sym_posdump.append( p.read_csv( str_trade_filename, index_col=0, header=0, parse_dates=True ) ) 
        except Exception as e:
            print( "Error reading trade dump file: {} ".format( str_trade_filename ) )
   
        print ("Successful read of data file: {}".format(str_trade_filename) )
        print len(sym_posdump)
        sym_posdump.columns = cols_posdump
        sym_posdump[['price', 'quantity']].plot(subplots=True)
    else:
        print ("Successful read of URL csv page: {}".format(str_full_url_trade_LtoStr))
        sym_trade_p_Dataframe[['price', 'quantity']].plot(subplots=True)

    
    
    
