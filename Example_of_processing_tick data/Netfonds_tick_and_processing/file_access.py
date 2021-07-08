
import os
import time_dmy_state as dmy_state
import webbrowser

m_netfonds_dir = '\\netfonds_data'
m_current_wd = os.getcwd()
m_str_netfonds_data_directory = 'C:\\users\jjplombo\\Documents\\netfonds_data'

def get_net_fonds_working_dir():
    str_net_fonds_working_dir = m_str_netfonds_data_directory
    return str_net_fonds_working_dir

symbol = 'IWM'
bid_offer_file = dmy_state.get_current_day_file_bid_ask(symbol)

def get_current_day_bid_ask_filename (symbol):
    bid_ask_filename = dmy_state.get_current_day_file_bid_ask(symbol)
    bid_ask_filename_dir = get_net_fonds_working_dir()
    bid_ask_filename_path = bid_ask_filename_dir + '\\' + bid_ask_filename
    bIsFileValid = os.path.isfile(bid_ask_filename_path)
    return bid_ask_filename_path
    
def get_last_business_day_bid_ask_filename(symbol):
    bid_ask_filename = dmy_state.get_last_business_day_file_bid_ask(symbol)
    bid_ask_filename_dir = get_net_fonds_working_dir()
    bid_ask_filename_path = bid_ask_filename_dir + '\\' + bid_ask_filename
    bIsFileValid = os.path.isfile(bid_ask_filename_path)
    return bid_ask_filename_path
    
def get_2nd_to_last_business_day_bid_ask_filename(symbol):
    bid_ask_filename = dmy_state.get_2nd_to_last_business_day_file_bid_ask(symbol)
    bid_ask_filename_dir = get_net_fonds_working_dir()
    bid_ask_filename_path = bid_ask_filename_dir + '\\' + bid_ask_filename
    bIsFileValid = os.path.isfile(bid_ask_filename_path)
    return bid_ask_filename_path
    
def get_current_day_trade_filename (symbol):
    trade_filename = dmy_state.get_current_day_file_trade(symbol)
    trade_filename_dir = get_net_fonds_working_dir()
    trade_filename_path = trade_filename_dir + '\\' + trade_filename
    bIsFileValid = os.path.isfile(trade_filename_path)
    return trade_filename_path
    
def get_last_business_day_trade_filename( symbol ):
    trade_filename = dmy_state.get_last_business_day_file_trade(symbol)
    trade_filename_dir = get_net_fonds_working_dir()
    trade_filename_path = trade_filename_dir + '\\' + trade_filename
    bIsFileValid = os.path.isfile(trade_filename_path)
    return trade_filename_path
    
   
