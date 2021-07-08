#Authentication
from upstox_api.api import *
import pandas as pd
import numpy as np
import datetime
import time
import csv
api_key=open('api_key.txt','r').read()
access_token=open('access_token.txt','r').read().strip()
u=Upstox(api_key,access_token)
#Scrips Details
u.get_master_contract('MCX_FO')
exchange='MCX_FO'
crude_dec_contract=211160
crude_dec=u.get_instrument_by_token(exchange,crude_dec_contract)
date = '13/12/2018'

long = 0
short = 0
 
def event_handler_quote_update(message):
    global long, short 
    time.sleep(302)
    DEC= pd.DataFrame(u.get_ohlc(u.get_instrument_by_token('MCX_FO', 211160), OHLCInterval.Minute_5, datetime.datetime.strptime('{}'.format(date), '%d/%m/%Y').date(), datetime.datetime.strptime('{}'.format(date), '%d/%m/%Y').date()))
    
    if len(DEC['close']) >= 5:
        ub = DEC['high'].rolling(5).mean()
        lb = DEC['low'].rolling(5).mean()
        price = DEC['close'].iloc[-1]
        print ('Upper Band ',ub.iloc[-1])
        print ('Lower Band ',lb.iloc[-1])
        print ('Price ', price )
        #Normal Short
        if (price >= ub.iloc[-1] and short == 0):
            place_order(book(),'Buy',price)
            long = 0
            short = 1
            
        #normal Long
        elif (price <= lb.iloc[-1] and long == 0):
            place_order(book(),'Sell',price)
            long = 1
            short = 0
        #Cover Order of Short Stoploss
        elif (short == 3 and price <= lb.iloc[-1]):
            place_order(100,'Sell - Exit Long Stoploss',price)
            place_order(100,'Buy - Cont.',price)
            print('Sell 1 lot and Buy 1 lot')
            short = 0
            long = 1
        elif (long == 3 and price >= ub.iloc[-1]):
            place_order(100,'Buy - Exit Short Stoploss',price)
            place_order(100,'Sell - Cont.',price)
            print('Buy 1 lot and Sell 1 lot')
            short = 1
            long = 0
            
    def check(long,short):
        
        if (long == 1):
            if ((ub.iloc[-1] - price) < -5):
                print ('Stoploss Hit in Long Position')
                place_order(book(),'Sell - Long Stoploss',price)
                long = 3
                short = 2
                print ('Sell 2 lots')
            
        elif (short == 1):
            if ((price - lb.iloc[-1]) < -5):
                print ('Stoploss Hit in Short Position')
                place_order(book(),'Buy - Short Stoploss',price)
                long = 2
                short = 3
                print ('Buy 2 lots')
    check(long,short)
          
            
def book():
    if (long == 0 and short == 0):
        Oty = 100
    else:
        Oty = 200
    return Oty

def place_order(Qty, Type, Price):
    with open('order_book.csv','a') as csvfile:
        wr=csv.writer(csvfile)
        wr.writerow([Type])
        wr.writerow([Price])
        wr.writerow([Qty])

def event_handler_socket_disconnect(err):
    print ("SOCKET DISCONNECTED" + str(datetime.datetime.now()))
    print (err)
    u.start_websocket(False)

 
#Listener        
def socket_connect():
    print ("Adding Socket Listeners")
    u.set_on_quote_update(event_handler_quote_update)
    u.set_on_disconnect(event_handler_socket_disconnect) 
    u.unsubscribe(crude_dec, LiveFeedType.Full)
    u.subscribe(crude_dec, LiveFeedType.Full)
    print ("Connecting to Socket")
    u.start_websocket(False)


socket_connect()
