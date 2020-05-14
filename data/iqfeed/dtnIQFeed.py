#!/usr/bin/python
# -*- coding: utf-8 -*-

#==========================================================
#===================== dtnIQFeed.py =======================
#==========================================================

# Purpose
#----------------------------------------------------------
# NOTE: EITHER MUST RUN ON WINDOWS OR THROUGH WINE AND ALSO HAVE A DTN IQFEED
# ACCOUNT PURCHASED
# 
# Connect with DTN IQFeeds "miniserver" set up to retrieve SPY and IWM ETGs 
# intraday data (minutely) for Jan 1st 2007 to now, leveraging sockets.



'''
DTN IQ feed is a high-end data feed provider for retail algorithmic traders.
Because of it's attractive price per data availability (relatively cheap 
intraday data) and accuracy it should be considered/ explored at some point in 
time by those developing their automated data market data retrieval stack.

Two main points should be noted:
    - the cost claims to start around 50$ but in reality ends up in the 
      150-200$
      range for all of the features and services that most subscriptions
      would need

    - The API can only be accessed through either a native Windows OS or by
      running through the WINE emulator on Linux or Mac. This is a
      requirement and may require some additional upkeep in the future.
'''


import sys
import socket

def read_historical_data_socket(sock, recv_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.
   
   Parameters:
   sock - The socket object
   recv_buffer - Amount in bytes to receive per read
   """
   buffer = ""
   data = ""
   while True:
       data = sock.recv(recv_buffer)
       buffer += data
       # Check if the end message string arrives
       if "!ENDMSG!" in buffer:
           break
       # Remove the end message string
       buffer = buffer[:-12]
       return buffer
if __name__ == "__main__":
    # Define server host, port and symbols to download
    host = "127.0.0.1" # Localhost
    port = 9100 # Historical data socket port
    syms = ["SPY", "IWM"]
    # Download each symbol to disk
    for sym in syms:
        print "Downloading symbol: %s..." % sym
        # Construct the message needed by IQFeed to retrieve data
        message = "HIT,%s,60,20070101 075000,,,093000,160000,1\n" % sym
        # Open a streaming socket to the IQFeed server locally
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        # Send the historical data request
        # message and buffer the data
        sock.sendall(message)74
        data = read_historical_data_socket(sock)
        sock.close
        # Remove all the endlines and line-ending
        # comma delimiter from each record
        data = "".join(data.split("\r"))
        data = data.replace(",\n","\n")[:-1]
        # Write the data stream to disk
        f = open("%s.csv" % sym, "w")
        f.write(data)
        f.close()
