#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_nse.py
#
# Description: List of quotes from www.nse-india.com/: NATIONAL STOCK ECHANGE
#              OF INDIA
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is from Gilles Dumortier.
# New code for NSE is from Michel Legrand.
#
# Portions created by the Initial Developer are Copyright (C) 2004-2008 the
# Initial Developer. All Rights Reserved.
#
# Contributor(s):
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see http://www.gnu.org/licenses/gpl.html
#
# History       Rev   Description
# 2007-04-14    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import thread
import time
import string
import httplib

# iTrade system
import itrade_config
import itrade_csv
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_nse()
#
# ============================================================================

def Import_ListOfQuotes_NSE(quotes,market='NATIONAL EXCHANGE OF INDIA',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )

    if market=='NATIONAL EXCHANGE OF INDIA':
        
        url = "http://www.nseindia.com/content/equities/EQUITY_L.csv"
    else:
        return False

    def splitLines(buf):
        lines = string.split(buf, '\n')
        lines = filter(lambda x:x, lines)
        def removeCarriage(s):
            if s[-1]=='\r':
                return s[:-1]
            else:
                return s
        lines = [removeCarriage(l) for l in lines]
        return lines

    info('Import_ListOfQuotes_NSE:connect to %s' % url)

    url = 'http://www.nseindia.com/content/equities/EQUITY_L.csv'

    host = 'www.nseindia.com'

    headers = { "Host": host
                , "User-Agent": " Mozilla/5.0 (Windows; U; Windows NT 5.1; fr; rv:1.9.2.12) Gecko/20101026 Firefox/3.6.12"
                , "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                , "Accept-Language": " fr,fr-fr;q=0.8,en-us;q=0.5,en;q=0.3"
                , "Accept-Encoding": "gzip,deflate"
                , "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7"
                , "Keep-Alive":115
                , "Connection": "keep-alive"
                }
    
    try:
        conn = httplib.HTTPConnection(host,80)
        conn.request("GET",url,None,headers)
        response = conn.getresponse()
    except:
        debug('Import_ListOfQuotes_NSE unable to connect :-(')
        return False

    debug("status:%s reason:%s" %(response.status, response.reason))
    if response.status != 200:
        debug('Import_ListOfQuotes_NSE:status!=200')
        return False
    
    data =response.read()

    # returns the data
    lines = splitLines(data)
    
    response.close()
    
    nlines = 0

    for line in lines[1:]:
        data = string.split (line, ',')
        if len(data) == 8:
            name = data[1]
            name = name.replace('Limited','LTD')
            ticker = data[0]
            if len(ticker)>9:
                ticker = ticker[:9]
            isin = data[6]
            if isin != 'INE195A01028':
                quotes.addQuote(isin=isin,name=name,ticker=ticker,
                market='NATIONAL EXCHANGE OF INDIA',currency='INR',place='NSE',country='IN')
                nlines = nlines + 1
    if itrade_config.verbose:
        print 'Imported %d lines from %s data.' % (nlines,market)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('NATIONAL EXCHANGE OF INDIA','NSE',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_NSE)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_NSE(quotes,'NSE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
