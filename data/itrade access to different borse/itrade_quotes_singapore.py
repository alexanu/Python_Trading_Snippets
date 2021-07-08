#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_singapore.py
#
# Description: List of quotes from http://www.sgx.com/
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is from Gilles Dumortier.
# New code for SGX is from Michel Legrand.

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
# 2007-05-15    dgil  Wrote it from scratch
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

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_SGX()
#
# ============================================================================

def Import_ListOfQuotes_SGX(quotes,market='SINGAPORE EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)

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
    
    # find date to update list
  
    try:
        data = connection.getDataFromUrl('http://info.sgx.com/webstocks.nsf/isincodedownload/')
    except:
        info('Import_ListOfQuotes_SGX_%s:unable to get file name :-(' % market)
        return False


    date = data[data.find('/ISINCODEDOWNLOAD/')+18:data.find('/$File/ISINCODE.txt')]
    url = 'http://info.sgx.com/webstocks.nsf/ISINCODEDOWNLOAD/'+date+'/%24File/ISINCODE.txt'
    #info('Import_ListOfQuotes_SGX_%s:connect to %s' % (market,url))

    if market == 'SINGAPORE EXCHANGE':
        currency = 'SGD'
        place = 'SGX'
        country = 'SG'
    else:
        return False

    try:
        data = connection.getDataFromUrl(url)
    except:
        info('Import_ListOfQuotes_SGX_%s:unable to connect :-(' % market)
        return False

    # returns the data
    
    lines = splitLines(data)

    n = 0

    for line in lines[1:]:
        n = n + 1
        name = line[:50]
        isin = line[60:72]
        ticker = line[80:89]
        
        name = name.strip()
        isin = isin.strip()
        ticker = ticker.strip()


        quotes.addQuote(isin = isin,name = name,ticker = ticker,market = market,currency = currency,place = place,country = country)
    if itrade_config.verbose:
        print 'Imported %d lines from %s' % (n,market)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('SINGAPORE EXCHANGE','SGX',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_SGX)
    
# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_SGX(quotes,'SINGAPORE EXCHANGE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
