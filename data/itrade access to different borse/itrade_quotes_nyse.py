#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_nyse.py
#
# Description: List of quotes from http://www.nasdaq.com : NYSE, NASDAQ, AMEX
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
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
# 2005-06-12    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
#import re
#import thread
#import time
import urllib
import csv
#import string

# iTrade system
import itrade_config
from itrade_logging import *
#from itrade_isin import buildISIN,extractCUSIP,filterName
#from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_NASDAQ()
#
# ============================================================================

def Import_ListOfQuotes_NASDAQ(quotes,market='NASDAQ',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )
    if market=='NYSE':
        url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?letter=0&exchange=nyse&render=download'
    elif market == 'NASDAQ':
        url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?letter=0&exchange=nasdaq&render=download'
    elif market == 'AMEX':
        url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'
    else:
        return False


    try:
        data = urllib.urlopen(url)
        #data=connection.getDataFromUrl(url)
    except:
        debug('Import_ListOfQuotes_NASDAQ:unable to connect :-(')
        return False


    reader = csv.reader(data, delimiter=',')
    count = -1
    isin = ''
    
    # returns the data

    for line in reader:
        count = count+1
        if count >0:
            name = line[1]
            name = name.strip()
            name =name.replace(',','').replace('&quot;','"')
            ticker = line[0]
            ticker = ticker.strip()
            ticker = ticker.replace('/','-').replace('^','-P')
            quotes.addQuote(isin=isin,name=name,ticker=ticker,market=market,currency='USD',place='NYC',country='US')
    if itrade_config.verbose:
        print 'Imported %d lines from NASDAQ data.' % count

    return True

# ============================================================================
# Export me
# ============================================================================
registerListSymbolConnector('NASDAQ','NYC',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_NASDAQ)
registerListSymbolConnector('AMEX','NYC',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_NASDAQ)
registerListSymbolConnector('NYSE','NYC',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_NASDAQ)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_NASDAQ(quotes,'NASDAQ')
    Import_ListOfQuotes_NASDAQ(quotes,'NYSE')
    Import_ListOfQuotes_NASDAQ(quotes,'AMEX')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
