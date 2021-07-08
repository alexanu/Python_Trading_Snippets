#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_asx.py
#
# Description: List of quotes from http://www.asx.com.au
# ASX - Australian Stock Exchange
#
# Developed for iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is	from Gilles Dumortier.
# New code for ASX is from Peter Mills and others.
#
# Portions created by the Initial Developer are Copyright (C) 2006-2008 the
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
# 2006-12-30    PeterMills  Initial version
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
# Import_ListOfQuotes_ASX()
#
# ============================================================================

def Import_ListOfQuotes_ASX(quotes,market='ASX',dlg=None,x=0):
    print 'Update %s list of symbols' % market
    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )

    if market=='ASX':
        url = "http://www.asx.com.au/asx/research/ASXListedCompanies.csv"
        n = 0
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

    try:
        data=connection.getDataFromUrl(url)
    except:
        debug('Import_ListOfQuotes_ASX:unable to connect :-(')
        return False

    # returns the data
    lines = splitLines(data)
    isin = ''
    for line in lines[3:]:
        line = line.replace('"','')
        data = string.split (line, ',')
        name=data[0]
        ticker=data[1]
        quotes.addQuote(isin=isin, name=name,
                ticker=ticker, market='ASX', currency='AUD', place='SYD', country='AU')

        n = n + 1

    if itrade_config.verbose:
        print 'Imported %d lines from %s data.' % (n,market)

    return True
# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('ASX','SYD',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_ASX)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    from itrade_quotes import quotes
    
    if itrade_excel.canReadExcel:
        Import_ListOfQuotes_ASX(quotes,'ASX')

        quotes.saveListOfQuotes()
    else:
        print 'XLRD package not installed :-('

# ============================================================================
# That's all folks !
# ============================================================================
