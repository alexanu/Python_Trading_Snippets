#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_swx.py
#
# Description: List of quotes from swx.com : SWISS MARKET
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is Gilles Dumortier.
# New code for SWX is from Michel Legrand.
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

# iTrade system
import itrade_config
import itrade_csv
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_SWX()
#
# ============================================================================

def Import_ListOfQuotes_SWX(quotes,market='SWISS EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )
    if market=='SWISS EXCHANGE':
        url = 'http://www.six-swiss-exchange.com/shares/companies/download/issuers_all_fr.csv'
        try:
            data = connection.getDataFromUrl(url)
        except:
            info('Import_ListOfQuotes_SWX_%s:unable to get file name :-(' % market)
            return False
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

    # returns the data
    lines = splitLines(data)
    n = 0
    isin = ''
    for line in lines[1:]:
        line = line.replace('!',' ')
        line = line.replace(',',' ')
        line = line.replace('à','a')
        line = line.replace('ä','a')
        line = line.replace('â','a')
        line = line.replace('ö','o')
        line = line.replace('ü','u')
        line = line.replace('é','e')
        line = line.replace('è','e')
        line = line.replace('+',' ')
        
        data = string.split(line,';') # csv line
        name = data[0].strip()
        ticker = data[1].strip()
        country = data[3].strip()
        currency = data[4].strip()
        exchange = data[5].strip()

        quotes.addQuote(isin=isin,name=name, ticker=ticker,market='SWISS EXCHANGE',
                    currency=currency,place=exchange,country=country)
        n = n + 1
    if itrade_config.verbose:
        print 'Imported %d lines from %s' % (n,market)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('SWISS EXCHANGE','XSWX',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_SWX)
registerListSymbolConnector('SWISS EXCHANGE','XVTX',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_SWX)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_SWX(quotes,'XSWX')
    Import_ListOfQuotes_SWX(quotes,'XVTX')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
