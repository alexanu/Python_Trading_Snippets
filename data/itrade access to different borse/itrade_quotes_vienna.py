#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_vienna.py
#
# Description: List of quotes from http://en.wienerborse.at/
#
# Developed for iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is	from Gilles Dumortier.
# New code for VIENNER BORSE is from Michel Legrand.

# Portions created by the Initial Developer are Copyright (C) 2007-2008 the
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
# 2007-12-28    dgil  Wrote it from template
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
# Import_ListOfQuotes_WBO()
#
# ============================================================================

def Import_ListOfQuotes_WBO(quotes,market='WIENER BORSE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)
    if market=='WIENER BORSE':
        url = "http://en.wienerborse.at/marketplace_products/trading/auction/?query=&markets=A_G_D&market=all"

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

    info('Import_ListOfQuotes_WBO_%s:connect to %s' % (market,url))

    try:
        data=connection.getDataFromUrl(url)
    except:
        debug('Import_ListOfQuotes_WBO:unable to connect :-(')
        return False

    # returns the data
    lines = splitLines(data)

    count = 0
    n = 1
    i = 0

    for line in lines:
        
        #typical lines:
        #<td class="left">AT00000ATEC9</td>
        #<td class="left">ATEC</td>
        #<td class="left">A-TEC INDUSTRIES AG</td>
        #<td class="left">08:55</td>
        #<td class="left">12:00</td>
        #<td class="left">17:30</td>
        
        # extract data

        if '<th colspan="6"><b>Prime Market.at</b></th>' in line : n = 0

        if n == 0 :
            
            if '<td class="left">' in line :
                i = i + 1
                ch = line[(line.find('>')+1):(line.find ('</td>'))]
                if i == 1 :
                    isin = ch
                elif i == 2 :
                    ticker = ch
                elif i == 3 :
                    name = ch
                    name = name.replace('ä','a')#\xe4
                    name = name.replace('ö','o')#\xf6
                    name = name.replace('Ö','O')#\xd6
                    name = name.replace('ü','u')#\xfc
                    name = name.replace('ß','?')#\xdf
                    
                elif i == 6 :
                    i = 0
                    
                    #print isin, name, ticker
                            
                    # ok to proceed
                            
                    quotes.addQuote(isin = isin,name = name,
                            ticker = ticker,market= market,currency = 'EUR',
                            place = 'WBO',country = 'AT')
                    
                    count = count + 1

    if itrade_config.verbose:
        print 'Imported %d lines from WIENER BORSE' % (count)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('WIENER BORSE','WBO',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_WBO)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_WBO(quotes,'WIENER BORSE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
