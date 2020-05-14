#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_nze.py
#
# Description: List of quotes from http://www.findata.co.nz/
#
# Developed for iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is	from Gilles Dumortier.
# New code for NZE is from Michel Legrand.

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
import urllib

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_NZE()
#
# ============================================================================


def Import_ListOfQuotes_NZE(quotes,market='NEW ZEALAND EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)
    
    if market=='NEW ZEALAND EXCHANGE':
        url = 'http://www.findata.co.nz/Markets/NZX/%s.htm'
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
    
    select_alpha = map(chr,range(65,91)) # A to Z
    
    count = 0
    isin = ''
    
    for letter in select_alpha:

        if dlg:
            dlg.Update(x,"    NZX   :  %s  to  Z"%letter)
     
        try:
            data=connection.getDataFromUrl(url%letter)
        except:
            debug('Import_ListOfQuotes_NZE unable to connect :-(')
            return False

        # returns the data
        lines = splitLines(data)

        for line in lines:
        
            if '"hideInfo();">' in line:
                tickername = line[line.find('"hideInfo();">')+14:line.find('</td><td align=right>')]
                if not 'Index' in tickername:
                    ticker = tickername[:tickername.index('<')]
                    if not '0' in ticker[-1:]:
                        name = tickername[tickername.index('<td>')+4:]

                        count = count + 1

                        # ok to proceed
                        quotes.addQuote(isin=isin,name=name,
                                ticker=ticker,market='NEW ZEALAND EXCHANGE',currency='NZD',place='NZE',country='NZ')
    if itrade_config.verbose:
        print 'Imported %d lines from NEW ZEALAND EXCHANGE' % (count)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('NEW ZEALAND EXCHANGE','NZE',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_NZE)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_NZE(quotes,'NEW ZEALAND EXCHANGE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
