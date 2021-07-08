#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_milan.py
#
# Description: List of quotes from http://www.borsaitaliana.it/
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is Gilles Dumortier.
# New code for Milan is from Michel Legrand.

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
import urllib2

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_MIL()
#
# ============================================================================

def Import_ListOfQuotes_MIL(quotes,market='MILAN EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )

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
    

    if market=='MILAN EXCHANGE':
        url = "http://www.borsaitaliana.it/bitApp/listino?main_list=1&sub_list=1&service=Results&search=nome&lang=it&target=null&nome="
    else:
        return False

    info('Import_ListOfQuotes_%s:connect to %s' % (market,url))
    
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20041202 Firefox/1.0')
    
    try:
        f = urllib2.urlopen(req)
        data = f.read()
        f.close()
    except:
        info('Import_ListOfQuotes_%s:unable to connect :-(' % market)
        return False
    
    # returns the data
    lines = splitLines(data)
    
    n = 0

    for line in lines:
        if line.find('a href="/bitApp/listino?target=null&lang=it&service=Detail&from=search&main_list=1&') != -1:
            finalurl = 'http://www.borsaitaliana.it'+line[line.index('/'):line.index('" class="table">')]
            
            req = urllib2.Request(finalurl)
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20041202 Firefox/1.0')
            try:
                f = urllib2.urlopen(req)
                datas = f.read()
                f.close()
            except:
                info('Import_ListOfQuotes_ISIN_TICKER_NAME_%s:unable to connect :-(' % market)
                return False

            finaldatas = splitLines(datas)
            for nline in finaldatas:
                if nline.find('<b>Denominazione<b>') != -1:
                    name = nline[nline.index('"right">')+8:nline.index('</td></tr>')]
                if nline.find('<b>Codice Isin<b>') != -1:
                    isin = nline[nline.index('"right">')+8:nline.index('</td></tr>')]
                if nline.find('<b>Codice Alfanumerico<b>') != -1:
                    ticker = nline[nline.index('"right">')+8:nline.index('</td></tr>')]
                    
                    n =  n + 1
                    dlg.Update(x,'BORSA ITALIANA : %d /~350'%n)
                    
                    quotes.addQuote(isin=isin, name=name,
                        ticker=ticker, market=market,
                        currency='EUR', place='MIL', country='IT')
    if itrade_config.verbose:       
        print 'Imported %d lines from %s data.' % (n,market)

    return True

# ============================================================================
# Export me
# ============================================================================
registerListSymbolConnector('MILAN EXCHANGE','MIL',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_MIL)
# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes
    Import_ListOfQuotes_LSE(quotes,'MILAN EXCHANGE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
