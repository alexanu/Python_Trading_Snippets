#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_hong-kong.py
#
# Description: List of quotes from http://www.hkex.com.hk/
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is Gilles Dumortier.
# New code for HKEX is from Michel Legrand.

# Portions created by the Initial Developer are Copyright (C) 2004-2007 the
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
import itrade_excel
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_HKG()
#
# ============================================================================


def Import_ListOfQuotes_HKG(quotes,market='HONG KONG EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)

    import xlrd

    if market=='HONG KONG EXCHANGE':
        # Two urls for download list of HONG KONG EXCHANGE
        urls = ['http://www.hkex.com.hk/eng/market/sec_tradinfo/isincode/documents/isino.xls','http://www.hkex.com.hk/eng/market/sec_tradinfo/isincode/documents/isinsehk.xls']
        
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

    for url in urls:

        info('Import_ListOfQuotes_HKG_%s:connect to %s' % (market,url))

        try:
            data = connection.getDataFromUrl(url)
        except:
            info('Import_ListOfQuotes_HKG_%s:unable to connect :-(' % market)
            return False


        # returns the data
        book = itrade_excel.open_excel(file=None,content=data)
        sh = book.sheet_by_index(0)

        #print 'Import_ListOfQuotes_HKG_%s:' % market,'book',book,'sheet',sh,'nrows=',sh.nrows


        for line in range(sh.nrows):
            if sh.cell_type(line,1) != xlrd.XL_CELL_EMPTY:
                if sh.cell_value(line,3) in ('ORD SH','PREF SH','TRT','RTS'):

                    isin=sh.cell_value(line,1)
                    
                    ticker = sh.cell_value(line,2)
                    if type(ticker)==float: ticker=int(ticker);ticker='%s' % ticker
                    if len(ticker) == 1 : ticker='000'+ticker
                    if len(ticker) == 2 : ticker='00'+ticker
                    if len(ticker) == 3 : ticker='0'+ticker
                            
                    name = sh.cell_value(line,0)

                    if ticker == '0657':
                        name = 'G-VISION INTERNATIONAL (HOLDINGS) LTD'
                        
                    name = name.decode().encode('utf8')
                    name = name.replace(',',' ')

                    currency='HKD'
                    place='HKG'
                    country='HK'
               
                    quotes.addQuote(isin = isin,name = name,ticker = ticker,market = 'HONG KONG EXCHANGE',currency = currency,place = place,country = country)

                    n = n + 1
                

    if itrade_config.verbose:
        print 'Imported %d lines from %s ' % (n,market)

    return True

# ============================================================================
# Export me
# ============================================================================

if itrade_excel.canReadExcel:
    registerListSymbolConnector('HONG KONG EXCHANGE','HKG',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_HKG)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes

    if itrade_excel.canReadExcel:
        Import_ListOfQuotes_HKG(quotes,'HKG')

        quotes.saveListOfQuotes()
    else:
        print 'XLRD package not installed :-('

# ============================================================================
# That's all folks !
# ============================================================================
