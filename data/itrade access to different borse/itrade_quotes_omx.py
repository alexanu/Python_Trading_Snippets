#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_omx.py
#
# Description: List of quotes from http://www.nasdaqomxnordic.com : OMX MARKET (STOCKHOLM,COPENHAGEN)
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is from Gilles Dumortier.
# New code for OMX is from Michel Legrand.
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
# Import_ListOfQuotes_OMX()
#
# ============================================================================

def Import_ListOfQuotes_OMX(quotes,market='STOCKHOLM EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)

    import xlrd

    # find url to update list
    ch ='href="/digitalAssets/'
    try:
        url = connection.getDataFromUrl('http://www.nasdaqomxnordic.com/shares?languadeID=1')
    except:
        info('Import_ListOfQuotes_OMX_%s:unable to get XLS file name :-(' % market)
        return False

    if url.find(ch):
        a = url.find(ch)+len(ch)
        endurl = url[a:url.index('"',a)]
    else:
        info('Import_ListOfQuotes_OMX_%s:unable to parse XLS file name :-(' % market)
        return False


    url = "http://www.nasdaqomxnordic.com/digitalAssets/" + endurl
    if market=='STOCKHOLM EXCHANGE':
        m_place='STO'
        country='SE'

    elif market=='COPENHAGEN EXCHANGE':
        #m_place='CSE'
        m_place='CPH'
        country='DK'
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

    info('Import_ListOfQuotes_OMX_%s:connect to %s' % (market,url))

    try:
        data = connection.getDataFromUrl(url)
    except:
        info('Import_ListOfQuotes_OMX_%s:unable to connect :-(' % market)
        return False
    
    # returns the data
    book = itrade_excel.open_excel(file=None,content=data)
    sh = book.sheet_by_index(1)
    n = 0
    indice = {}
    country = ''

    #print 'Import_ListOfQuotes_OMX_%s:' % market,'book',book,'sheet',sh,'nrows=',sh.nrows

    for line in range(sh.nrows):
        if sh.cell_type(line,1) != xlrd.XL_CELL_EMPTY:
            if n==0:
                for i in range(sh.ncols):
                    val = sh.cell_value(line,i)
                    indice[val] = i

                    # be sure we have detected the title
                    if val=='ISIN': n = n + 1

                if n==1:

                    iISIN = indice['ISIN']
                    iTicker = indice['Short Name']
                    iCurrency = indice['Currency']
                    iPlace = indice['Exchange']

            else:
                place=sh.cell_value(line,iPlace)

                if place == m_place :
                    if place == 'CPH' : place = 'CSE'
                    
                    isin=sh.cell_value(line,iISIN)

                    ticker = sh.cell_value(line,iTicker)
                    if type(ticker)==float: ticker='%s' % ticker
                    ticker=ticker.replace(' ','-')

                    name = sh.cell_value(line,0)
                    name = name.strip()
                    # caractere error in this name
                    # Black Earth Farming Ltd. SDB ('á' is between Farming and Ltd)
                    if 'Black Earth Farming' in name:
                        name = 'Black Earth Farming Ltd. SDB'

                    name = name.encode('cp1252')

                    name = name.replace('æ','ae')
                    name = name.replace('ä','a')
                    name = name.replace('å','a')
                    #name = name.replace('á',' ')not valid
                    name = name.replace('Å','A')
                    name = name.replace('ø','o')
                    name = name.replace('Ø','O')
                    name = name.replace('ó','o')
                    name = name.replace('ö','o')
                    name = name.replace('Ö','O')
                    name = name.replace('ü','u')
                    name = name.replace(',',' ')
                    
                    currency=sh.cell_value(line,iCurrency)
                    quotes.addQuote(isin = isin,name = name,ticker = ticker,market = market,currency=currency,place=place,country=country)

                    n = n + 1
    if itrade_config.verbose:
        print 'Imported %d/%d lines from %s' % (n-1,sh.nrows,market)

    return True

# ============================================================================
# Export me
# ============================================================================

if itrade_excel.canReadExcel:
    registerListSymbolConnector('STOCKHOLM EXCHANGE','STO',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_OMX)
    registerListSymbolConnector('COPENHAGEN EXCHANGE','CSE',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_OMX)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes

    if itrade_excel.canReadExcel:
        Import_ListOfQuotes_OMX(quotes,'STO')
        #Import_ListOfQuotes_OMX(quotes,'CSE')

        quotes.saveListOfQuotes()
    else:
        print 'XLRD package not installed :-('

# ============================================================================
# That's all folks !
# ============================================================================
