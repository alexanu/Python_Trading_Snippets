#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_buenos-aires.py
#
# Description: List of quotes from
#   http://www.bolsar.com/NET/Research/Especies/Acciones.aspx
#
# Developed for iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is from Gilles Dumortier.
# New code for BUE is from Michel Legrand.
#
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
# Import_ListOfQuotes_BUE()
#
# ============================================================================


def Import_ListOfQuotes_BUE(quotes,market='BUENOS AIRES EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)

    if market=='BUENOS AIRES EXCHANGE':
        url = 'http://www.bolsar.com/NET/Research/Especies/Acciones.aspx'
    else:
        return False

    try:
        data=urllib.urlopen(url)

    except:
        debug('Import_ListOfQuotes_BUE unable to connect :-(')
        return False

    nlines = 0
    i = 0
    n = 0
    ch_ticker = '		<td Class="Oscuro"><A href="/NET/Research/Especies/Acciones.aspx?especie='
    ch_name = '		<td><A class="LinkAzul" href="../sociedades/fichaTecnica.aspx?emisor='

    #typical lines:
            #<tr Class="Claro">
    #		<td Class="Oscuro"><A href="/NET/Research/Especies/Acciones.aspx?especie=4">ACIN</a></td>
    #		<td>Ordinarias Escriturales &quot;B&quot; (1 Voto)</td>
    #		<td>ARP008791179</td>
    #		<td><A class="LinkAzul" href="../sociedades/fichaTecnica.aspx?emisor=2">ACINDAR S.A.</a></td>

    for line in data:
        if ch_ticker in line:
            i = 1
            ticker = line[len(ch_ticker):]
            ticker = ticker[ticker.index('">')+2 : ticker.index('</a></td>')]
            #print ticker
        elif i == 1:
            n= n + 1
            if n == 2:
                isin = line[line.index('<td>')+4:line.index('</td>')]
                #print isin
            if n == 3:
                name = line[len(ch_name):]
                name = name[name.index('">')+2:name.index('</a></td>')]

                name = name.decode('utf-8').encode('cp1252')

                name = name.replace(' S.A.','')
                name = name.replace(' S. A.','')
                name = name.replace(',','')

                name = name.replace('í','i') #í
                name = name.replace('Í','i') #Í
                name = name.replace('ó','o') #Ó
                name = name.replace('Á','a') #Á
                name = name.replace('Ñ','n') #Ñ

                name = name.upper()
                i = 0
                n = 0

                # ok to proceed
                if isin!='':
                    quotes.addQuote(isin=isin, name=name,
                    ticker=ticker, market='BUENOS AIRES EXCHANGE',currency='ARS',place='BUE',country='AR')
                    nlines = nlines + 1
    if itrade_config.verbose:
        print 'Imported %d lines from BUENOS AIRES EXCHANGE data.' % (nlines)
    data.close()
    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('BUENOS AIRES EXCHANGE','BUE',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_BUE)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_BUE(quotes,'BUENOS AIRES EXCHANGE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
