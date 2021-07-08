#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_tokyo.py
#
# Description: List of quotes from http://www.tse.or.jp/english/index.html
#
# Developed for iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is	from Gilles Dumortier.
# New code for TSE is from Michel Legrand.

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
import urllib2
import httplib

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_TKS()
#
# ============================================================================

def Import_ListOfQuotes_TKS(quotes,market='TOKYO EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection=ITradeConnection(cookies=None,
                                proxy=itrade_config.proxyHostname,
                                proxyAuth=itrade_config.proxyAuthentication)
    if market=='TOKYO EXCHANGE':
        url = "http://www.tse.or.jp/english/index.html"
    else:
        return False

    def splitLines(buf):
        lines = string.split(buf, '</td>')
        lines = filter(lambda x:x, lines)
        def removeCarriage(s):
            if s[-1]=='\r':
                return s[:-1]
            else:
                return s
        lines = [removeCarriage(l) for l in lines]
        return lines

    info('Import_ListOfQuotes_tks_%s:connect to %s' % (market,url))

    ch = '<td valign="top" bgcolor="#FFFFE0"><font size="2"'
    count = 0
    url = '/tse/qsearch.exe?F=listing%2Fecslist&KEY1=&KEY5=&shijyo0=1stSec&shijyo1=2ndSec&shijyo2=Mothers&KEY3=&kind=TTCODE&sort=%2B&MAXDISP=100&KEY2=1stSec%2C2ndSec%2CMothers&REFINDEX=%2BTTCODE'

    host = 'quote.tse.or.jp'

    headers = { "Host": host
                , "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; fr; rv:1.9.0.5) Gecko/2008120122 Firefox/3.0.5"
                , "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                , "Accept-Language": "fr"
                , "Accept-Encoding": "gzip,deflate"
                , "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7"
                , "Keep-Alive":300
                , "Connection": "keep-alive"
                , "Referer": "http://quote.tse.or.jp/tse/quote.cgi?F=listing/Ecs00"
                   }



    for cursor in range(0,2400,100):

        if cursor == 0:
            url ='/tse/qsearch.exe?F=listing%2Fecslist&KEY1=&KEY5=&shijyo0=1stSec&shijyo1=2ndSec&shijyo2=Mothers&KEY3=&kind=TTCODE&sort=%2B&MAXDISP=100&KEY2=1stSec%2C2ndSec%2CMothers&REFINDEX=%2BTTCODE'
        else:
            url = '/tse/qsearch.exe?F=listing%2Fecslist&KEY1=&KEY5=&shijyo0=1stSec&shijyo1=2ndSec&shijyo2=Mothers&KEY3=&kind=TTCODE&sort=%2B&MAXDISP=100&KEY2=1stSec%2C2ndSec%2CMothers&REFINDEX=%2BTTCODE&GO_BEFORE=&BEFORE='+str(cursor)

        conn = httplib.HTTPConnection(host,80)
        conn.request("GET",url,None,headers)
        response = conn.getresponse()
        data = response.read()
        lines = splitLines(data)
        response.close()
        
        #typical lines

        #<td valign="top" bgcolor="#FFFFE0"><font size="2"><a href="quote.cgi?F=listing/EDetail1&MKTN=T&QCODE=3086" target="_top" class="lst">J.FRONT RETAILING CO., LTD.</a></font></td>

        q = 0
        
        for line in lines:

            if ch in line :
                q = 1
                count = count +1
                ticker = line[(line.find('QCODE=')+6):(line.find ('" target="_top"'))]
                name = line[(line.find('"lst">')+6):(line.find ('</a>'))]
                name = name.replace(',','')
                name = name.replace('  ',' ')
                name = name.replace('¥',' - ')
                # ok to proceed
                
                dlg.Update(x,'TSE : %s /~2370'%cursor)
                
                quotes.addQuote(isin = '',name = name,
                        ticker = ticker,market= 'TOKYO EXCHANGE',currency = 'JPY',
                        place = 'TKS',country = 'JP')
        if q == 0:
            break
    if itrade_config.verbose:
        print 'Imported %d lines from TOKYO EXCHANGE' % (count)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('TOKYO EXCHANGE','TKS',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_TKS)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    from itrade_quotes import quotes

    Import_ListOfQuotes_TKS(quotes,'TOKYO EXCHANGE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
