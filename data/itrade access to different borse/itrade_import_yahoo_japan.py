#!/usr/bin/env python
# -*- coding: cp1252 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_import_yahoo_japan.py
#
# Description: Import quotes from http://quote.yahoo.co.jp/
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
# New code for yahoo_japan is from Michel Legrand.

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
# 2005-10-17    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import string
from datetime import *

# iTrade system
from itrade_logging import *
from itrade_quotes import *
from itrade_datation import Datation,dd_mmm_yy2yyyymmdd,re_p3_1
from itrade_defs import *
from itrade_ext import *
from itrade_market import yahooTicker,yahooUrlJapan
from itrade_connection import ITradeConnection
import itrade_config

# ============================================================================
# Import_yahoojp()
#
# ============================================================================

class Import_yahoojp(object):
    def __init__(self):
        debug('Import_yahoojp:__init__')

        self.m_connection = ITradeConnection(cookies = None,
                                           proxy = itrade_config.proxyHostname,
                                           proxyAuth = itrade_config.proxyAuthentication,
                                           connectionTimeout = itrade_config.connectionTimeout
                                           )

    def name(self):
        return 'yahoojp'

    def interval_year(self):
        return 0.5

    def connect(self):
        return True

    def disconnect(self):
        pass

    def getstate(self):
        return True

    def parseDate(self,d):
        return (d.year, d.month, d.day)

    def splitLines(self,buf):
        lines = string.split(buf, '\n')
        lines = filter(lambda x:x, lines)
        def removeCarriage(s):
            if s[-1]=='\r':
                return s[:-1]
            else:
                return s
        lines = [removeCarriage(l) for l in lines]
        return lines

    def getdata(self,quote,datedebut=None,datefin=None):
        # specific numTradeYears
        itrade_config.numTradeYears = 2
        
        if not datefin:
            datefin = date.today()
        if not datedebut:
            datedebut = date.today()
        if isinstance(datedebut,Datation):
            datedebut = datedebut.date()
        if isinstance(datefin,Datation):
            datefin = datefin.date()
        d1 = self.parseDate(datedebut)
        d2 = self.parseDate(datefin)

        debug("Import_yahoojp:getdata quote:%s begin:%s end:%s" % (quote,d1,d2))

        sname = yahooTicker(quote.ticker(),quote.market(),quote.place())

        ss = sname
            
        ch = '<tr align=right bgcolor="#ffffff">'
        lines = []
        
        for cursor in range(0,4650,50):
            
            url = yahooUrlJapan(quote.market(),live=False) + '?' +'c=%s&a=%s&b=%s&f=%s&d=%s&e=%s&g=d&s=%s&y=%s&z=%s' % (d1[0],d1[1],d1[2],d2[0],d2[1],d2[2],ss,str(cursor),ss)
            #url = 'http://table.yahoo.co.jp/t?s=%s&a=1&b=1&c=2000&d=%s&e=%s&f=%s&g=d&q=t&y=%s&z=/b?p=tjfzqcvy4.ewcf7pt&x=.csv' % (ss,d2[1],d2[2],d2[0],str(cursor))
            
            debug("Import_yahoojp:getdata: url=%s ",url)
            try:
                buf=self.m_connection.getDataFromUrl(url)
            except:
                debug('Import_yahoojp:unable to connect :-(')
                return None
            # pull data
            linesjp = self.splitLines(buf)
            if len(linesjp)<=0:
                # empty content
                return None
            
            #typical lines indices
            
            #<tr align=right bgcolor="#ffffff">
            #<td><small>2009126</small></td>      (DATE)
            #<td><small>772.59</small></td>            (OPEN)
            #<td><small>777.91</small></td>            (HIGH)
            #<td><small>767.82</small></td>            (LOW)
            #<td><small><b>768.28</b></small></td>     (LAST)
            
            #typical lines quotes

            #<tr align=right bgcolor="#ffffff">        
            #<td><small>2009119</small></td>           (DATE)
            #<td><small>198</small></td>               (OPEN)
            #<td><small>200</small></td>               (HIGH)
            #<td><small>198</small></td>               (LOW)
            #<td><small><b>199</b></small></td>        (LAST)
            #<td><small>92,000</small></td>            (VOLUME)
            #<td><small>199</small></td>               (ADJUSTCLOSE)
            #</tr><tr align=right bgcolor="#ffffff">

            #<td><small>2009116</small></td>
            #<td><small>197</small></td>
            #<td><small>200</small></td>

            
            n = 0
            i = 0   
            q = 0
            #header = 'Date,Open,High,Low,Close,Volume,Adj Close'
            #filedata.write(header+'\n')
            for line in linesjp:

                if ch in line : n = 1
                if n == 1 :
                    q = 1
                    if '<td><small>' in line:
                        i = i + 1
                        data = line[(line.find('small>')+6):(line.find ('</'))]
                        if i == 1 :
                            date = data
                            date = date.replace('Ç¯',' ')
                            date = date.replace('·î',' ')
                            date = date.replace('Æü','')
                            date = date.split()
                            if len(date[1]) == 1 : date[1] = '0'+date[1]
                            if len(date[2]) == 1 : date[2] = '0'+date[2]
                            date = '-'.join(date)
                        elif i == 2 :
                            open = data
                            open = open.replace(',','')
                        elif i == 3 :
                            high = data
                            high = high.replace(',','')
                        elif i == 4 :
                            low = data
                            low = low.replace(',','')
                        elif i == 5 :
                            close = data[3:]
                            close = close.replace(',','')
                            
                            if ss == '998405' or ss == '998407' or ss =='23337' :
                                volume = '0'
                                open = open.replace(',','')
                                high = high.replace(',','')
                                low = low.replace(',','')
                                close = close.replace(',','')
                                adjustclose = close
                                i = 0
                                n = 0
                                ligne = ','.join([date,open,high,low,close,volume,adjustclose])
                                #print ligne
                                lines.append(ligne)
                                
                        elif i == 6 :
                            volume = data
                            volume = volume.replace(',','')
                        elif i == 7 :
                            i = 0
                            n = 0
                            adjustclose = data
                            adjustclose = adjustclose.replace(',','')
                            ligne = ','.join([date,open,high,low,close,volume,adjustclose])
                            #print ligne
                            lines.append(ligne)
            if q == 0:
                break   
        data = ""
        for eachLine in lines:
            sdata = string.split (eachLine, ',')
            sdate = sdata[0]
            open = string.atof(sdata[1])
            high = string.atof(sdata[2])
            low = string.atof(sdata[3])
            value = string.atof(sdata[6])   #   Adj. Close*
            volume = string.atoi(sdata[5])

            if volume>=0:
                # encode in EBP format
                # ISIN;DATE;OPEN;HIGH;LOW;CLOSE;VOLUME
                line = (
                    quote.key(),
                    sdate,
                    open,
                    high,
                    low,
                    value,
                    volume
                )
                line = map(lambda (val): '%s' % str(val), line)
                line = string.join(line, ';')

                # append
                data = data + line + '\r\n'
        return data

# ============================================================================
# Export me
# ============================================================================

try:
    ignore(gImportYahoojp)
except NameError:
    gImportYahoojp = Import_yahoojp()

registerImportConnector('TOKYO EXCHANGE','TKS',QLIST_ANY,QTAG_IMPORT,gImportYahoojp,bDefault=True)



# ============================================================================
# Test ME
#
# ============================================================================

def test(ticker,d):
    if gImportYahoo.connect():

        state = gImportYahoo.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'TOKYO EXCHANGE')
            data = gImportYahoo.getdata(quote,d)
            if data!=None:
                if data:
                    debug(data)
                else:
                    debug("nodata")
            else:
                print "getdata() failure :-("
        else:
            print "getstate() failure :-("

        gImportYahoo.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    setLevel(logging.INFO)

    # never failed - fixed date
    print "15/03/2005"
    test('AAPL',date(2005,3,15))

    # never failed except week-end
    print "yesterday-today :-("
    test('AAPL',date.today()-timedelta(1))

    # always failed
    print "tomorrow :-)"
    test('AAPL',date.today()+timedelta(1))

# ============================================================================
# That's all folks !
# ============================================================================
