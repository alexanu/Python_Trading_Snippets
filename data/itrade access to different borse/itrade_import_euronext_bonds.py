#!/usr/bin/env python
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_import_euronext.py
#
# Description: Import quotes from euronext.com
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
# New code for euronext_bonds is from Michel Legrand.
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
# 2006-11-01    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import string
import time
import urllib2
from datetime import *

# iTrade system
from itrade_logging import *
from itrade_quotes import *
from itrade_datation import Datation,jjmmaa2yyyymmdd
from itrade_defs import *
from itrade_ext import *
from itrade_market import euronextmic
from itrade_connection import ITradeConnection
import itrade_config

# ============================================================================
# Import_euronext_bonds()
#
# ============================================================================

class Import_euronext_bonds(object):
    def __init__(self):
        debug('Import_euronext_bonds:__init__')
        self.m_url = 'https://bonds.nyx.com/en/popup/data/download?ml=nyx_pd_bonds&cmd=default&formKey=nyx_pd_filter_values%3Acddaabb01525e7a66f50cd7f51ece3dd'

        self.m_connection = ITradeConnection(cookies = None,
                                           proxy = itrade_config.proxyHostname,
                                           proxyAuth = itrade_config.proxyAuthentication,
                                           connectionTimeout = itrade_config.connectionTimeout
                                           )

    def name(self):
        return 'euronext_bonds'

    def interval_year(self):
        return 2

    def connect(self):
        return True

    def disconnect(self):
        pass

    def getstate(self):
        return True

    def parseDate(self,d):
        return (d.year, d.month, d.day)

    def parseFValue(self,d):
        val = string.split(d,',')
        ret = ''
        for val in val:
            ret = ret+val
        return string.atof(ret)

    def parseLValue(self,d):
        if d=='-': return 0
        if ',' in d:
            s = ','
        else:
            s = '\xA0'
        val = string.split(d,s)
        ret = ''
        for val in val:
            ret = ret+val
        return string.atol(ret)

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

        #IdInstrument = euronext_InstrumentId(quote)
        #if IdInstrument == None: return None

        # get historic data itself !
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

        mic = euronextmic(quote.market(),quote.place())
        
        format = '%Y-%m-%d %H:%M:%S'
        #origin = "1970-01-01 00:00:00"
        datefrom = str(datedebut) + " 02:00:00"
        dateto = str(datefin) + " 23:00:00"
        datefrom = time.mktime(time.strptime(datefrom, format))
        datefromurl =str(int(datefrom/100))+'00000'
        dateto = time.mktime(time.strptime(dateto, format))
        datefinurl =str(int(dateto)/100)+'00000'
        endurl = 'typefile=csv&layout=vertical&typedate=dmy&separator=comma&mic=%s&isin=%s&name=&namefile=Price_Data_Historical&from=%s&to=%s&adjusted=1&base=0' % (mic,quote.isin(),datefromurl,datefinurl)

        debug("Import_euronext_bonds:getdata quote:%s begin:%s end:%s" % (quote,d1,d2))

        query = (
            ('typefile', 'csv'),
            ('layout', 'vertical'),
            ('typedate', 'dmy'),
            ('separator', 'comma'),
            ('mic', mic),
            ('isin', quote.isin()),
            ('name', ''),
            ('namefile', 'Price_Data_Historical'),
            ('from', datefromurl),
            ('to', datefinurl),
            ('adjusted', '1'),
            ('base', '0'),
        )

        query = map(lambda (var, val): '%s=%s' % (var, str(val)), query)
        query = string.join(query, '&')
        url = self.m_url + '?' + query
        #print url
        debug("Import_euronext_bonds:getdata: url=%s ",url)
        try:

            req = urllib2.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20041202 Firefox/1.0')

            f = urllib2.urlopen(req)
            buf = f.read()
            f.close()
            
            #buf=self.m_connection.getDataFromUrl(url)
        except:
            debug('Import_euronext_bonds:unable to connect :-(')
            return None

        # pull data
        lines = self.splitLines(buf)
        data = ''
        #print lines

        for eachLine in lines[4:]:
            eachLine = eachLine.replace('","',';')
            eachLine = eachLine.replace('"','')
            sdata = string.split(eachLine,';')
            
            if len(sdata)== 11:
                #print sdata
                #if (sdata[0] != "Date") and (quote.list() == QLIST_INDICES):
                sdate = jjmmaa2yyyymmdd(sdata[2])
                open = self.parseFValue(sdata[3].replace(',','.'))
                print 'open:',open
                high = self.parseFValue(sdata[4].replace(',','.'))
                low = self.parseFValue(sdata[5].replace(',','.'))
                value = self.parseFValue(sdata[6].replace(',','.'))
                volume = self.parseLValue(sdata[7])
                #print quote.key(),sdate,open,high,low,value,volume

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
                #print line

                # append
                data = data + line + '\r\n'
        return data

# ============================================================================
# Export me
# ============================================================================
try:
    ignore(gImportEuronext)
except NameError:
    gImportEuronext = Import_euronext_bonds()
registerImportConnector('EURONEXT','PAR',QLIST_BONDS,QTAG_IMPORT,gImportEuronext,bDefault=True)
registerImportConnector('EURONEXT','BRU',QLIST_BONDS,QTAG_IMPORT,gImportEuronext,bDefault=True)
registerImportConnector('EURONEXT','AMS',QLIST_BONDS,QTAG_IMPORT,gImportEuronext,bDefault=True)
registerImportConnector('EURONEXT','LIS',QLIST_BONDS,QTAG_IMPORT,gImportEuronext,bDefault=True)


# ============================================================================
# Test ME
#
# ============================================================================

def test(ticker,d):
    if gImportEuronext.connect():

        state = gImportEuronext.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'EURONEXT')
            data = gImportEuronext.getdata(quote,d)
            if data!=None:
                if data:
                    debug(data)
                else:
                    debug("nodata")
            else:
                print "getdata() failure :-("
        else:
            print "getstate() failure :-("

        gImportEuronext.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    setLevel(logging.INFO)

    quotes.loadMarket('EURONEXT')

    # never failed - fixed date
    print "15/03/2005"
    test('OSI',date(2005,3,15))

    # never failed except week-end
    print "yesterday-today :-("
    test('OSI',date.today()-timedelta(1))

    # always failed
    print "tomorrow :-)"
    test('OSI',date.today()+timedelta(1))


# ============================================================================
# That's all folks !
# ============================================================================
